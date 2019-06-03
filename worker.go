package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"sync"
	"time"

	_ "github.com/mailru/go-clickhouse"
	"github.com/mimuret/dtap"
	log "github.com/sirupsen/logrus"

	"github.com/nats-io/go-nats"
	"github.com/pkg/errors"
)

type Worker struct {
	config *Config
	rBuf   *dtap.RBuf
	sub    *nats.Subscription
}

func NewWorker(c *Config) *Worker {
	return &Worker{
		config: c,
		rBuf:   dtap.NewRbuf(uint(c.GetQueueSize()), SubscribeMessages, LostMessages),
	}
}
func (w *Worker) Run(ctx context.Context) error {
	sub, err := w.subscribe()
	if err != nil {
		return err
	}
	wg := sync.WaitGroup{}
	for i := 0; i < w.config.ClickHouse.GetWorkerNum(); i++ {
		wg.Add(1)
		log.Debugf("start push record")
		go func() {
			w.pushRecord(ctx)
			wg.Done()
		}()
	}
	wg.Wait()
	sub.Unsubscribe()
	sub.Drain()
	return nil
}

func (w *Worker) subscribe() (*nats.Subscription, error) {
	var err error
	c := w.config
	var con *nats.Conn
	if c.Nats.Token != "" {
		con, err = nats.Connect(c.Nats.Host, nats.Token(c.Nats.Token))
	} else if c.Nats.User != "" {
		con, err = nats.Connect(c.Nats.Host, nats.UserInfo(c.Nats.User, c.Nats.Password))
	} else {
		con, err = nats.Connect(c.Nats.Host)
	}
	if err != nil {
		return nil, errors.Errorf("can't connect nats: %v", err)
	}
	return con.QueueSubscribe(c.Nats.Subject, c.Nats.Group, w.subscribeCB)
}
func (w *Worker) subscribeCB(msg *nats.Msg) {
	w.rBuf.Write(msg.Data)
}

func (w *Worker) pushRecord(ctx context.Context) error {
	c := w.config
	connect, err := sql.Open("clickhouse", c.ClickHouse.Dsn)
	if err != nil {
		return err
	}
	defer connect.Close()

	if err := connect.Ping(); err != nil {
		return err
	}
L:
	for {
		select {
		case <-ctx.Done():
			break L
		case bs := <-w.rBuf.Read():
			data := []dtap.DnstapFlatT{}
			err := json.Unmarshal(bs, &data)
			if err != nil {
				log.Errorf("can't parse msg: %v", err)
				return err
			}
			tx, err := connect.Begin()
			if err != nil {
				log.Errorf("can't start transaction: %v", err)
				break
			}
			tableName := c.ClickHouse.Prefix + "_" + time.Now().Format("20060102_1504")

			stmt, err := tx.Prepare("INSERT INTO " + tableName + " (timestamp,query_time,query_address,query_address_hash,query_port,response_time,response_address,response_address_hash,response_port,response_zone,ecs_net,identity,type,socket_family,socket_protocol,version,extra,tld,sld,third_ld,fourth_ld,qname,qclass,qtype,message_size,txid,rcode,aa,tc,rd,ra,ad,cd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
			if err != nil {
				log.Errorf("can't prepare statement: %v", err)
			}
			for _, r := range data {
				if _, err := stmt.Exec(
					toMySQLDateTime(r.Timestamp),
					toMySQLDateTime(r.QueryTime),
					r.QueryAddress,
					r.QueryAddressHash,
					r.QueryPort,
					toMySQLDateTime(r.ResponseTime),
					r.ResponseAddress,
					r.ResponseAddressHash,
					r.ResponsePort,
					r.ResponseZone,
					r.EcsNet.String(),
					r.Identity,
					r.Type,
					r.SocketFamily,
					r.SocketProtocol,
					r.Version,
					r.Extra,
					r.TopLevelDomainName,
					r.SecondLevelDomainName,
					r.ThirdLevelDomainName,
					r.FourthLevelDomainName,
					r.Qname,
					r.Qclass,
					r.Qtype,
					r.MessageSize,
					r.Txid,
					r.Rcode,
					r.AA,
					r.TC,
					r.RD,
					r.RA,
					r.AD,
					r.CD,
				); err != nil {
					log.Errorf("can't write record: %v, %v", err, r)
					continue
				}
			}
			if err := tx.Commit(); err != nil {
				log.Errorf("can't commit records: %v", err)
			}
		}
	}

	return nil
}
