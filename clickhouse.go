/*
 * Copyright (c) 2018 Manabu Sonoda
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse"
	"github.com/mimuret/dtap"

	log "github.com/sirupsen/logrus"
)

func createTable(c *Config) error {
	connect, err := sql.Open("clickhouse", c.ClickHouse.Dsn)
	if err != nil {
		return err
	}
	defer connect.Close()

	if err := connect.Ping(); err != nil {
		return err
	}
	t := time.Now()
	for i := 0; i < 1440; i++ {
		t := t.Add(time.Duration(i) * time.Minute)
		tn := c.ClickHouse.Prefix + "_" + t.Format("20060102_1504")
		_, err = connect.Exec(`CREATE TABLE IF NOT EXISTS ` + tn + ` (
				id UUID,
				timestamp DateTime,
				query_time DateTime,
				query_address FixedString(40),
				query_port UInt16,
				response_time DateTime,
				response_address FixedString(40),
				response_port UInt16,
				response_zone String,
				identity String,
				type String,
				socket_family String,
				socket_protocol String,
				version String,
				extra String,
				tld String,
				sld String,
				third_ld String,
				fourth_ld String,
				qname String,
				qclass String,
				qtype String,
				message_size UInt16,
				txid UInt16,
				rcode String,
				aa UInt8,
				tc UInt8,
				rd UInt8,
				ra UInt8,
				ad UInt8,
				cd UInt8
			) engine=Log
		`)
	}
	if err != nil {
		return err
	}
	return nil
}
func dropTable(c *Config) error {
	connect, err := sql.Open("clickhouse", c.ClickHouse.Dsn)
	if err != nil {
		return err
	}
	defer connect.Close()

	if err := connect.Ping(); err != nil {
		return err
	}
	rows, err := connect.Query("show tables")
	if err != nil {
		return err
	}
	drop := []string{}
	defer rows.Close()
	for rows.Next() {
		var tn string
		err := rows.Scan(&tn)
		if err != nil {
			return err
		}
		l := strings.Split(tn, "_")
		if len(l) < 3 {
			continue
		}
		lt := strings.Join(l[len(l)-2:], "_")
		t, err := time.Parse("20060102_1504", lt)
		if err != nil {
			log.Debugf("error parse table: %s,%s", tn, lt)
			log.Debugf("error parse table: %v", err)
			continue
		}
		du := t.Sub(time.Now())
		if t.Before(time.Now()) && du > time.Duration(c.ClickHouse.SaveHour)*time.Hour {
			log.Debugf("drop table %s, %v", tn, du)
			drop = append(drop, tn)
		}
	}
	for _, tn := range drop {
		connect.Exec("drop table " + tn)
	}
	return nil
}
func createNextTable(ctx context.Context, c *Config) error {
	nextSec := time.Now().Unix() % 60
	if nextSec == 0 {
		nextSec = 60
	}
	ticker := time.NewTimer(time.Duration(nextSec) * time.Second)
L:
	for {
		select {
		case <-ticker.C:
			createTable(c)
			dropTable(c)
		case <-ctx.Done():
			break L
		}
	}
	return nil
}
func pushRecord(ctx context.Context, c *Config) error {
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
		case msg := <-msgCh:
			data := []dtap.DnstapFlatT{}
			err := json.Unmarshal(msg.Data, &data)
			if err != nil {
				log.Errorf("can't parse msg: %v", err)
				break
			}
			tx, err := connect.Begin()
			if err != nil {
				log.Errorf("can't start transaction: %v", err)
				break
			}
			tableName := c.ClickHouse.Prefix + "_" + time.Now().Format("20060102_1504")

			stmt, err := tx.Prepare("INSERT INTO " + tableName + " (timestamp,query_time,query_address,query_port,response_time,response_address,response_port,response_zone,identity,type,socket_family,socket_protocol,version,extra,tld,sld,third_ld,fourth_ld,qname,qclass,qtype,message_size,txid,rcode,aa,tc,rd,ra,ad,cd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
			if err != nil {
				log.Errorf("can't prepare statement: %v", err)
			}
			for _, r := range data {
				if _, err := stmt.Exec(
					toMySQLDateTime(r.Timestamp),
					toMySQLDateTime(r.QueryTime),
					r.QueryAddress,
					r.QueryPort,
					toMySQLDateTime(r.ResponseTime),
					r.ResponseAddress,
					r.ResponsePort,
					r.ResponseZone,
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

func toMySQLDateTime(in string) string {
	t, err := time.Parse(time.RFC3339Nano, in)
	if err != nil {
		return "1970-01-01 00:00:01"
	}
	return t.Format("2006-01-02 15:04:05")
}
