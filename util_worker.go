package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/mailru/go-clickhouse"

	log "github.com/sirupsen/logrus"
)

type UtilWorker struct {
	config *Config
}

func NewUtilWorker(c *Config) *UtilWorker {
	return &UtilWorker{
		config: c,
	}
}

func (u *UtilWorker) Prepare() error {
	var err error
	err = u.createTable()
	if err != nil {
		return fmt.Errorf("can't create table: %v", err)
	}
	err = u.dropTable()
	if err != nil {
		return fmt.Errorf("can't drop table: %v", err)
	}
	return nil
}

func (u *UtilWorker) Run(ctx context.Context) error {
	nextSec := time.Now().Unix() % 60
	if nextSec == 0 {
		nextSec = 60
	}
	ticker := time.NewTicker(time.Duration(nextSec) * time.Second)
L:
	for {
		select {
		case <-ticker.C:
			u.createTable()
			u.dropTable()
		case <-ctx.Done():
			break L
		}
	}
	return nil
}

func (u *UtilWorker) createTable() error {
	c := u.config
	connect, err := sql.Open("clickhouse", c.ClickHouse.Dsn)
	if err != nil {
		return err
	}
	defer connect.Close()

	if err := connect.Ping(); err != nil {
		return err
	}
	t := time.Now()

	for i := 0; i < 720; i++ {
		t := t.Add(time.Duration(i) * time.Minute)
		tn := c.ClickHouse.Prefix + "_" + t.Format("20060102_1504")
		sql, err := c.ClickHouse.GetCreateTableSql(tn, "Log")
		if err != nil {
			return err
		}
		_, err = connect.Exec(sql)
		if err != nil {
			return err
		}
		if c.ClickHouse.CreateDistributed {
			sql, err := c.ClickHouse.GetCreateTableSql("d_"+tn, "Distributed(cluster,default,"+tn+")")
			if err != nil {
				return err
			}
			_, err = connect.Exec(sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (u *UtilWorker) dropTable() error {
	c := u.config
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
	loc, _ := time.LoadLocation("Asia/Tokyo")
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
		t, err := time.ParseInLocation("20060102_1504", lt, loc)
		if err != nil {
			continue
		}
		du := time.Now().Sub(t)
		if du > time.Duration(c.ClickHouse.SaveHour)*time.Hour {
			log.Debugf("drop table %s, %v", tn, du)
			drop = append(drop, tn)
		}
	}
	for _, tn := range drop {
		connect.Exec("drop table " + tn)
	}
	return nil
}
