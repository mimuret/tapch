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
	"bytes"
	"html/template"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

var DefaultSQLTemplate = `CREATE TABLE IF NOT EXISTS {{ .TableName }} (
	id UUID,
	timestamp DateTime,
	query_time DateTime,
	query_address String,
	query_address_hash String,
	query_port UInt16,
	response_time DateTime,
	response_address String,
	response_address_hash String,
	response_port UInt16,
	response_zone String,
	ecs_net String,
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
) engine={{ .Engine }}`

type Config struct {
	ClickHouse       ClickHouseConfig
	Nats             NatsConfig
	WorkerNum        int
	QueueSize        int
	PropPort         int
	PrometheusListen string
}

type TemplateVal struct {
	TableName string
	Engine    string
}

func (c *Config) GetWorkerNum() int {
	if c.WorkerNum <= 0 {
		return 1
	}
	return c.WorkerNum
}
func (c *Config) GetQueueSize() int {
	if c.QueueSize <= 65535 {
		return 65535
	}
	return c.QueueSize
}
func (c *Config) GetPrometheusListen() string {
	if c.PrometheusListen == "" {
		return ":9520"
	}
	return c.PrometheusListen
}

type ClickHouseConfig struct {
	Dsn                 string
	Prefix              string
	SaveHour            int
	WorkerNum           int
	CreateDistributed   bool
	CreateTableTemplate string
}

func (c *ClickHouseConfig) GetWorkerNum() int {
	if c.WorkerNum <= 0 {
		return 1
	}
	return c.WorkerNum
}
func (c *ClickHouseConfig) GetCreateTableSql(tn string, engine string) (string, error) {
	var tmplStr = DefaultSQLTemplate
	if c.CreateTableTemplate != "" {
		r, err := os.Open(c.CreateTableTemplate)
		if err != nil {
			return "", err
		}
		bs, err := ioutil.ReadAll(r)
		if err != nil {
			return "", err
		}
		tmplStr = string(bs)
	}
	tmpl, err := template.New("sql").Parse(tmplStr)
	if err != nil {
		return "", err
	}
	val := TemplateVal{TableName: tn, Engine: engine}
	b := bytes.NewBuffer(nil)
	err = tmpl.Execute(b, val)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

type NatsConfig struct {
	Host     string
	Subject  string
	User     string
	Password string
	Token    string
	Group    string
}

func NewConfigFromFile(filename string) (*Config, error) {
	c := Config{}
	v := viper.New()
	v.SetConfigFile(filename)

	v.SetConfigType("toml")
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "can't read config")
	}
	if err := v.Unmarshal(&c); err != nil {
		return nil, errors.Wrap(err, "can't parse config")
	}
	return &c, nil
}
