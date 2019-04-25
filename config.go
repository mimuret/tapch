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
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type Config struct {
	ClickHouse ClickHouseConfig
	Nats       NatsConfig
}

type ClickHouseConfig struct {
	Dsn      string
	Prefix   string
	SaveHour int
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
