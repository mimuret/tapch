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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	_ "github.com/mailru/go-clickhouse"

	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

var msgCh chan *nats.Msg

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

var (
	flagConfigFile = flag.String("c", "config.toml", "config file path")
	flagLogLevel   = flag.String("d", "info", "log level(debug,info,warn,error,fatal)")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]...\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	var err error

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Usage = usage
	flag.Parse()
	// set log level
	switch *flagLogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	case "fatal":
		log.SetLevel(log.FatalLevel)
	default:
		usage()
		os.Exit(1)
	}
	c, err := NewConfigFromFile(*flagConfigFile)
	if err != nil {
		log.Fatal(err)
	}
	msgCh = make(chan *nats.Msg, 65535)
	ctx, cancel := context.WithCancel(context.Background())
	sub, err := subscribe(c)
	if err != nil {
		log.Fatal(err)
	}
	err = createTable(c)
	if err != nil {
		log.Fatalf("can't create table: %v", err)
	}
	err = dropTable(c)
	if err != nil {
		log.Fatalf("can't drop table: %v", err)
	}

	go func() {
		if err := createNextTable(ctx, c); err != nil {
			log.Fatal(err)
		}
	}()
	go func() {
		if err := pushRecord(ctx, c); err != nil {
			log.Fatal(err)
		}
	}()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	log.WithFields(log.Fields{
		"func": "serv",
	}).Info("start server")

	<-sigCh

	cancel()
	sub.Unsubscribe()
	sub.Drain()
}
