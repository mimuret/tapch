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
	"strconv"
	"sync"
	"syscall"

	"net/http"
	_ "net/http/pprof"
	"runtime/debug"

	_ "github.com/mailru/go-clickhouse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/nats-io/go-nats"
	log "github.com/sirupsen/logrus"
)

var msgCh chan *nats.Msg

func init() {
	log.SetOutput(os.Stdout)
	log.SetLevel(log.DebugLevel)
}

var (
	flagConfigFile = flag.String("c", "config.toml", "config file path")
	flagLogLevel   = flag.String("d", "info", "log level(debug,info,warn,error,fatal)")
)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]...\n", os.Args[0])
	flag.PrintDefaults()
}

var (
	SubscribeMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tapch_subscribe_messages",
		Help: "The total number of input messages",
	})
	LostMessages = promauto.NewCounter(prometheus.CounterOpts{
		Name: "tapch_lost_messages",
		Help: "The total number of lost messages",
	})
)

func PrometheusExporter(ctx context.Context, listen string) {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(listen, nil)
	if err != nil {
		log.Error(err)
	}
}

func main() {
	var err error
	debug.FreeOSMemory()

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
	if c.PropPort > 0 {
		go func() {
			log.Println(http.ListenAndServe("localhost:"+strconv.Itoa(c.PropPort), nil))
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go PrometheusExporter(ctx, c.GetPrometheusListen())

	workerNum := c.GetWorkerNum() * (c.ClickHouse.GetWorkerNum() + 2)
	if workerNum > runtime.NumCPU() {
		workerNum = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(workerNum)

	utilWorker := NewUtilWorker(c)
	if err := utilWorker.Prepare(); err != nil {
		log.Fatalf("faild to prepare database: %v", err)
	}
	go func() {
		utilWorker.Run(ctx)
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < workerNum; i++ {
		worker := NewWorker(c)
		wg.Add(1)
		log.Debugf("start worker %d", i)
		go func() {
			if err := worker.Run(ctx); err != nil {
				log.Error(err)
			}
			wg.Done()
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	log.WithFields(log.Fields{
		"func": "serv",
	}).Info("start server")

	endCh := make(chan struct{})
	go func() {
		wg.Wait()
		endCh <- struct{}{}
	}()

	select {
	case <-endCh:
	case <-sigCh:
		cancel()
	}
	wg.Wait()
}
