// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	logPath  string
	logLevel string
)

func main() {
	var (
		upstreamURIStr  string
		configFile      string
		enableProfiling bool
	)
	groupID := fmt.Sprintf("ticdc_kafka_consumer_%s", uuid.New().String())
	consumerOption := newOption()

	flag.StringVar(&configFile, "config", "", "config file for changefeed")
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log file path")
	flag.BoolVar(&enableProfiling, "enable-profiling", false, "enable pprof profiling")

	flag.StringVar(&consumerOption.downstreamURI, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&consumerOption.schemaRegistryURI, "schema-registry-uri", "", "schema registry uri")
	flag.StringVar(&consumerOption.upstreamTiDBDSN, "upstream-tidb-dsn", "", "upstream TiDB DSN")
	flag.StringVar(&consumerOption.groupID, "consumer-group-id", groupID, "consumer group id")
	flag.StringVar(&consumerOption.timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&consumerOption.ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&consumerOption.cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&consumerOption.key, "key", "", "Private key path for Kafka SSL connection")
	flag.Parse()

	err := logger.InitLogger(&logger.Config{
		Level: logLevel,
		File:  logPath,
	})
	if err != nil {
		log.Panic("init logger failed", zap.Error(err))
	}
	version.LogVersionInfo("kafka consumer")

	err = consumerOption.Adjust(upstreamURIStr, configFile)
	if err != nil {
		log.Panic("adjust consumer option failed", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	if enableProfiling {
		g.Go(func() error {
			return http.ListenAndServe(":6060", nil)
		})
	}

	consumer := newConsumer(ctx, consumerOption)
	g.Go(func() error {
		return consumer.Consume(ctx)
	})

	g.Go(func() error {
		sigterm := make(chan os.Signal, 1)
		signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-ctx.Done():
			log.Info("terminating: context cancelled")
		case <-sigterm:
			log.Info("terminating: via signal")
		}
		cancel()
		return nil
	})
	err = g.Wait()
	if err != nil {
		log.Error("kafka consumer exited with error", zap.Error(err))
	} else {
		log.Info("kafka consumer exited")
	}
}
