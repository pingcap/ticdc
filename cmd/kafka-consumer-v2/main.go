// Copyright 2026 PingCAP, Inc.
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
	"runtime/debug"
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
	debug.SetMemoryLimit(14 * 1024 * 1024 * 1024)

	var (
		upstreamURIStr  string
		configFile      string
		enableProfiling bool
	)

	o := newOption()
	groupID := fmt.Sprintf("ticdc_kafka_consumer_v2_%s", uuid.New().String())
	o.generatedGroupID = groupID

	flag.StringVar(&configFile, "config", "", "config file for changefeed")
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "Kafka uri")
	flag.StringVar(&logPath, "log-file", "cdc_kafka_consumer_v2.log", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.BoolVar(&enableProfiling, "enable-profiling", false, "enable pprof profiling")

	flag.StringVar(&o.downstreamURI, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&o.schemaRegistryURI, "schema-registry-uri", "", "schema registry uri")
	flag.StringVar(&o.upstreamTiDBDSN, "upstream-tidb-dsn", "", "upstream TiDB DSN")
	flag.StringVar(&o.groupID, "consumer-group-id", groupID, "consumer group id")
	flag.StringVar(&o.timezone, "tz", "System", "Specify time zone of Kafka consumer")
	flag.StringVar(&o.ca, "ca", "", "CA certificate path for Kafka SSL connection")
	flag.StringVar(&o.cert, "cert", "", "Certificate path for Kafka SSL connection")
	flag.StringVar(&o.key, "key", "", "Private key path for Kafka SSL connection")
	flag.Parse()

	if err := logger.InitLogger(&logger.Config{
		Level: logLevel,
		File:  logPath,
	}); err != nil {
		log.Panic("init logger failed", zap.Error(err))
	}
	version.LogVersionInfo("kafka consumer v2")

	if err := o.adjust(upstreamURIStr, configFile); err != nil {
		log.Error("adjust kafka consumer v2 option failed", zap.Error(err))
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)

	if enableProfiling {
		g.Go(func() error {
			return http.ListenAndServe(":6060", nil)
		})
	}

	consumer, err := newConsumer(ctx, o)
	if err != nil {
		log.Error("create kafka consumer v2 failed", zap.Error(err))
		os.Exit(1)
	}
	g.Go(func() error {
		return consumer.Run(ctx)
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

	if err = g.Wait(); err != nil {
		log.Error("kafka consumer v2 exited with error", zap.Error(err))
		os.Exit(1)
	}
	log.Info("kafka consumer v2 exited")
}
