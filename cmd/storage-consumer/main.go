// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/logger"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
)

var (
	upstreamURIStr   string
	upstreamURI      *url.URL
	downstreamURIStr string
	configFile       string
	logFile          string
	logLevel         string
	flushInterval    time.Duration
	fileIndexWidth   int
	enableProfiling  bool
	timezone         string
)

func init() {
	version.LogVersionInfo("storage consumer")
	flag.StringVar(&upstreamURIStr, "upstream-uri", "", "storage uri")
	flag.StringVar(&downstreamURIStr, "downstream-uri", "", "downstream sink uri")
	flag.StringVar(&configFile, "config", "", "changefeed configuration file")
	flag.StringVar(&logFile, "log-file", "", "log file path")
	flag.StringVar(&logLevel, "log-level", "info", "log level")
	flag.DurationVar(&flushInterval, "flush-interval", 2*time.Second, "flush interval")
	flag.IntVar(&fileIndexWidth, "file-index-width",
		config.DefaultFileIndexWidth, "file index width")
	flag.BoolVar(&enableProfiling, "enable-profiling", false, "whether to enable profiling")
	flag.StringVar(&timezone, "tz", "System", "Specify time zone of storage consumer")
	flag.Parse()

	err := logger.InitLogger(&logger.Config{
		Level: logLevel,
		File:  logFile,
	})
	if err != nil {
		log.Error("init logger failed", zap.Error(err))
		os.Exit(1)
	}

	uri, err := url.Parse(upstreamURIStr)
	if err != nil {
		log.Error("invalid upstream-uri", zap.Error(err))
		os.Exit(1)
	}
	upstreamURI = uri
	scheme := strings.ToLower(upstreamURI.Scheme)
	if !config.IsStorageScheme(scheme) {
		log.Error("invalid storage scheme, the scheme of upstream-uri must be file/s3/azblob/gcs")
		os.Exit(1)
	}
}

func main() {
	var consumer *consumer
	var err error

	if enableProfiling {
		go func() {
			server := &http.Server{
				Addr:              "127.0.0.1:6060",
				ReadHeaderTimeout: 5 * time.Second,
			}

			if err := server.ListenAndServe(); err != nil {
				log.Fatal("http pprof", zap.Error(err))
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	deferFunc := func() int {
		stop()
		if consumer != nil {
			consumer.sink.Close(false)
		}
		if err != nil && err != context.Canceled {
			return 1
		}
		return 0
	}

	consumer, err = newConsumer(ctx)
	if err != nil {
		log.Error("failed to create storage consumer", zap.Error(err))
		goto EXIT
	}

	if err = consumer.run(ctx); err != nil {
		log.Error("error occurred while running consumer", zap.Error(err))
	}

EXIT:
	os.Exit(deferFunc())
}
