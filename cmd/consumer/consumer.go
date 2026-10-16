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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"cmp"
	"context"
	"database/sql"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	cmdutil "github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/logger"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type sourceType string

const (
	sourceKafka   sourceType = "kafka"
	sourcePulsar  sourceType = "pulsar"
	sourceStorage sourceType = "storage"
)

type consumer struct {
	g             *errgroup.Group
	reader        *reader
	assembler     *assembler
	writer        *writer
	upstreamDB    *sql.DB
	profileServer *http.Server
}

func newConsumer(ctx context.Context, options *options) (c *consumer) {
	if err := logger.InitLogger(&logger.Config{
		Level: options.logLevel,
		File:  options.logFile,
	}); err != nil {
		log.Panic("consumer logger initialization failed", zap.Error(err))
	}
	version.LogVersionInfo("consumer")

	upstreamURI := parseRequiredURI(options.upstreamURI)
	sourceType := sourceTypeFromURI(upstreamURI)
	consumerID := options.consumerID
	if sourceType != sourceStorage && strings.TrimSpace(consumerID) == "" {
		consumerID = "ticdc_consumer_" + uuid.NewString()
	}
	replicaConfig := loadReplicaConfig(options.configFile)
	log.Info("consumer configuration loaded", zap.String("sourceType", string(sourceType)), zap.String("consumerID", consumerID))

	memory := newMemoryUsage()
	confirmable := make(chan struct{}, 1)
	reader := newReader(ctx, upstreamURI, consumerID, replicaConfig, memory, confirmable)
	decoding := newDecodeConfig(upstreamURI, options.timezone, replicaConfig)
	upstreamDB := newUpstreamDB(ctx, upstreamURI)
	defer func() {
		if c != nil {
			return
		}
		reader.close()
		if upstreamDB != nil {
			if closeErr := upstreamDB.Close(); closeErr != nil {
				log.Warn("consumer upstream database close failed", zap.Error(closeErr))
			}
		}
	}()
	var streams map[int32]*decodeStream
	tables := simple.NewTableInfoCache()
	if sourceType == sourceKafka {
		partitions := reader.client.(*kafkaReader).partitions
		streams = make(map[int32]*decodeStream, len(partitions))
		// Protocol constructors reset table ID allocation. Prepare all partition
		// cursors before decoding starts.
		for id := range partitions {
			streams[id] = newDecodeStream(ctx, int(id), decoding, upstreamDB, tables)
		}
	}
	assembler := newAssembler(ctx, decoding, upstreamDB, replicaConfig, memory, streams, reader.client, tables)
	writer := newWriter(ctx, options.downstreamURI, replicaConfig, memory)
	return &consumer{
		reader:        reader,
		assembler:     assembler,
		writer:        writer,
		upstreamDB:    upstreamDB,
		profileServer: newProfileServer(options.enableProfiling),
	}
}

func parseRequiredURI(rawURI string) *url.URL {
	if rawURI == "" {
		log.Panic("consumer URI is required", zap.String("rawURI", rawURI))
	}
	uri, err := url.Parse(rawURI)
	if err != nil {
		log.Panic("consumer URI parsing failed", zap.String("rawURI", rawURI), zap.Error(err))
	}
	uri.Scheme = strings.ToLower(uri.Scheme)
	if uri.Scheme == "" {
		log.Panic("consumer URI must include a scheme", zap.String("rawURI", rawURI))
	}
	return uri
}

func sourceTypeFromURI(uri *url.URL) sourceType {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return sourceKafka
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return sourcePulsar
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return sourceStorage
	}
	log.Panic("consumer upstream scheme is unsupported", zap.String("scheme", uri.Scheme))
	return ""
}

func loadReplicaConfig(configFile string) *config.ReplicaConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile == "" {
		return replicaConfig
	}
	if err := cmdutil.StrictDecodeFile(configFile, "consumer", replicaConfig); err != nil {
		log.Panic("consumer configuration loading failed", zap.String("configFile", configFile), zap.Error(err))
	}
	if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
		log.Panic("consumer filter rules are invalid", zap.Error(err))
	}
	return replicaConfig
}

// Resolve transport-specific defaults and requirements before constructing the assembler.
func newDecodeConfig(upstreamURI *url.URL, timezone string, replicaConfig *config.ReplicaConfig) *decodeConfig {
	source := sourceTypeFromURI(upstreamURI)
	protocolName := upstreamURI.Query().Get(config.ProtocolKey)
	switch source {
	case sourcePulsar:
		protocolName = cmp.Or(protocolName, putil.GetOrZero(replicaConfig.Sink.Protocol), "canal-json")
	case sourceStorage:
		protocolName = putil.GetOrZero(replicaConfig.Sink.Protocol)
	}
	protocol, err := config.ParseSinkProtocolFromString(protocolName)
	if err != nil {
		log.Panic("consumer protocol is invalid", zap.Error(err))
	}
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		log.Panic("consumer codec configuration is invalid", zap.Error(err))
	}
	if replicaConfig.Integrity != nil {
		codecConfig.EnableRowChecksum = replicaConfig.Integrity.Enabled()
	}
	switch source {
	case sourceStorage:
		if protocol == config.ProtocolCanalJSON {
			codecConfig.EnableTiDBExtension = true
		}
	default:
		switch protocol {
		case config.ProtocolCanalJSON, config.ProtocolDebezium:
			if !codecConfig.EnableTiDBExtension {
				log.Panic("consumer requires TiDB protocol extensions", zap.String("protocol", protocol.String()))
			}
		case config.ProtocolAvro, config.ProtocolDebeziumAvro:
			if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
				log.Panic("consumer requires TiDB protocol extensions and Avro watermarks", zap.String("protocol", protocol.String()))
			}
			if codecConfig.AvroConfluentSchemaRegistry == "" {
				log.Panic("consumer requires an Avro schema registry")
			}
		}
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		log.Panic("consumer time zone is invalid", zap.Error(err))
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		log.Panic("consumer requires Debezium schemas")
	}
	format := messageFormat
	if source == sourceStorage {
		format = rowFormat
	}
	return &decodeConfig{
		codec:  codecConfig,
		format: format,
		topic:  strings.Trim(upstreamURI.Path, "/"),
	}
}

func newUpstreamDB(ctx context.Context, upstreamURI *url.URL) *sql.DB {
	if upstreamURI.Scheme != config.KafkaScheme && upstreamURI.Scheme != config.KafkaSSLScheme {
		return nil
	}
	dsn := upstreamURI.Query().Get("upstream-tidb-dsn")
	if dsn == "" {
		return nil
	}
	upstreamDB, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Panic("consumer upstream database open failed", zap.Error(err))
	}
	defer func() {
		if err == nil {
			return
		}
		if closeErr := upstreamDB.Close(); closeErr != nil {
			log.Warn("consumer upstream database close failed", zap.Error(closeErr))
		}
	}()
	upstreamDB.SetMaxOpenConns(10)
	upstreamDB.SetMaxIdleConns(10)
	upstreamDB.SetConnMaxLifetime(10 * time.Minute)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err = upstreamDB.PingContext(ctx); err != nil {
		log.Panic("consumer upstream database connection failed", zap.Error(err))
	}
	return upstreamDB
}

func newProfileServer(enabled bool) *http.Server {
	if !enabled {
		return nil
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	return &http.Server{
		Addr:              profileAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func (c *consumer) consume(ctx context.Context) error {
	c.g, ctx = errgroup.WithContext(ctx)
	if c.profileServer != nil {
		c.g.Go(func() error {
			if err := c.profileServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("consumer profiling server failed", zap.Error(err))
				return err
			}
			return nil
		})
	}
	inputs := make(chan []*readData, 1)
	results := make(chan []*writeEvent, 1)
	writeDone := make(chan struct{})
	c.g.Go(func() error {
		return c.reader.start(ctx, inputs)
	})
	c.g.Go(func() error {
		return c.assembler.start(ctx, inputs, results)
	})
	c.g.Go(func() error {
		defer close(writeDone)
		return c.writer.start(ctx, results)
	})
	<-ctx.Done()
	// Stop dispatching before the caller closes the sink.
	<-writeDone
	return context.Cause(ctx)
}

func (c *consumer) stop() {
	if c.profileServer != nil {
		if closeErr := c.profileServer.Close(); closeErr != nil {
			log.Warn("consumer profiling server close failed", zap.Error(closeErr))
		}
	}
	_ = c.g.Wait()
	c.writer.close()
	c.assembler.close()
	c.reader.close()
	if c.upstreamDB != nil {
		if closeErr := c.upstreamDB.Close(); closeErr != nil {
			log.Warn("consumer upstream database close failed", zap.Error(closeErr))
		}
	}
}
