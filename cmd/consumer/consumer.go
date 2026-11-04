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
	"context"
	"database/sql"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // pprof is served on 127.0.0.1 only and gated by --enable-profiling.
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
	"github.com/uber-go/atomic"
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
	conductor     *conductor
	decoder       *inputDecoder
	writer        *writer
	upstreamDB    *sql.DB
	profileServer *http.Server
}

func newConsumer(ctx context.Context, options *options) (c *consumer) {
	if err := logger.InitLogger(&logger.Config{
		Level: "info",
		File:  options.logFile,
	}); err != nil {
		log.Panic("consumer logger initialization failed", zap.Error(err))
	}
	version.LogVersionInfo("consumer")

	upstreamURI := parseRequiredURI(options.upstreamURI)
	sourceType := sourceTypeFromURI(upstreamURI)

	consumerID := options.consumerID
	if strings.TrimSpace(consumerID) == "" {
		consumerID = "consumer_" + uuid.NewString()
	}
	replicaConfig := loadReplicaConfig(options.configFile, upstreamURI)
	codecConfig := newCodecConfig(upstreamURI, replicaConfig)
	logConsumerConfig(sourceType, consumerID, upstreamURI, options.downstreamURI, replicaConfig, codecConfig)

	checkpoint := new(atomic.Uint64)
	memory := newMemoryBudget()
	confirmable := make(chan struct{}, 1)
	upstreamDB := newUpstreamDB(ctx, upstreamURI)
	tables := simple.NewTableInfoCache()
	reader := newReader(ctx, upstreamURI, consumerID, replicaConfig, codecConfig, memory, confirmable, checkpoint, upstreamDB, tables)
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
	decoder := newInputDecoder(reader.client.Decoders(), tables, checkpoint)
	conductor := newConductor(memory, reader.client, decoder, codecConfig, checkpoint, confirmable)
	writer := newWriter(ctx, options.downstreamURI, replicaConfig, memory)
	return &consumer{
		reader:        reader,
		decoder:       decoder,
		conductor:     conductor,
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

func logConsumerConfig(
	sourceType sourceType,
	consumerID string,
	upstreamURI *url.URL,
	downstreamURI string,
	replicaConfig *config.ReplicaConfig,
	codecConfig *codecCommon.Config,
) {
	fields := []zap.Field{
		zap.String("sourceType", string(sourceType)),
		zap.String("consumerID", consumerID),
		zap.String("upstreamURI", putil.MaskSensitiveDataInURI(upstreamURI.String())),
		zap.String("downstreamURI", putil.MaskSensitiveDataInURI(downstreamURI)),
		zap.String("protocol", codecConfig.Protocol.String()),
		zap.String("timeZone", codecConfig.TimeZone.String()),
		zap.Bool("caseSensitive", putil.GetOrZero(replicaConfig.CaseSensitive)),
		zap.Bool("enableTiDBExtension", codecConfig.EnableTiDBExtension),
		zap.Bool("avroEnableWatermark", codecConfig.AvroEnableWatermark),
	}
	if codecConfig.AvroConfluentSchemaRegistry != "" {
		fields = append(fields,
			zap.String("schemaRegistry", putil.MaskSensitiveDataInURI(codecConfig.AvroConfluentSchemaRegistry)))
	}
	switch sourceType {
	case sourceKafka:
		fields = append(fields,
			zap.String("topic", strings.Trim(upstreamURI.Path, "/")),
			zap.Int("maxMessageBytes", codecConfig.MaxMessageBytes),
			zap.Int("maxBatchSize", codecConfig.MaxBatchSize))
	case sourcePulsar:
		fields = append(fields, zap.String("topic", strings.Trim(upstreamURI.Path, "/")))
	case sourceStorage:
		fields = append(fields,
			zap.Int("fileIndexWidth", putil.GetOrZero(replicaConfig.Sink.FileIndexWidth)),
			zap.Any("dateSeparator", putil.GetOrZero(replicaConfig.Sink.DateSeparator)))
		if codecConfig.Protocol == config.ProtocolCsv {
			fields = append(fields,
				zap.String("csvDelimiter", codecConfig.Delimiter),
				zap.String("csvTerminator", codecConfig.Terminator),
				zap.String("csvNull", codecConfig.NullString),
				zap.Bool("csvIncludeCommitTs", codecConfig.IncludeCommitTs),
				zap.Bool("csvOutputOldValue", codecConfig.OutputOldValue))
		}
	}
	log.Info("consumer configuration", fields...)
}

func loadReplicaConfig(configFile string, upstreamURI *url.URL) *config.ReplicaConfig {
	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile != "" {
		if err := cmdutil.StrictDecodeFile(configFile, "consumer", replicaConfig); err != nil {
			log.Panic("consumer configuration loading failed", zap.String("configFile", configFile), zap.Error(err))
		}
		if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
			log.Panic("consumer filter rules are invalid", zap.Error(err))
		}
	}
	if err := replicaConfig.ValidateAndAdjust(upstreamURI); err != nil {
		log.Panic("consumer configuration is invalid", zap.Error(err))
	}
	return replicaConfig
}

// newCodecConfig builds the decoder config. loadReplicaConfig has already
// applied the sink URI parameters, including protocol, to replicaConfig.Sink.

func newCodecConfig(upstreamURI *url.URL, replicaConfig *config.ReplicaConfig) *codecCommon.Config {
	protocol, err := config.ParseSinkProtocolFromString(putil.GetOrZero(replicaConfig.Sink.Protocol))
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

	// The upstream encoder only emits commit-ts, DDL and resolved events when
	// these flags are enabled; the decoder cannot reconstruct them.
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
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		log.Panic("consumer requires Debezium schemas")
	}
	return codecConfig
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
	return &http.Server{
		Addr:              profileAddress,
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
	tasks := make(chan []task, 1)
	writeDone := make(chan struct{})
	c.g.Go(func() error {
		return c.reader.start(ctx, c.decoder, tasks)
	})
	c.g.Go(func() error {
		defer close(writeDone)
		return c.conductor.start(ctx, tasks, c.writer)
	})
	c.g.Go(func() error {
		return c.writer.start(ctx)
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
	c.conductor.close()
	c.reader.close()
	if c.upstreamDB != nil {
		if closeErr := c.upstreamDB.Close(); closeErr != nil {
			log.Warn("consumer upstream database close failed", zap.Error(closeErr))
		}
	}
}
