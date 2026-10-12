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
	"maps"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"slices"
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
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	maxMemoryBytes      = 1 << 30
	progressLogInterval = 5 * time.Second
)

type sourceType string

const (
	sourceKafka   sourceType = "kafka"
	sourcePulsar  sourceType = "pulsar"
	sourceStorage sourceType = "storage"
)

type consumer struct {
	g             *errgroup.Group
	reader        reader
	decoder       *decoderGroup
	assembler     *assembler
	writer        *writer
	upstreamDB    *sql.DB
	profileServer *http.Server
	progress      *inputProgress
	completed     chan struct{}
	confirmable   chan struct{}
}

func newConsumer(ctx context.Context, options *options) (c *consumer, err error) {
	if err := logger.InitLogger(&logger.Config{
		Level: options.logLevel,
		File:  options.logFile,
	}); err != nil {
		return nil, errors.WrapError(errors.ErrInternalCheckFailed, err, "initialize consumer logger")
	}
	version.LogVersionInfo("consumer")

	upstreamURI, err := parseRequiredURI("upstream-uri", options.upstreamURI)
	if err != nil {
		return nil, err
	}
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	if _, err := parseRequiredURI("downstream-uri", options.downstreamURI); err != nil {
		return nil, err
	}
	consumerID := options.consumerID
	if source != sourceStorage && strings.TrimSpace(consumerID) == "" {
		consumerID = "ticdc_consumer_" + uuid.NewString()
	}
	replicaConfig, err := loadReplicaConfig(options.configFile)
	if err != nil {
		return nil, err
	}
	log.Info("consumer configuration loaded", zap.String("sourceType", string(source)), zap.String("consumerID", consumerID))

	memory := &memoryUsage{}
	progress := &inputProgress{memory: memory}
	schemas := &schemaUsage{memory: memory}
	completed := make(chan struct{}, 1)
	confirmable := make(chan struct{}, 1)
	reader, streams, err := newReader(ctx, upstreamURI, consumerID, replicaConfig, progress, schemas, confirmable)
	if err != nil {
		return nil, err
	}
	var upstreamDB *sql.DB
	defer func() {
		if c != nil {
			return
		}
		reader.Close()
		if upstreamDB != nil {
			if closeErr := upstreamDB.Close(); closeErr != nil {
				log.Warn("consumer upstream database close failed", zap.Error(closeErr))
			}
		}
	}()
	decoding, err := newDecodeConfig(upstreamURI, options.timezone, replicaConfig, streams)
	if err != nil {
		return nil, err
	}
	upstreamDB, err = newUpstreamDB(ctx, upstreamURI)
	if err != nil {
		return nil, err
	}
	decoder := newDecoderGroup(schemas, completed)
	assembler, err := newAssembler(ctx, decoding, upstreamDB, replicaConfig, memory, schemas, decoder)
	if err != nil {
		return nil, err
	}
	writer, err := newWriter(ctx, options.downstreamURI, replicaConfig, progress, completed)
	if err != nil {
		return nil, err
	}
	return &consumer{
		reader:        reader,
		decoder:       decoder,
		assembler:     assembler,
		writer:        writer,
		upstreamDB:    upstreamDB,
		profileServer: newProfileServer(options.enableProfiling),
		progress:      progress,
		completed:     completed,
		confirmable:   confirmable,
	}, nil
}

func parseRequiredURI(name, rawURI string) (*url.URL, error) {
	if rawURI == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs(name + " is required")
	}
	uri, err := url.Parse(rawURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, putil.MaskSensitiveDataInURLError(err), "invalid "+name)
	}
	uri.Scheme = strings.ToLower(uri.Scheme)
	if uri.Scheme == "" {
		return nil, errors.ErrInvalidReplicaConfig.FastGenByArgs(name + " must include a URI scheme")
	}
	return uri, nil
}

func sourceTypeFromURI(uri *url.URL) (sourceType, error) {
	switch uri.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		return sourceKafka, nil
	case config.PulsarScheme, config.PulsarSSLScheme, config.PulsarHTTPScheme, config.PulsarHTTPSScheme:
		return sourcePulsar, nil
	case config.FileScheme, config.S3Scheme, config.GCSScheme, config.GSScheme,
		config.AzblobScheme, config.AzureScheme:
		return sourceStorage, nil
	default:
		return "", errors.ErrInvalidReplicaConfig.FastGenByArgs("unsupported upstream-uri scheme " + uri.Scheme)
	}
}

func loadReplicaConfig(configFile string) (*config.ReplicaConfig, error) {
	replicaConfig := config.GetDefaultReplicaConfig()
	if configFile == "" {
		return replicaConfig, nil
	}
	if err := cmdutil.StrictDecodeFile(configFile, "consumer", replicaConfig); err != nil {
		return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err, "decode consumer config")
	}
	if _, err := filter.VerifyTableRules(replicaConfig.Filter); err != nil {
		return nil, errors.WrapError(errors.ErrInvalidReplicaConfig, err, "verify consumer filter rules")
	}
	return replicaConfig, nil
}

// Return logical stream IDs with the reader so decoder initialization does not
// depend on transport-specific reader fields. An empty list uses one decoder.
func newReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, progress *inputProgress, schemas *schemaUsage, confirmable chan<- struct{}) (reader, []int32, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, nil, err
	}
	switch source {
	case sourceKafka:
		reader, err := newKafkaReader(ctx, upstreamURI, consumerID, progress, replicaConfig, confirmable)
		if err != nil {
			return nil, nil, err
		}
		return reader, slices.Collect(maps.Keys(reader.partitions)), nil
	case sourcePulsar:
		reader, err := newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, progress, confirmable)
		return reader, nil, err
	default:
		reader, err := newStorageReader(ctx, upstreamURI, replicaConfig, progress, schemas, confirmable)
		return reader, nil, err
	}
}

// Resolve transport-specific defaults and requirements before constructing the assembler.
func newDecodeConfig(upstreamURI *url.URL, timezone string, replicaConfig *config.ReplicaConfig, streams []int32) (*decodeConfig, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	protocolName := upstreamURI.Query().Get(config.ProtocolKey)
	switch source {
	case sourcePulsar:
		protocolName = cmp.Or(protocolName, putil.GetOrZero(replicaConfig.Sink.Protocol), "canal-json")
	case sourceStorage:
		protocolName = putil.GetOrZero(replicaConfig.Sink.Protocol)
	}
	protocol, err := config.ParseSinkProtocolFromString(protocolName)
	if err != nil {
		return nil, err
	}
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return nil, err
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
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension must be true")
			}
		case config.ProtocolAvro, config.ProtocolDebeziumAvro:
			if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension and avro-enable-watermark must be true")
			}
			if codecConfig.AvroConfluentSchemaRegistry == "" {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("schema-registry is required")
			}
		}
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		return nil, err
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("debezium-disable-schema must be false")
	}
	format := messageFormat
	if source == sourceStorage {
		format = rowFormat
	}
	return &decodeConfig{
		codec:   codecConfig,
		format:  format,
		topic:   strings.Trim(upstreamURI.Path, "/"),
		streams: streams,
	}, nil
}

func newUpstreamDB(ctx context.Context, upstreamURI *url.URL) (*sql.DB, error) {
	if upstreamURI.Scheme != config.KafkaScheme && upstreamURI.Scheme != config.KafkaSSLScheme {
		return nil, nil
	}
	dsn := upstreamURI.Query().Get("upstream-tidb-dsn")
	if dsn == "" {
		return nil, nil
	}
	upstreamDB, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "open consumer upstream TiDB")
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
		return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
	}
	return upstreamDB, nil
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
		listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", c.profileServer.Addr)
		if err != nil {
			return errors.WrapError(errors.ErrInternalCheckFailed, err, "listen for consumer profiling")
		}
		c.g.Go(func() error {
			if err := c.profileServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return errors.WrapError(errors.ErrInternalCheckFailed, err, "serve consumer profiling")
			}
			return nil
		})
	}
	results := make(chan *writeEvent, 64)
	writeDone := make(chan struct{})
	c.g.Go(func() error {
		return c.writer.start(ctx)
	})
	c.g.Go(func() error {
		return c.decoder.start(ctx)
	})
	c.g.Go(func() error {
		return c.confirm(ctx)
	})
	c.g.Go(func() error {
		defer close(results)
		return c.read(ctx, results)
	})
	c.g.Go(func() error {
		defer close(writeDone)
		return c.write(ctx, results)
	})
	<-ctx.Done()
	// Stop dispatching before the caller closes the sink.
	<-writeDone
	return nil
}

func (c *consumer) stop() error {
	if c.profileServer != nil {
		if closeErr := c.profileServer.Close(); closeErr != nil {
			log.Warn("consumer profiling server close failed", zap.Error(closeErr))
		}
	}
	err := c.g.Wait()
	c.writer.close()
	c.decoder.close()
	c.reader.Close()
	if c.upstreamDB != nil {
		if closeErr := c.upstreamDB.Close(); closeErr != nil {
			log.Warn("consumer upstream database close failed", zap.Error(closeErr))
		}
	}
	return err
}

func (c *consumer) read(ctx context.Context, results chan<- *writeEvent) error {
	for ctx.Err() == nil {
		result, err := c.next(ctx)
		if err != nil {
			return err
		}
		if result == nil {
			return errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an empty result")
		}
		select {
		case results <- result:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return context.Cause(ctx)
}

func (c *consumer) next(ctx context.Context) (*writeEvent, error) {
	a := c.assembler
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if result := a.next(); result != nil {
			return result, nil
		}
		decoded := a.decodePending()
		if decoded == nil {
			feedback := a.feedback()
			progress, err := c.reader.Advance(ctx, feedback)
			if err != nil {
				return nil, err
			}
			a.advance(feedback, progress)
			if progress.control != nil || (progress.boundary != nil && progress.boundary.reached) {
				continue
			}
			if result := a.next(); result != nil {
				return result, nil
			}
			if !progress.needsMoreInput && !a.needsMoreInput() {
				if err := c.waitMemory(ctx); err != nil {
					return nil, err
				}
			}
			data, err := c.reader.Read(ctx)
			if err != nil {
				return nil, err
			}
			if data == nil {
				return nil, errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an empty result")
			}
			decoded = a.decode(ctx, data)
		}
		for result, err := range decoded {
			if err != nil {
				return nil, err
			}
			progress, err := c.reader.Advance(ctx, result.feedback)
			if err != nil {
				return nil, err
			}
			if err := a.accept(ctx, result, progress); err != nil {
				return nil, err
			}
		}
	}
}

// Backpressure applies to new independent inputs. Retained caches alone must
// not prevent reading, and unfinished input boundaries bypass this wait.
func (c *consumer) waitMemory(ctx context.Context) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if c.progress.memory.used() < maxMemoryBytes || c.progress.received.Load() == c.progress.confirmed.Load() {
			return nil
		}
		changed := c.progress.memory.changes()
		// Subscribe before rechecking: a concurrent release either satisfies the
		// admission condition or closes changed.
		if c.progress.memory.used() < maxMemoryBytes || c.progress.received.Load() == c.progress.confirmed.Load() {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (c *consumer) write(ctx context.Context, results <-chan *writeEvent) error {
	progress := c.progress
	ticker := time.NewTicker(progressLogInterval)
	defer ticker.Stop()
	for {
		var (
			items []*writeEvent
			err   error
		)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result, ok := <-results:
			if !ok {
				return context.Cause(ctx)
			}
			items, err = c.assembler.assemble(ctx, results, result)
		case <-c.completed:
			items = c.assembler.collectDecoded()
		case <-ticker.C:
			log.Info("consumer progress", zap.Int64("receivedInputs", progress.received.Load()),
				zap.Int64("decodedRows", c.writer.decodedRows), zap.Int64("writtenRows", c.writer.writtenRows),
				zap.Int64("completedInputs", progress.confirmed.Load()), zap.Int("pendingDMLCount", len(c.writer.pendingDML)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int64("uncompletedInputs", progress.received.Load()-progress.confirmed.Load()), zap.Int64("bufferedBytes", progress.memory.used()))
			continue
		}
		if err != nil {
			return err
		}
		for _, item := range items {
			c.writer.consume(item)
		}
		if err := c.writer.flushDML(ctx); err != nil {
			return err
		}
	}
}

func (c *consumer) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.confirmable:
			if err := c.reader.Confirm(ctx); err != nil {
				return err
			}
		}
	}
}
