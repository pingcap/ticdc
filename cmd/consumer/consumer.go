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
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	maxMemoryBytes      = 1 << 30
	progressLogInterval = 5 * time.Second
)

type consumer struct {
	wg           sync.WaitGroup
	reader       reader
	assembler    *assembler
	writer       *writer
	upstreamDB   *sql.DB
	progress     *inputProgress
	completed    chan struct{}
	confirmable  chan struct{}
	progressTick <-chan time.Time
}

func newConsumer(ctx context.Context, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) (c *consumer, err error) {
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
				log.Warn("consumer upstream database close failed", zap.Error(errors.WrapError(errors.ErrMySQLConnectionError, closeErr, "close consumer upstream TiDB")))
			}
		}
	}()
	decoding, err := newDecodeConfig(upstreamURI, timezone, replicaConfig, streams)
	if err != nil {
		return nil, err
	}
	switch upstreamURI.Scheme {
	case config.KafkaScheme, config.KafkaSSLScheme:
		if dsn := upstreamURI.Query().Get("upstream-tidb-dsn"); dsn != "" {
			upstreamDB, err = sql.Open("mysql", dsn)
			if err != nil {
				return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "open consumer upstream TiDB")
			}
			upstreamDB.SetMaxOpenConns(10)
			upstreamDB.SetMaxIdleConns(10)
			upstreamDB.SetConnMaxLifetime(10 * time.Minute)
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := upstreamDB.PingContext(ctx); err != nil {
				return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
			}
		}
	}
	assembler, err := newAssembler(ctx, decoding, upstreamDB, replicaConfig, memory, schemas, completed)
	if err != nil {
		return nil, err
	}
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	target, err := sink.New(ctx, &config.ChangefeedConfig{
		ChangefeedID:           changefeedID,
		SinkURI:                downstreamURI,
		SinkConfig:             replicaConfig.Sink,
		CaseSensitive:          putil.GetOrZero(replicaConfig.CaseSensitive),
		EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		return nil, err
	}
	progressTick := time.Tick(progressLogInterval)
	return &consumer{
		reader:       reader,
		assembler:    assembler,
		upstreamDB:   upstreamDB,
		progress:     progress,
		completed:    completed,
		confirmable:  confirmable,
		progressTick: progressTick,
		writer: &writer{
			downstream:   target,
			memory:       memory,
			progress:     progress,
			completed:    completed,
			mutations:    make(map[mutationKey]*writeBatch),
			progressTick: progressTick,
			ddlJobs:      make(chan *writeEvent, 1),
			ddlDone:      make(chan struct{}, 1),
		},
	}, nil
}

func (c *consumer) stop() {
	c.reader.Close()
	if c.upstreamDB != nil {
		if closeErr := c.upstreamDB.Close(); closeErr != nil {
			log.Warn("consumer upstream database close failed", zap.Error(errors.WrapError(errors.ErrMySQLConnectionError, closeErr, "close consumer upstream TiDB")))
		}
	}
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
		decoded := a.decodePending(ctx)
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
			items, err = c.assembler.collectDecoded(ctx)
		case <-c.progressTick:
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
			if err := c.writer.consume(ctx, item); err != nil {
				return err
			}
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
	switch source {
	case sourceKafka:
		switch protocol {
		case config.ProtocolOpen, config.ProtocolCanalJSON, config.ProtocolAvro, config.ProtocolSimple, config.ProtocolDebezium, config.ProtocolDebeziumAvro:
		default:
			return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("unsupported Kafka protocol " + protocol.String())
		}
	case sourcePulsar:
		if protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar consumer requires canal-json")
		}
	case sourceStorage:
		if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrStorageSinkInvalidConfig.FastGenByArgs("Storage consumer requires csv or canal-json")
		}
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
