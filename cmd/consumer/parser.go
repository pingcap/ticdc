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
	"iter"
	"net/url"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

// inputScope carries source-provided identity and ordering facts into parsed work.
// The decoder copies these facts into tasks. The conductor advances shared
// boundaries through Reader.Advance and Reader.Boundary.
// Encoded contents and transport positions remain in the source implementation.
type inputScope struct {
	// Kafka partition ID, Pulsar partition-topic index, or zero for storage.
	// Selects the decoder and distinguishes per-table input groups.
	stream int32
	// Proof that preceding DDLs for a row are known: reached grants source-side
	// admission, or watermark covers the row's commitTs. Nil adds no proof;
	// the conductor can still use global progress or a later scoped DDL.
	dmlBoundary *readBoundary
	// Proof that the affected inputs have been read through this DDL. Nil adds
	// no proof; the conductor uses global progress or requests a scoped boundary.
	// Both boundaries describe reading; downstream DDL fences still apply.
	ddlBoundary *readBoundary
	// Kafka marks non-P0 DDLs as copies. They wait for the canonical P0 DDL;
	// its arrival lets the conductor replace the copy.
	ddlCopy bool
	// inputOrder preserves DDL arrival order. Pulsar uses commitOrder to stably
	// sort pending DDLs by commitTs across partition topics.
	ddlOrder readOrder
}

type task interface{ isTask() }

func (*decodeTask) isTask()     {}
func (*inputDDL) isTask()       {}
func (*inputWatermark) isTask() {}
func (*watermark) isTask()      {}

type inputDDL struct {
	ddl          *event.DDLEvent
	scope        inputScope
	registration ddlRegistration
}

type ddlRegistration interface {
	done()
}

type inputWatermark struct {
	watermark        uint64
	stream           int32
	sourceCheckpoint bool // Topic-wide progress, matching the legacy Pulsar consumer.
}

// An inputDecoder owns the decoder for each source stream and the shared
// simple schema cache. Parallel conversion workers receive captured messages,
// never these decoders.
type inputDecoder struct {
	codecConfig       *codecCommon.Config
	upstreamDB        *sql.DB
	selectors         *columnselector.ColumnSelectors
	decoders          map[int32]codecCommon.Decoder
	schemas           *simple.TableInfoCache
	checkpoint        *atomic.Uint64
	cleanedCheckpoint uint64
}

func newInputDecoder(
	ctx context.Context,
	codecConfig *codecCommon.Config,
	replicaConfig *config.ReplicaConfig,
	streams []streamSpec,
	upstreamURI *url.URL,
	checkpoint *atomic.Uint64,
) *inputDecoder {
	var selectors *columnselector.ColumnSelectors
	if codecConfig.Protocol == config.ProtocolCsv {
		var err error
		selectors, err = columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
	}
	tables := simple.NewTableInfoCache()
	upstreamDB := newUpstreamDB(ctx, upstreamURI)
	decoders := make(map[int32]codecCommon.Decoder, len(streams))
	for _, stream := range streams {
		decoders[stream.id] = newSourceDecoder(ctx, int(stream.id), codecConfig, stream.topic, upstreamDB, tables)
	}
	return &inputDecoder{
		codecConfig: codecConfig,
		upstreamDB:  upstreamDB,
		selectors:   selectors,
		decoders:    decoders,
		schemas:     tables,
		checkpoint:  checkpoint,
	}
}

func (p *inputDecoder) close() {
	if p.upstreamDB == nil {
		return
	}
	if err := p.upstreamDB.Close(); err != nil {
		log.Warn("consumer upstream database close failed", zap.Error(err))
	}
}

func (p *inputDecoder) messageTask(scope inputScope, message *codecCommon.DMLMessage) *decodeTask {
	return &decodeTask{
		message: message, name: event.SchemaTableName{SchemaName: message.Schema, TableName: message.Table},
		commitTs: message.GetCommitTs(), tableID: message.TableID, stream: scope.stream,
		boundary: scope.dmlBoundary, bytes: message.ApproximateSize, done: make(chan struct{}),
	}
}

func (p *inputDecoder) decodeMessage(
	ctx context.Context,
	scope inputScope,
	key, value []byte,
	memory *memoryBudget,
) iter.Seq2[task, error] {
	return func(yield func(task, error) bool) {
		p.cleanHistory()
		stream := p.decoders[scope.stream]
		stream.AddKeyValue(key, value)
		for {
			if err := context.Cause(ctx); err != nil {
				yield(nil, err)
				return
			}
			kind, more := stream.HasNext()
			if !more {
				break
			}
			switch kind {
			case codecCommon.MessageTypeRow:
				message := stream.NextDMLMessage()
				if message == nil {
					// Simple holds the row until its schema version arrives. Account
					// its captured size now and transfer it to the task at the DDL.
					pending := stream.(*simple.Decoder).PendingDMLMessage
					memory.reserve(pending.ApproximateSize)
					continue
				}
				memory.reserve(message.ApproximateSize)
				task := p.messageTask(scope, message)
				if !yield(task, nil) {
					return
				}
			case codecCommon.MessageTypeDDL:
				ddl := stream.NextDDLEvent()
				if decoder, ok := stream.(*simple.Decoder); ok {
					// The DDL carries the missing schema. These rows belong to
					// this input's scope, and their quota was reserved when cached.
					for _, message := range decoder.GetCachedMessages() {
						task := p.messageTask(scope, message)
						if !yield(task, nil) {
							return
						}
					}
					if ddl.Query == "" {
						continue
					}
				}
				if !yield(&inputDDL{ddl: ddl, scope: scope}, nil) {
					return
				}
			case codecCommon.MessageTypeResolved:
				if !yield(&inputWatermark{watermark: stream.NextResolvedEvent(), stream: scope.stream}, nil) {
					return
				}
			}
		}
	}
}

// Sources associate a fully decoded transport position with the maximum timestamp
// observed in its prefix. Checkpoint coverage then proves the entire prefix done.
func taskTimestamp(work task) uint64 {
	switch work := work.(type) {
	case *decodeTask:
		return work.commitTs
	case *inputDDL:
		return work.ddl.GetCommitTs()
	case *inputWatermark:
		return work.watermark
	case *watermark:
		return work.watermark
	}
	return 0
}

// Cache access and reclamation stay on the decoding goroutine. The conductor
// publishes only the downstream checkpoint; captured conversions keep their
// metadata alive.
func (p *inputDecoder) cleanHistory() {
	checkpoint := p.checkpoint.Load()
	if checkpoint > p.cleanedCheckpoint {
		p.schemas.ReleaseBefore(checkpoint)
		p.cleanedCheckpoint = checkpoint
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

func newSourceDecoder(
	ctx context.Context,
	id int,
	codecConfig *codecCommon.Config,
	topic string,
	upstreamDB *sql.DB,
	tables simple.TableInfoProvider,
) codecCommon.Decoder {
	var (
		decoder codecCommon.Decoder
		err     error
	)
	if codecConfig.Protocol == config.ProtocolSimple {
		decoder, err = simple.NewDecoder(ctx, codecConfig, upstreamDB, tables)
	}
	if codecConfig.Protocol != config.ProtocolSimple {
		decoder, err = codec.NewEventDecoder(ctx, id, codecConfig, topic, upstreamDB)
	}
	if err != nil {
		log.Panic("consumer event decoder initialization failed", zap.String("protocol", codecConfig.Protocol.String()), zap.Error(err))
	}
	return decoder
}
