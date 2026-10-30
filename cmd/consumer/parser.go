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

type readFormat uint8

const (
	messageFormat readFormat = iota
	rowFormat
)

type decodeConfig struct {
	codec  *codecCommon.Config
	format readFormat
	topic  string
}

// inputScope carries source-provided identity and ordering facts into parsed work.
// The parser copies these facts into tasks. The conductor advances shared
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
func (*resumeTask) isTask()     {}

type inputDDL struct {
	ddl        *event.DDLEvent
	scope      inputScope
	registered func()
}

type inputWatermark struct {
	watermark        uint64
	stream           int32
	sourceCheckpoint bool // Production progress whose read coverage is published separately.
}

type resumeTask struct{ task *decodeTask }

// A source owns its codec cursors and schema-dependent row cache. Parallel
// conversion workers receive captured messages, never these cursors.
type inputParser struct {
	memory            *memoryUsage
	codecConfig       *codecCommon.Config
	selectors         *columnselector.ColumnSelectors
	streams           map[int32]*decodeStream
	schemas           *simple.TableInfoCache
	checkpoint        *atomic.Uint64
	cleanedCheckpoint uint64
}

func newInputParser(ctx context.Context, decoding *decodeConfig, upstreamDB *sql.DB, replicaConfig *config.ReplicaConfig, streams map[int32]*decodeStream, schemas *simple.TableInfoCache) *inputParser {
	var selectors *columnselector.ColumnSelectors
	if decoding.codec.Protocol == config.ProtocolCsv {
		var err error
		selectors, err = columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
	}
	if streams == nil && decoding.format == messageFormat {
		streams = map[int32]*decodeStream{0: newDecodeStream(ctx, 0, decoding, upstreamDB, schemas)}
	}
	return &inputParser{codecConfig: decoding.codec, selectors: selectors, streams: streams, schemas: schemas}
}

func (p *inputParser) messageTask(scope inputScope, message *codecCommon.DMLMessage) *decodeTask {
	p.memory.reserve(message.ApproximateSize)
	return &decodeTask{
		message: message, name: event.SchemaTableName{SchemaName: message.Schema, TableName: message.Table},
		commitTs: message.GetCommitTs(), tableID: message.TableID, stream: scope.stream,
		boundary: scope.dmlBoundary, bytes: message.ApproximateSize, done: make(chan struct{}),
	}
}

func (p *inputParser) parseMessage(ctx context.Context, scope inputScope, key, value []byte) iter.Seq2[task, error] {
	return func(yield func(task, error) bool) {
		p.cleanHistory()
		stream := p.streams[scope.stream]
		stream.decoder.AddKeyValue(key, value)
		for {
			if err := context.Cause(ctx); err != nil {
				yield(nil, err)
				return
			}
			kind, more := stream.decoder.HasNext()
			if !more {
				break
			}
			switch kind {
			case codecCommon.MessageTypeRow:
				message := stream.decoder.NextDMLMessage()
				waiting := message == nil
				if waiting {
					message = stream.decoder.(*simple.Decoder).PendingDMLMessage
				}
				task := p.messageTask(scope, message)
				task.waiting = waiting
				if waiting {
					if stream.cachedRecords == nil {
						stream.cachedRecords = make(map[*codecCommon.DMLMessage]*decodeTask)
					}
					stream.cachedRecords[message] = task
				}
				if !yield(task, nil) {
					return
				}
			case codecCommon.MessageTypeDDL:
				ddl := stream.decoder.NextDDLEvent()
				if p.codecConfig.Protocol == config.ProtocolSimple {
					for _, source := range p.streams {
						if len(source.cachedRecords) == 0 {
							continue
						}
						for _, message := range source.decoder.(*simple.Decoder).GetCachedMessages() {
							cached := source.cachedRecords[message]
							delete(source.cachedRecords, message)
							if !yield(&resumeTask{task: cached}, nil) {
								return
							}
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
				if !yield(&inputWatermark{watermark: stream.decoder.NextResolvedEvent(), stream: scope.stream}, nil) {
					return
				}
			}
		}
	}
}

// Sources associate a fully parsed transport position with the maximum timestamp
// observed in its prefix. Checkpoint coverage then proves the entire prefix done.
func taskTimestamp(work task) uint64 {
	switch work := work.(type) {
	case *decodeTask:
		return work.commitTs
	case *resumeTask:
		return work.task.commitTs
	case *inputDDL:
		return work.ddl.GetCommitTs()
	case *inputWatermark:
		return work.watermark
	case *watermark:
		return work.watermark
	}
	return 0
}

// Cache access and reclamation stay on the parser goroutine. The conductor publishes
// only the downstream checkpoint; captured conversions keep their metadata alive.
func (p *inputParser) cleanHistory() {
	if p.codecConfig.Protocol != config.ProtocolSimple || p.checkpoint == nil {
		return
	}
	checkpoint := p.checkpoint.Load()
	if checkpoint > p.cleanedCheckpoint {
		p.schemas.ReleaseBefore(checkpoint)
		p.cleanedCheckpoint = checkpoint
	}
}

type decodeStream struct {
	decoder       codecCommon.Decoder
	cachedRecords map[*codecCommon.DMLMessage]*decodeTask
}

func newDecodeStream(ctx context.Context, id int, decoding *decodeConfig, upstreamDB *sql.DB, tables simple.TableInfoProvider) *decodeStream {
	var (
		decoder codecCommon.Decoder
		err     error
	)
	if decoding.codec.Protocol == config.ProtocolSimple {
		decoder, err = simple.NewDecoder(ctx, decoding.codec, upstreamDB, tables)
	}
	if decoding.codec.Protocol != config.ProtocolSimple {
		decoder, err = codec.NewEventDecoder(ctx, id, decoding.codec, decoding.topic, upstreamDB)
	}
	if err != nil {
		log.Panic("consumer event decoder initialization failed", zap.String("protocol", decoding.codec.Protocol.String()), zap.Error(err))
	}
	return &decodeStream{
		decoder: decoder,
	}
}
