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
	"net/url"
	"slices"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
)

// Read and Confirm may run concurrently. Confirm tolerates retry after a
// partially successful confirmation. Close runs after both have stopped.
type reader interface {
	Read(ctx context.Context) (*readResult, error)
	Confirm(ctx context.Context) error
	BufferedBytes() int64
	Close() error
}

func newReader(ctx context.Context, upstreamURI *url.URL, consumerID, timezone string, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (reader, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	switch source {
	case sourceKafka:
		return newKafkaReader(ctx, upstreamURI, consumerID, timezone, replicaConfig, memory)
	case sourcePulsar:
		return newPulsarReader(ctx, upstreamURI, consumerID, timezone, replicaConfig, memory)
	default:
		return newStorageReader(ctx, upstreamURI, timezone, replicaConfig, memory)
	}
}

type readResult struct {
	dml          *event.DMLEvent
	ddl          *event.DDLEvent
	onFlush      func()
	bytes        int64
	watermark    uint64
	hasWatermark bool
	tableID      int64
}

type partition struct {
	decoder          codecCommon.Decoder
	watermark        uint64
	hasWatermark     bool
	records          []*ack
	cachedRecords    []*ack
	cachedUnreleased int
	paused           bool
	readSequence     uint64
	schemas          map[schemaKey]bool
	schemaPointers   map[*common.TableInfo]bool
}

type schemaKey struct {
	schema  string
	table   string
	version uint64
}

type readDDL struct {
	event  *event.DDLEvent
	record *ack
	bytes  int64
}

type readDML struct {
	event *event.DMLEvent
	bytes int64
}

// Shared read-side storage contains decoded input, never a downstream sink.
type readBuffer struct {
	memory      *memoryUsage
	protocol    config.Protocol
	partitions  map[int32]*partition
	pendingDML  []*readDML
	pendingDDL  []*readDDL
	dmlDirty    bool
	orderedDML  bool
	dmlBoundary uint64
}

func (b *readBuffer) newAck(ctx context.Context, bytes int64) (*ack, error) {
	if bytes < 0 || bytes > maxMemoryBytes-2*maxInFlightBytes {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer input exceeds its memory budget")
	}
	// Admit new input only when decoding and filtering can also make progress.
	if err := b.memory.reserve(ctx, bytes+2*maxInFlightBytes); err != nil {
		return nil, err
	}
	b.memory.release(2 * maxInFlightBytes)
	record := &ack{}
	record.memory.Store(bytes)
	record.refs.Store(1)
	b.memory.readBytes.Add(bytes)
	b.memory.records.Add(1)
	b.memory.received.Add(1)
	b.memory.effects.Add(1)
	return record, nil
}

func (b *readBuffer) queueDML(ctx context.Context, dml *event.DMLEvent, records []*ack, p *partition) error {
	if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
		return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
	}
	if p != nil {
		if err := b.trackSchema(ctx, p, dml.TableInfo); err != nil {
			return err
		}
	}
	bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64 + 256
	if bytes > maxInFlightBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer DML exceeds its in-flight byte limit")
	}
	if err := b.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	b.memory.effects.Add(int64(len(records)))
	for _, record := range records {
		record.refs.Add(1)
	}
	dml.AddPostFlushFunc(func() {
		for _, record := range records {
			record.refs.Add(-1)
			b.memory.effects.Add(-1)
		}
	})
	b.memory.readBytes.Add(bytes)
	b.pendingDML = append(b.pendingDML, &readDML{event: dml, bytes: bytes})
	b.dmlDirty = true
	return nil
}

func (b *readBuffer) trackSchema(ctx context.Context, p *partition, table *common.TableInfo) error {
	if table == nil {
		return nil
	}
	key := schemaKey{schema: table.GetSchemaName(), table: table.GetTableName(), version: table.GetUpdateTS()}
	unversioned := table.GetUpdateTS() == 0 && (b.protocol == config.ProtocolCanalJSON || b.protocol == config.ProtocolOpen)
	if unversioned {
		if p.schemaPointers[table] {
			return nil
		}
	} else if p.schemas[key] {
		return nil
	}
	data, err := table.Marshal()
	if err != nil {
		return errors.WrapError(errors.ErrCodecDecode, err, "measure consumer table metadata")
	}
	bytes := int64(len(data))*4 + 1024
	if err := b.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	if unversioned {
		p.schemaPointers[table] = true
	} else {
		p.schemas[key] = true
	}
	b.memory.readBytes.Add(bytes)
	return nil
}

func (b *readBuffer) queueDDL(ctx context.Context, ddl *event.DDLEvent, record *ack) error {
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
	if err := b.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	b.memory.effects.Add(1)
	record.refs.Add(1)
	b.memory.readBytes.Add(bytes)
	b.pendingDDL = append(b.pendingDDL, &readDDL{event: ddl, record: record, bytes: bytes})
	return nil
}

func (b *readBuffer) nextReady(watermark uint64) *readResult {
	if b.dmlDirty {
		if !b.orderedDML {
			slices.SortStableFunc(b.pendingDML, func(a, b *readDML) int { return cmp.Compare(a.event.CommitTs, b.event.CommitTs) })
		}
		b.dmlDirty = false
	}
	var ddl *readDDL
	if len(b.pendingDDL) != 0 {
		ddl = b.pendingDDL[0]
	}
	dmlBoundary := watermark
	if b.orderedDML {
		dmlBoundary = b.dmlBoundary
	}
	for index, dml := range b.pendingDML {
		if dml.event.CommitTs <= dmlBoundary && (ddl == nil || dml.event.CommitTs <= ddl.event.GetCommitTs()) {
			if index == 0 {
				b.pendingDML[0] = nil
				b.pendingDML = b.pendingDML[1:]
			} else {
				copy(b.pendingDML[index:], b.pendingDML[index+1:])
				b.pendingDML[len(b.pendingDML)-1] = nil
				b.pendingDML = b.pendingDML[:len(b.pendingDML)-1]
			}
			b.memory.readBytes.Add(-dml.bytes)
			return &readResult{dml: dml.event, bytes: dml.bytes}
		}
		if !b.orderedDML {
			break
		}
	}
	if ddl == nil {
		return nil
	}
	early := false
	switch timodel.ActionType(ddl.event.Type) {
	case timodel.ActionCreateSchema:
		early = true
	case timodel.ActionCreateTable:
		blocked := ddl.event.GetBlockedTables()
		early = blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal && len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID && len(ddl.event.GetBlockedTableNames()) == 0
	}
	if ddl.event.GetCommitTs() > watermark && !early && (!b.orderedDML || len(b.partitions) != 1) {
		return nil
	}
	record := ddl.record
	result := &readResult{ddl: ddl.event, bytes: ddl.bytes, onFlush: func() {
		record.refs.Add(-1)
		b.memory.effects.Add(-1)
	}}
	b.memory.readBytes.Add(-result.bytes)
	b.pendingDDL[0] = nil
	b.pendingDDL = b.pendingDDL[1:]
	return result
}
