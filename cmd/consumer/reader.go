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
	"slices"
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
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

// Reader-owned decoding references stay outstanding until every derived effect has
// been registered. Each effect is released only after its downstream write.
type inputRecord struct {
	pending atomic.Int64
	bytes   atomic.Int64
}

type readResult struct {
	dml          *event.DMLEvent
	ddl          *event.DDLEvent
	onFlush      func()
	bytes        int64
	watermark    uint64
	hasWatermark bool
	tableID      int64
	force        bool
}

type bufferUsage struct {
	bytes         atomic.Int64
	readBytes     atomic.Int64
	records       atomic.Int64
	effects       atomic.Int64
	received      atomic.Int64
	confirmed     atomic.Int64
	externalBytes func() int64
}

func (m *bufferUsage) reserve(bytes int64) error {
	for {
		used := m.bytes.Load()
		external := int64(0)
		if m.externalBytes != nil {
			external = m.externalBytes()
		}
		if bytes < 0 || bytes > maxBufferedBytes || used+bytes+external > maxBufferedBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("consumer buffer limit exceeded; input remains unconfirmed")
		}
		if m.bytes.CompareAndSwap(used, used+bytes) {
			return nil
		}
	}
}

type partition struct {
	decoder          codeccommon.Decoder
	watermark        uint64
	hasWatermark     bool
	records          []*inputRecord
	cachedRecords    []*inputRecord
	cachedUnreleased int
	paused           bool
	schemas          map[schemaKey]bool
	schemaPointers   map[*common.TableInfo]bool
}

type schemaKey struct {
	schema  string
	table   string
	version uint64
}

type ddlKey struct {
	commitTs uint64
	schema   string
	table    string
}

type readDDL struct {
	key        ddlKey
	event      *event.DDLEvent
	partitions map[int32]bool
	records    []*inputRecord
	delivered  bool
	bytes      int64
}

type readDML struct {
	event *event.DMLEvent
	bytes int64
}

// Shared read-side storage contains decoded input, never a downstream sink.
type readBuffer struct {
	memory      *bufferUsage
	protocol    config.Protocol
	partitions  map[int32]*partition
	pendingDML  []*readDML
	pendingDDL  []*readDDL
	ddls        map[ddlKey]*readDDL
	schemaBytes int64
	schemaCount int
	dmlDirty    bool
	ddlDirty    bool
}

func (b *readBuffer) newRecord(bytes int64) (*inputRecord, error) {
	if b.memory.records.Load() >= maxRecords {
		return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer input exceeds its resource limit; input remains unconfirmed")
	}
	if err := b.memory.reserve(bytes); err != nil {
		return nil, err
	}
	record := &inputRecord{}
	record.bytes.Store(bytes)
	record.pending.Store(1)
	b.memory.readBytes.Add(bytes)
	b.memory.records.Add(1)
	b.memory.received.Add(1)
	b.memory.effects.Add(1)
	return record, nil
}

func (b *readBuffer) queueDML(dml *event.DMLEvent, records []*inputRecord, p *partition) error {
	if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
		return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
	}
	if p != nil {
		if err := b.trackSchema(p, dml.TableInfo); err != nil {
			return err
		}
	}
	bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64 + 256
	if bytes > maxInFlightBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer DML exceeds its in-flight byte limit")
	}
	if err := b.memory.reserve(bytes); err != nil {
		return err
	}
	if b.memory.effects.Add(int64(len(records))) > maxEffects {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer decoded input exceeds its effect limit")
	}
	for _, record := range records {
		record.pending.Add(1)
	}
	dml.AddPostFlushFunc(func() {
		for _, record := range records {
			record.pending.Add(-1)
			b.memory.effects.Add(-1)
		}
	})
	b.memory.readBytes.Add(bytes)
	b.pendingDML = append(b.pendingDML, &readDML{event: dml, bytes: bytes})
	b.dmlDirty = true
	return nil
}

func (b *readBuffer) trackSchema(p *partition, table *common.TableInfo) error {
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
	if b.schemaCount >= maxSchemas {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its count limit")
	}
	data, err := table.Marshal()
	if err != nil {
		return errors.WrapError(errors.ErrCodecDecode, err, "measure consumer table metadata")
	}
	bytes := int64(len(data))*4 + 1024
	if b.schemaBytes+bytes > maxSchemaBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its byte limit")
	}
	if err := b.memory.reserve(bytes); err != nil {
		return err
	}
	if unversioned {
		p.schemaPointers[table] = true
	} else {
		p.schemas[key] = true
	}
	b.schemaBytes += bytes
	b.schemaCount++
	b.memory.readBytes.Add(bytes)
	return nil
}

func (b *readBuffer) queueDDL(ddl *event.DDLEvent, record *inputRecord, partitionID int32, canonical bool) error {
	key := ddlKey{commitTs: ddl.GetCommitTs(), schema: ddl.GetSchemaName(), table: ddl.GetTableName()}
	pending := b.ddls[key]
	if pending != nil && pending.delivered {
		return nil
	}
	if pending == nil {
		bytes := int64(len(ddl.Query) + len(key.schema) + len(key.table) + 1024)
		if err := b.memory.reserve(bytes); err != nil {
			return err
		}
		pending = &readDDL{key: key, partitions: make(map[int32]bool), bytes: bytes}
		b.memory.readBytes.Add(bytes)
		b.ddls[key] = pending
		b.pendingDDL = append(b.pendingDDL, pending)
		b.ddlDirty = true
	}
	if err := b.memory.reserve(48); err != nil {
		return err
	}
	b.memory.readBytes.Add(48)
	pending.bytes += 48
	pending.partitions[partitionID] = true
	pending.records = append(pending.records, record)
	if b.memory.effects.Add(1) > maxEffects {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer DDL exceeds its effect limit")
	}
	record.pending.Add(1)
	if canonical {
		pending.event = ddl
	}
	return nil
}

func (b *readBuffer) nextReady(watermark uint64) (*readResult, error) {
	if b.dmlDirty {
		slices.SortStableFunc(b.pendingDML, func(a, b *readDML) int { return cmp.Compare(a.event.CommitTs, b.event.CommitTs) })
		b.dmlDirty = false
	}
	if b.ddlDirty {
		slices.SortStableFunc(b.pendingDDL, func(a, b *readDDL) int { return cmp.Compare(a.key.commitTs, b.key.commitTs) })
		b.ddlDirty = false
	}
	var ddl *readDDL
	if len(b.pendingDDL) != 0 {
		ddl = b.pendingDDL[0]
	}
	if len(b.pendingDML) != 0 {
		dml := b.pendingDML[0]
		if dml.event.CommitTs <= watermark && (ddl == nil || dml.event.CommitTs <= ddl.key.commitTs) {
			b.pendingDML[0] = nil
			b.pendingDML = b.pendingDML[1:]
			b.memory.readBytes.Add(-dml.bytes)
			return &readResult{dml: dml.event, bytes: dml.bytes}, nil
		}
	}
	if ddl == nil {
		return nil, nil
	}
	early := false
	if ddl.event != nil {
		switch timodel.ActionType(ddl.event.Type) {
		case timodel.ActionCreateSchema:
			early = true
		case timodel.ActionCreateTable:
			blocked := ddl.event.GetBlockedTables()
			early = blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal && len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID && len(ddl.event.GetBlockedTableNames()) == 0
		}
	}
	if ddl.key.commitTs > watermark && !early {
		return nil, nil
	}
	expected := len(b.partitions)
	if b.protocol == config.ProtocolCanalJSON {
		expected = 1
	}
	if len(ddl.partitions) != expected {
		if ddl.key.commitTs > watermark {
			return nil, nil
		}
		return nil, errors.ErrCodecDecode.FastGenByArgs("DDL is missing an expected copy at the complete boundary")
	}
	if ddl.event == nil {
		return nil, errors.ErrCodecDecode.FastGenByArgs("DDL is missing its canonical event")
	}
	records := ddl.records
	result := &readResult{ddl: ddl.event, bytes: ddl.bytes, onFlush: func() {
		for _, record := range records {
			record.pending.Add(-1)
			b.memory.effects.Add(-1)
		}
	}}
	ddl.delivered = true
	ddl.event = nil
	ddl.records = nil
	ddl.partitions = nil
	ddl.bytes = int64(len(ddl.key.schema) + len(ddl.key.table) + 192)
	if err := b.memory.reserve(ddl.bytes); err != nil {
		return nil, err
	}
	b.memory.readBytes.Add(ddl.bytes - result.bytes)
	b.pendingDDL[0] = nil
	b.pendingDDL = b.pendingDDL[1:]
	return result, nil
}

func (b *readBuffer) advance(watermark uint64) {
	for key, ddl := range b.ddls {
		if ddl.delivered && key.commitTs < watermark {
			b.memory.bytes.Add(-ddl.bytes)
			b.memory.readBytes.Add(-ddl.bytes)
			delete(b.ddls, key)
		}
	}
}
