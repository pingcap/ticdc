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

// Shared read-side storage contains decoded input, never a downstream sink.
type readBuffer struct {
	memory      *memoryUsage
	protocol    config.Protocol
	partitions  map[int32]*partition
	pendingDML  []*readResult
	pendingDDL  []*readDDL
	dmlDirty    bool
	orderedDML  bool
	dmlBoundary uint64
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
	for _, record := range records {
		record.refs.Add(1)
	}
	dml.AddPostFlushFunc(func() {
		for _, record := range records {
			record.refs.Add(-1)
		}
	})
	b.pendingDML = append(b.pendingDML, &readResult{dml: dml, bytes: bytes})
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
	return nil
}

func (b *readBuffer) queueDDL(ctx context.Context, ddl *event.DDLEvent, record *ack) error {
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
	if err := b.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	record.refs.Add(1)
	b.pendingDDL = append(b.pendingDDL, &readDDL{event: ddl, record: record, bytes: bytes})
	return nil
}

func (b *readBuffer) nextReady(watermark uint64) *readResult {
	if b.dmlDirty {
		if !b.orderedDML {
			slices.SortStableFunc(b.pendingDML, func(a, b *readResult) int { return cmp.Compare(a.dml.CommitTs, b.dml.CommitTs) })
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
	for index, result := range b.pendingDML {
		// The head DDL is a scoped barrier. Independent tables' DDLs need not
		// arrive in commit order, so only release DML belonging to this barrier.
		if result.dml.CommitTs <= dmlBoundary && (ddl == nil ||
			(result.dml.CommitTs <= ddl.event.GetCommitTs() && ddlBlocksTable(ddl.event, result.dml))) {
			if index == 0 {
				b.pendingDML[0] = nil
				b.pendingDML = b.pendingDML[1:]
			} else {
				b.pendingDML = slices.Delete(b.pendingDML, index, index+1)
			}
			return result
		}
		if !b.orderedDML && result.dml.CommitTs > dmlBoundary {
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
	}}
	b.pendingDDL[0] = nil
	b.pendingDDL = b.pendingDDL[1:]
	return result
}

func ddlBlocksTable(ddl *event.DDLEvent, dml *event.DMLEvent) bool {
	// Incomplete multi-table metadata must retain a global barrier.
	if ddl.SchemaName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeAll) {
		return true
	}
	if (ddl.Type == byte(timodel.ActionRenameTables) || ddl.Type == byte(timodel.ActionExchangeTablePartition)) && len(ddl.BlockedTableNames) == 0 && len(ddl.MultipleTableInfos) == 0 {
		return true
	}
	table := dml.TableInfo
	if table == nil {
		return true
	}
	if (table.GetSchemaName() == ddl.SchemaName && table.GetTableName() == ddl.TableName) ||
		(table.GetSchemaName() == ddl.ExtraSchemaName && table.GetTableName() == ddl.ExtraTableName) {
		return true
	}
	for _, name := range ddl.BlockedTableNames {
		if table.GetSchemaName() == name.SchemaName && table.GetTableName() == name.TableName {
			return true
		}
	}
	for _, info := range ddl.MultipleTableInfos {
		if info != nil && table.GetSchemaName() == info.GetSchemaName() && table.GetTableName() == info.GetTableName() {
			return true
		}
	}
	if ddl.BlockedTables != nil && slices.Contains(ddl.BlockedTables.TableIDs, dml.PhysicalTableID) {
		return true
	}
	return (ddl.TableName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeDB)) && table.GetSchemaName() == ddl.SchemaName
}
