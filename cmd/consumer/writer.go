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
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

const (
	batchRows           = 1024
	batchBytes          = 2 << 20
	batchLinger         = 10 * time.Millisecond
	maxInFlight         = 4
	maxInFlightBytes    = 32 << 20
	maxRecordBytes      = 16 << 20
	maxBufferedBytes    = 128 << 20
	memoryHighWater     = 96 << 20
	maxRecords          = 64 << 10
	maxEffects          = 256 << 10
	maxSchemaBytes      = 32 << 20
	maxSchemas          = 4096
	shutdownTimeout     = 5 * time.Second
	progressLogInterval = 5 * time.Second
)

type messageRecord struct {
	partition int32
	remaining int
	complete  bool
	bytes     int64
}

type messagePartition struct {
	decoder       codeccommon.Decoder
	watermark     uint64
	hasWatermark  bool
	records       []*messageRecord
	cachedRecords []*messageRecord
	// Simple can release cached messages out of order. Keep their records
	// incomplete until every message in this cache group has been flushed.
	cachedUnreleased int
	cachedUnflushed  int
	paused           bool
	schemas          map[schemaKey]bool
	schemaPointers   map[*common.TableInfo]bool
}

type schemaKey struct {
	schema  string
	table   string
	version uint64
}

type pendingDML struct {
	event           *event.DMLEvent
	record          *messageRecord
	cachedPartition *messagePartition
	bytes           int64
}

type ddlKey struct {
	commitTs uint64
	schema   string
	table    string
}

type pendingDDL struct {
	key        ddlKey
	event      *event.DDLEvent
	partitions map[int32]bool
	records    []*messageRecord
	flushed    bool
	bytes      int64
}

type eventWriter struct {
	downstream      sink.Sink
	protocol        config.Protocol
	partitions      map[int32]*messagePartition
	pendingDML      []*pendingDML
	pendingDDL      []*pendingDDL
	ddls            map[ddlKey]*pendingDDL
	inFlight        []*writeBatch
	mutations       map[mutationKey]*mutation
	readySince      time.Time
	dmlDirty        bool
	ddlDirty        bool
	writtenBefore   uint64
	inputBytes      int64
	polledBytes     int64
	dmlBytes        int64
	controlBytes    int64
	mutationBytes   int64
	schemaBytes     int64
	schemaCount     int
	inFlightBytes   int64
	recordCount     int
	effectCount     int
	confirm         func(context.Context) error
	bufferedInput   func() int64
	receivedInputs  int64
	decodedRows     int64
	writtenRows     int64
	completedInputs int64
	lastProgressLog time.Time
}

type writeBatch struct {
	items   []*pendingDML
	events  []*event.DMLEvent
	dropped []*event.DMLEvent
	keys    []mutationKey
	bytes   int64
	done    chan bool
	flushed atomic.Int64
}

type mutationKey struct {
	tableID  int64
	commitTs uint64
	rowType  common.RowType
	handle   string
}

type mutation struct {
	batch  *writeBatch
	digest [sha256.Size]byte
}

func (c *eventWriter) queueDML(message *codeccommon.DMLMessage, record *messageRecord, cachedPartition *messagePartition) error {
	dml := message.ToDMLEvent()
	if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
		return errors.ErrCodecDecode.FastGenByArgs("DML message cannot be materialized into nonempty rows with table metadata")
	}
	partition := cachedPartition
	if record != nil {
		partition = c.partitions[record.partition]
	}
	if err := c.trackSchema(partition, dml.TableInfo); err != nil {
		return err
	}
	bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64 + 256
	if bytes > maxInFlightBytes || c.bufferedBytes()+bytes > maxBufferedBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer DML exceeds its buffer limit; source record remains unconfirmed")
	}
	c.dmlBytes += bytes
	c.pendingDML = append(c.pendingDML, &pendingDML{event: dml, record: record, cachedPartition: cachedPartition, bytes: bytes})
	c.decodedRows += int64(dml.Len())
	c.dmlDirty = true
	return nil
}

func (c *eventWriter) trackSchema(partition *messagePartition, table *common.TableInfo) error {
	if table == nil {
		return nil
	}
	key := schemaKey{schema: table.GetSchemaName(), table: table.GetTableName(), version: table.GetUpdateTS()}
	// Kafka/Pulsar Canal metadata has no schema version. Storage supplies the
	// version from its schema file, so per-row metadata shares one cache entry.
	unversioned := c.protocol == config.ProtocolCanalJSON && table.GetUpdateTS() == 0
	if unversioned {
		if _, known := partition.schemaPointers[table]; known {
			return nil
		}
	} else if _, known := partition.schemas[key]; known {
		return nil
	}
	if c.schemaCount >= maxSchemas {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its count limit; source record remains unconfirmed")
	}
	data, err := table.Marshal()
	if err != nil {
		return errors.WrapError(errors.ErrCodecDecode, err, "measure consumer table metadata")
	}
	// Serialized size is an accounting estimate, not a process RSS limit.
	// Reserve space for decoded columns, indexes, maps and generated SQL too.
	bytes := int64(len(data))*4 + 1024
	if c.schemaBytes+bytes > maxSchemaBytes || c.bufferedBytes()+bytes > maxBufferedBytes {
		return errors.ErrInternalCheckFailed.FastGenByArgs("consumer schema cache exceeds its byte limit; source record remains unconfirmed")
	}
	if unversioned {
		partition.schemaPointers[table] = true
	} else {
		partition.schemas[key] = true
	}
	c.schemaBytes += bytes
	c.schemaCount++
	return nil
}

func (c *eventWriter) flushReady(ctx, writeCtx context.Context, watermark uint64, force bool) error {
	if c.ddlDirty {
		slices.SortStableFunc(c.pendingDDL, func(a, b *pendingDDL) int { return cmp.Compare(a.key.commitTs, b.key.commitTs) })
		c.ddlDirty = false
	}
	for len(c.pendingDDL) != 0 {
		ddl := c.pendingDDL[0]
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
			break
		}
		expected := len(c.partitions)
		if c.protocol == config.ProtocolCanalJSON {
			expected = 1
		}
		// Every partition has crossed this inclusive watermark. A missing
		// DDL copy can no longer arrive without violating the source boundary.
		if len(ddl.partitions) != expected {
			if ddl.key.commitTs > watermark {
				break
			}
			return errors.ErrCodecDecode.FastGenByArgs("DDL is missing an expected copy at the complete boundary")
		}
		if ddl.event == nil {
			return errors.ErrCodecDecode.FastGenByArgs("DDL is missing its canonical event")
		}
		// Independent creation has no earlier DML in its target scope. It
		// may initialize the downstream while the source watermark is stalled.
		if ddl.key.commitTs <= watermark {
			if err := c.flushDML(ctx, writeCtx, ddl.key.commitTs, false, true); err != nil {
				return err
			}
		}
		// Only batches that touch this DDL's scope need to be durable first.
		// Keep unrelated batches in flight while the synchronous DDL executes.
		for _, batch := range slices.Clone(c.inFlight) {
			if batchAffectedByDDL(batch, ddl.event) {
				if err := c.waitBatch(ctx, batch); err != nil {
					return err
				}
			}
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := c.downstream.FlushDMLBeforeBlock(ddl.event); err != nil {
			return err
		}
		if err := c.downstream.WriteBlockEvent(ddl.event); err != nil {
			return err
		}
		for _, record := range ddl.records {
			if err := c.finishRecordEffect(record); err != nil {
				return err
			}
		}
		c.controlBytes -= ddl.bytes
		ddl.flushed = true
		ddl.event = nil
		ddl.partitions = nil
		ddl.records = nil
		ddl.bytes = int64(len(ddl.key.schema) + len(ddl.key.table) + 192)
		c.controlBytes += ddl.bytes
		c.pendingDDL[0] = nil
		c.pendingDDL = c.pendingDDL[1:]
	}
	return c.flushDML(ctx, writeCtx, watermark, true, force)
}

func (c *eventWriter) flushDML(ctx, writeCtx context.Context, commitTs uint64, inclusive, force bool) error {
	var building *writeBatch
	defer func() {
		// Cancellation can interrupt filtering while it waits for an earlier
		// batch. Roll back unsubmitted identities so the shutdown drain can retry.
		if building == nil {
			return
		}
		for _, key := range building.keys {
			delete(c.mutations, key)
			c.mutationBytes -= int64(len(key.handle) + 192)
		}
		originalBytes := int64(0)
		for _, item := range building.items {
			originalBytes += item.bytes
		}
		c.dmlBytes -= building.bytes - originalBytes
	}()
	if c.dmlDirty {
		slices.SortStableFunc(c.pendingDML, func(a, b *pendingDML) int { return cmp.Compare(a.event.CommitTs, b.event.CommitTs) })
		c.dmlDirty = false
	}
	for len(c.pendingDML) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		rows, bytes, end := 0, int64(0), 0
		for _, item := range c.pendingDML {
			if item.event.CommitTs > commitTs || (!inclusive && item.event.CommitTs == commitTs) {
				break
			}
			rows += int(item.event.Len())
			bytes += item.bytes
			end++
			if rows >= batchRows || bytes >= batchBytes {
				break
			}
		}
		if end == 0 {
			c.readySince = time.Time{}
			return nil
		}
		if c.readySince.IsZero() {
			c.readySince = time.Now()
		}
		if !force && rows < batchRows && bytes < batchBytes && time.Since(c.readySince) < batchLinger {
			return nil
		}
		for len(c.inFlight) >= maxInFlight || c.inFlightBytes+bytes > maxInFlightBytes {
			if err := c.waitBatch(ctx, c.inFlight[0]); err != nil {
				return err
			}
		}
		batch := &writeBatch{items: slices.Clone(c.pendingDML[:end]), bytes: bytes, done: make(chan bool)}
		building = batch
		for _, item := range batch.items {
			filtered, err := c.filterRows(ctx, item.event, batch)
			if err != nil {
				return err
			}
			if filtered == nil {
				batch.dropped = append(batch.dropped, item.event)
				continue
			}
			if filtered != item.event {
				// The original chunk stays alive through its decoder callbacks.
				copyBytes := filtered.Rows.MemoryUsage() + int64(len(filtered.RowTypes))*64 + 256
				if c.bufferedBytes()+copyBytes > maxBufferedBytes {
					return errors.ErrInternalCheckFailed.FastGenByArgs("consumer filtered DML exceeds its buffer limit; source record remains unconfirmed")
				}
				batch.bytes += copyBytes
				c.dmlBytes += copyBytes
			}
			batch.events = append(batch.events, filtered)
		}
		if batch.bytes > maxInFlightBytes {
			return errors.ErrInternalCheckFailed.FastGenByArgs("consumer batch exceeds its in-flight byte limit; source record remains unconfirmed")
		}
		for c.inFlightBytes+batch.bytes > maxInFlightBytes {
			if err := c.waitBatch(ctx, c.inFlight[0]); err != nil {
				return err
			}
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		c.inFlight = append(c.inFlight, batch)
		building = nil
		c.inFlightBytes += batch.bytes
		copy(c.pendingDML, c.pendingDML[end:])
		clear(c.pendingDML[len(c.pendingDML)-end:])
		c.pendingDML = c.pendingDML[:len(c.pendingDML)-end]
		c.readySince = time.Time{}
		if len(batch.events) == 0 {
			close(batch.done)
		}
		for _, dml := range batch.events {
			// An admitted batch is handed to the sink in full even if reading
			// stops halfway through submission. Sink failure still cancels it.
			if err := context.Cause(writeCtx); err != nil {
				return err
			}
			var flushed atomic.Bool
			dml.AddPostFlushFunc(func() {
				if flushed.CompareAndSwap(false, true) && batch.flushed.Add(1) == int64(len(batch.events)) {
					close(batch.done)
				}
			})
			c.downstream.AddDMLEvent(dml)
		}
		if err := c.finishBatches(); err != nil {
			return err
		}
	}
	c.readySince = time.Time{}
	return nil
}

func (c *eventWriter) waitBatch(ctx context.Context, batch *writeBatch) error {
	tick := time.Tick(progressLogInterval)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-batch.done:
			if err := c.finishBatches(); err != nil {
				return err
			}
			return c.confirm(ctx)
		case <-tick:
			if time.Since(c.lastProgressLog) >= progressLogInterval {
				c.lastProgressLog = time.Now()
				log.Info("consumer waiting for batch",
					zap.Int64("receivedInputs", c.receivedInputs), zap.Int64("decodedRows", c.decodedRows),
					zap.Int64("writtenRows", c.writtenRows), zap.Int64("completedInputs", c.completedInputs),
					zap.Int("pendingDMLCount", len(c.pendingDML)), zap.Int("pendingDDLCount", len(c.pendingDDL)),
					zap.Int("inFlightBatches", len(c.inFlight)), zap.Int64("inFlightBytes", c.inFlightBytes),
					zap.Int("uncompletedInputs", c.recordCount), zap.Int64("bufferedBytes", c.bufferedBytes()))
			}
		}
	}
}

func (c *eventWriter) finishBatches() error {
	remaining := c.inFlight[:0]
	for _, batch := range c.inFlight {
		select {
		case <-batch.done:
		default:
			remaining = append(remaining, batch)
			continue
		}
		for _, dml := range batch.dropped {
			dml.PostFlush()
		}
		for _, dml := range batch.events {
			c.writtenRows += int64(dml.Len())
		}
		for _, item := range batch.items {
			if item.record != nil {
				if err := c.finishRecordEffect(item.record); err != nil {
					return err
				}
				continue
			}
			partition := item.cachedPartition
			partition.cachedUnflushed--
			if partition.cachedUnreleased == 0 && partition.cachedUnflushed == 0 {
				for _, record := range partition.cachedRecords {
					if err := c.finishRecordEffect(record); err != nil {
						return err
					}
				}
				clear(partition.cachedRecords)
				partition.cachedRecords = nil
			}
		}
		for _, key := range batch.keys {
			c.mutations[key].batch = nil
		}
		c.dmlBytes -= batch.bytes
		c.inFlightBytes -= batch.bytes
	}
	clear(c.inFlight[len(remaining):])
	c.inFlight = remaining
	return nil
}

func (c *eventWriter) advanceReplay(watermark uint64) {
	if watermark <= c.writtenBefore {
		return
	}
	// This is an exclusive frontier: keep identities at the current boundary
	// for equal-ts replays, and never move past queued or in-flight effects.
	before := watermark
	for _, item := range c.pendingDML {
		before = min(before, item.event.CommitTs)
	}
	for _, batch := range c.inFlight {
		for _, item := range batch.items {
			before = min(before, item.event.CommitTs)
		}
	}
	for _, ddl := range c.pendingDDL {
		before = min(before, ddl.key.commitTs)
	}
	if before <= c.writtenBefore {
		return
	}
	c.writtenBefore = before
	for key, ddl := range c.ddls {
		if ddl.flushed && key.commitTs < before {
			c.controlBytes -= ddl.bytes
			delete(c.ddls, key)
		}
	}
	for key := range c.mutations {
		if key.commitTs < before {
			c.mutationBytes -= int64(len(key.handle) + 192)
			delete(c.mutations, key)
		}
	}
}

func (c *eventWriter) filterRows(ctx context.Context, dml *event.DMLEvent, batch *writeBatch) (*event.DMLEvent, error) {
	if dml.CommitTs < c.writtenBefore {
		return nil, nil
	}
	if dml.CommitTs == 0 || len(dml.TableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		return dml, nil
	}
	keep := make([]bool, 0, dml.Len())
	kept, physicalRows := 0, 0
	dml.Rewind()
	defer dml.Rewind()
	for row, ok := dml.GetNextRow(); ok; row, ok = dml.GetNextRow() {
		handleRow := row.Row
		if row.RowType != common.RowTypeInsert {
			handleRow = row.PreRow
		}
		var handle []byte
		for _, id := range dml.TableInfo.GetOrderedHandleKeyColumnIDs() {
			offset := dml.TableInfo.MustGetColumnOffsetByID(id)
			value := common.ExtractColVal(&handleRow, dml.TableInfo.GetColumns()[offset], offset)
			if value == nil {
				handle = nil
				break
			}
			valueString := common.ColumnValueString(value)
			handle = binary.AppendUvarint(handle, uint64(len(valueString)))
			handle = append(handle, valueString...)
		}
		retain := true
		if len(handle) != 0 {
			key := mutationKey{tableID: dml.PhysicalTableID, commitTs: dml.CommitTs, rowType: row.RowType, handle: string(handle)}
			var data []byte
			for _, values := range []chunk.Row{row.PreRow, row.Row} {
				if values.IsEmpty() {
					data = append(data, 0)
					continue
				}
				data = append(data, 1)
				for offset, column := range dml.TableInfo.GetColumns() {
					value := common.ExtractColVal(&values, column, offset)
					if value == nil {
						data = append(data, 0)
						continue
					}
					data = append(data, 1)
					valueString := common.ColumnValueString(value)
					data = binary.AppendUvarint(data, uint64(len(valueString)))
					data = append(data, valueString...)
				}
			}
			digest := sha256.Sum256(data)
			if previous := c.mutations[key]; previous != nil {
				if previous.digest != digest {
					return nil, errors.ErrCodecDecode.FastGenByArgs("conflicting rows share a table, commit-ts, mutation type and handle key")
				}
				if previous.batch != nil && previous.batch != batch {
					if err := c.waitBatch(ctx, previous.batch); err != nil {
						return nil, err
					}
				}
				retain = false
			} else {
				bytes := int64(len(key.handle) + 192)
				if len(c.mutations) >= maxEffects || c.bufferedBytes()+bytes > maxBufferedBytes {
					return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer replay state exceeds its buffer limit; source record remains unconfirmed")
				}
				c.mutations[key] = &mutation{batch: batch, digest: digest}
				c.mutationBytes += bytes
				batch.keys = append(batch.keys, key)
			}
		}
		keep = append(keep, retain)
		if retain {
			kept++
			physicalRows++
			if row.RowType == common.RowTypeUpdate {
				physicalRows++
			}
		}
	}
	if kept == len(keep) {
		return dml, nil
	}
	if kept == 0 {
		return nil, nil
	}
	filtered := event.NewDMLEvent(dml.DispatcherID, dml.PhysicalTableID, dml.StartTs, dml.CommitTs, dml.TableInfo)
	filtered.Version, filtered.Seq, filtered.Epoch = dml.Version, dml.Seq, dml.Epoch
	filtered.ReplicatingTs, filtered.TableInfoVersion, filtered.ApproximateSize = dml.ReplicatingTs, dml.TableInfoVersion, dml.ApproximateSize
	filtered.Rows = chunk.NewChunkWithCapacity(dml.TableInfo.GetFieldSlice(), physicalRows)
	filtered.AddPostFlushFunc(dml.PostFlush)
	filtered.AddPostEnqueueFunc(dml.PostEnqueue)
	physicalOffset := 0
	for index, retain := range keep {
		width := 1
		if dml.RowTypes[physicalOffset] == common.RowTypeUpdate {
			width = 2
		}
		if retain {
			filtered.Rows.Append(dml.Rows, dml.PreviousTotalOffset+physicalOffset, dml.PreviousTotalOffset+physicalOffset+width)
			filtered.RowTypes = append(filtered.RowTypes, dml.RowTypes[physicalOffset:physicalOffset+width]...)
			if len(dml.RowKeys) != 0 {
				filtered.RowKeys = append(filtered.RowKeys, dml.RowKeys[physicalOffset:physicalOffset+width]...)
			}
			if len(dml.Checksum) != 0 {
				filtered.Checksum = append(filtered.Checksum, dml.Checksum[index])
			}
			filtered.Length++
		}
		physicalOffset += width
	}
	return filtered, nil
}

func batchAffectedByDDL(batch *writeBatch, ddl *event.DDLEvent) bool {
	// Missing scope or multi-table scope cannot safely be narrowed.
	if ddl.SchemaName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeAll) {
		return true
	}
	if (ddl.Type == byte(timodel.ActionRenameTables) || ddl.Type == byte(timodel.ActionExchangeTablePartition)) && len(ddl.BlockedTableNames) == 0 && len(ddl.MultipleTableInfos) == 0 {
		return true
	}
	for _, item := range batch.items {
		table := item.event.TableInfo
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
		if ddl.BlockedTables != nil && slices.Contains(ddl.BlockedTables.TableIDs, item.event.PhysicalTableID) {
			return true
		}
		if (ddl.TableName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeDB)) && table.GetSchemaName() == ddl.SchemaName {
			return true
		}
	}
	return false
}

func (c *eventWriter) finishRecordEffect(record *messageRecord) error {
	if record.remaining <= 0 {
		return errors.ErrInternalCheckFailed.FastGenByArgs("source record effect completed more than once")
	}
	record.remaining--
	c.effectCount--
	if record.remaining == 0 {
		record.complete = true
	}
	return nil
}

func (c *eventWriter) bufferedBytes() int64 {
	input := int64(0)
	if c.bufferedInput != nil {
		input = c.bufferedInput()
	}
	return c.inputBytes + c.polledBytes + c.dmlBytes + c.controlBytes + c.mutationBytes + c.schemaBytes + input
}

func (c *eventWriter) queueDDL(ddl *event.DDLEvent, state *messageRecord, partitionID int32, canonical bool) error {
	if ddl.GetCommitTs() < c.writtenBefore {
		return nil
	}
	key := ddlKey{commitTs: ddl.GetCommitTs(), schema: ddl.GetSchemaName(), table: ddl.GetTableName()}
	pending := c.ddls[key]
	if pending == nil {
		pending = &pendingDDL{key: key, partitions: make(map[int32]bool), bytes: int64(len(ddl.Query) + len(key.schema) + len(key.table) + 1024)}
		c.controlBytes += pending.bytes
		c.ddls[key] = pending
		c.pendingDDL = append(c.pendingDDL, pending)
		c.ddlDirty = true
	}
	if pending.flushed {
		return nil
	}
	state.remaining++
	c.effectCount++
	if !pending.partitions[partitionID] {
		pending.partitions[partitionID] = true
		pending.bytes += 32
		c.controlBytes += 32
	}
	pending.records = append(pending.records, state)
	pending.bytes += 16
	c.controlBytes += 16
	if canonical {
		pending.event = ddl
	}
	return nil
}
