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
	"encoding/binary"
	"slices"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
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

type pendingDML struct {
	event *event.DMLEvent
	bytes int64
}

type writer struct {
	downstream      sink.Sink
	memory          *bufferUsage
	pendingDML      []*pendingDML
	inFlight        []*writeBatch
	mutations       map[mutationKey]*writeBatch // nil batches mark durable mutations retained at the watermark boundary.
	readySince      time.Time
	dmlDirty        bool
	writtenBefore   uint64
	dmlBytes        int64
	mutationBytes   int64
	inFlightBytes   int64
	confirm         func(context.Context) error
	decodedRows     int64
	writtenRows     int64
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

func (c *writer) writeDDL(ctx, writeCtx context.Context, result *readResult) error {
	if err := c.flushDML(ctx, writeCtx, ^uint64(0), true, true); err != nil {
		return err
	}
	for _, batch := range slices.Clone(c.inFlight) {
		if batchAffectedByDDL(batch, result.ddl) {
			if err := c.waitBatch(ctx, batch); err != nil {
				return err
			}
		}
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := c.downstream.FlushDMLBeforeBlock(result.ddl); err != nil {
		return err
	}
	if err := c.downstream.WriteBlockEvent(result.ddl); err != nil {
		return err
	}
	if result.onFlush != nil {
		result.onFlush()
	}
	c.memory.bytes.Add(-result.bytes)
	return nil
}

func (c *writer) flushDML(ctx, writeCtx context.Context, commitTs uint64, inclusive, force bool) error {
	var building *writeBatch
	defer func() {
		// Cancellation can interrupt filtering while it waits for an earlier
		// batch. Roll back unsubmitted identities so the shutdown drain can retry.
		if building == nil {
			return
		}
		for _, key := range building.keys {
			delete(c.mutations, key)
			bytes := int64(len(key.handle) + 192)
			c.mutationBytes -= bytes
			c.memory.bytes.Add(-bytes)
		}
		originalBytes := int64(0)
		for _, item := range building.items {
			originalBytes += item.bytes
		}
		c.dmlBytes -= building.bytes - originalBytes
		c.memory.bytes.Add(-(building.bytes - originalBytes))
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
				if err := c.memory.reserve(copyBytes); err != nil {
					return err
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
		c.finishBatches()
	}
	c.readySince = time.Time{}
	return nil
}

func (c *writer) waitBatch(ctx context.Context, batch *writeBatch) error {
	tick := time.Tick(progressLogInterval)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-batch.done:
			c.finishBatches()
			return c.confirm(ctx)
		case <-tick:
			if time.Since(c.lastProgressLog) >= progressLogInterval {
				c.lastProgressLog = time.Now()
				log.Info("consumer waiting for batch",
					zap.Int64("receivedInputs", c.memory.received.Load()), zap.Int64("decodedRows", c.decodedRows),
					zap.Int64("writtenRows", c.writtenRows), zap.Int64("completedInputs", c.memory.confirmed.Load()),
					zap.Int("pendingDMLCount", len(c.pendingDML)),
					zap.Int("inFlightBatches", len(c.inFlight)), zap.Int64("inFlightBytes", c.inFlightBytes),
					zap.Int64("uncompletedInputs", c.memory.records.Load()), zap.Int64("bufferedBytes", c.bufferedBytes()))
			}
		}
	}
}

func (c *writer) finishBatches() {
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
		for _, key := range batch.keys {
			c.mutations[key] = nil
		}
		c.dmlBytes -= batch.bytes
		c.memory.bytes.Add(-batch.bytes)
		c.inFlightBytes -= batch.bytes
	}
	clear(c.inFlight[len(remaining):])
	c.inFlight = remaining
}

func (c *writer) advanceReplay(watermark uint64, tableID int64) {
	before := watermark
	for _, item := range c.pendingDML {
		if tableID == 0 || item.event.PhysicalTableID == tableID {
			before = min(before, item.event.CommitTs)
		}
	}
	for _, batch := range c.inFlight {
		for _, item := range batch.items {
			if tableID == 0 || item.event.PhysicalTableID == tableID {
				before = min(before, item.event.CommitTs)
			}
		}
	}
	if tableID == 0 {
		if before <= c.writtenBefore {
			return
		}
		c.writtenBefore = before
	}
	// Storage's per-table progress retires identities, not incoming rows:
	// unread cross-node file groups can still contain older commit timestamps.
	for key := range c.mutations {
		if (tableID == 0 || key.tableID == tableID) && key.commitTs < before {
			bytes := int64(len(key.handle) + 192)
			c.mutationBytes -= bytes
			c.memory.bytes.Add(-bytes)
			delete(c.mutations, key)
		}
	}
}

func (c *writer) filterRows(ctx context.Context, dml *event.DMLEvent, batch *writeBatch) (*event.DMLEvent, error) {
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
			if previous, exists := c.mutations[key]; exists {
				if previous != nil && previous != batch {
					if err := c.waitBatch(ctx, previous); err != nil {
						return nil, err
					}
				}
				retain = false
			} else {
				bytes := int64(len(key.handle) + 192)
				if len(c.mutations) >= maxEffects {
					return nil, errors.ErrInternalCheckFailed.FastGenByArgs("consumer replay state exceeds its buffer limit; source record remains unconfirmed")
				}
				if err := c.memory.reserve(bytes); err != nil {
					return nil, err
				}
				c.mutations[key] = batch
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

func (c *writer) bufferedBytes() int64 {
	bytes := c.memory.bytes.Load()
	if c.memory.externalBytes != nil {
		bytes += c.memory.externalBytes()
	}
	return bytes
}
