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
	"encoding/binary"
	"reflect"
	"slices"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
)

const (
	batchRows           = 1024
	batchBytes          = 2 << 20
	maxInFlightEvents   = 4096
	maxInFlightBytes    = 32 << 20
	maxRecordBytes      = 16 << 20
	maxRecords          = 64 << 10
	progressLogInterval = 5 * time.Second
)

type writer struct {
	downstream        sink.Sink
	memory            *memoryUsage
	pendingDML        []*writeEvent
	pendingDDL        []*writeEvent
	ddlInFlight       *writeEvent
	ddlJobs           chan *writeEvent
	ddlDone           chan struct{}
	inFlight          []*writeBatch
	mutations         map[mutationKey]*writeBatch // nil batches mark durable mutations retained at the watermark boundary.
	writtenBefore     uint64
	inFlightBytes     int64
	inFlightEvents    int
	decodedRows       int64
	writtenRows       int64
	pendingWatermarks []*writeEvent
	watermarks        map[int64]uint64
	progressChanged   bool
	progressTick      <-chan time.Time
}
type writeBatch struct {
	items   []*writeEvent
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

func (w *writer) consume(ctx context.Context, result *writeEvent) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if result.dml != nil {
		w.pendingDML = append(w.pendingDML, result)
		w.decodedRows += int64(result.dml.Len())
		w.progressChanged = true
	}
	if result.ddl != nil {
		w.pendingDDL = append(w.pendingDDL, result)
		w.progressChanged = true
	}
	if result.hasWatermark {
		if w.watermarks == nil {
			w.watermarks = make(map[int64]uint64)
		}
		if result.watermark > w.watermarks[result.tableID] {
			w.watermarks[result.tableID] = result.watermark
			w.progressChanged = true
		}
		if result.onFlush != nil {
			w.pendingWatermarks = append(w.pendingWatermarks, result)
			w.progressChanged = true
		}
	}
	return nil
}

func (w *writer) writeDDL(ctx context.Context, result *writeEvent) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := w.downstream.FlushDMLBeforeBlock(result.ddl); err != nil {
		return err
	}
	if err := w.downstream.WriteBlockEvent(result.ddl); err != nil {
		return err
	}
	if result.onFlush != nil {
		result.onFlush()
	}
	w.memory.release(result.bytes)
	return nil
}

func (w *writer) flushDDL(ctx context.Context) error {
	if w.ddlInFlight != nil || len(w.pendingDDL) == 0 {
		return nil
	}
	result := w.pendingDDL[0]
	for _, item := range w.pendingDML {
		if item.dml.CommitTs <= result.ddl.GetCommitTs() && ddlBlocksTable(result.ddl, item.dml) {
			return nil
		}
	}
	for _, batch := range w.inFlight {
		if slices.ContainsFunc(batch.items, func(item *writeEvent) bool { return ddlBlocksTable(result.ddl, item.dml) }) {
			return nil
		}
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case w.ddlJobs <- result:
		w.ddlInFlight = result
		w.pendingDDL[0] = nil
		w.pendingDDL = w.pendingDDL[1:]
		return nil
	}
}

func (w *writer) flushDML(ctx context.Context) error {
	w.finishBatches()
	for len(w.pendingDML) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		active := make(map[int64]*writeEvent)
		for _, batch := range w.inFlight {
			for _, item := range batch.items {
				active[item.dml.PhysicalTableID] = item
			}
		}
		blocked := make(map[int64]bool)
		rows, bytes := 0, int64(0)
		items := make([]*writeEvent, 0, len(w.pendingDML))
		tables := make(map[int64]*writeEvent)
		for _, item := range w.pendingDML {
			id := item.dml.PhysicalTableID
			if blocked[id] {
				continue
			}
			fenced := w.ddlInFlight != nil && item.dml.CommitTs > w.ddlInFlight.ddl.GetCommitTs() && ddlBlocksTable(w.ddlInFlight.ddl, item.dml)
			if fenced || slices.ContainsFunc(w.pendingDDL, func(control *writeEvent) bool {
				return item.dml.CommitTs > control.ddl.GetCommitTs() && ddlBlocksTable(control.ddl, item.dml)
			}) {
				blocked[id] = true
				continue
			}
			if earlier := active[id]; earlier != nil && (earlier.sequential || item.sequential || !sameTableSchema(earlier.dml.TableInfo, item.dml.TableInfo)) {
				blocked[id] = true
				continue
			}
			if previous := tables[item.dml.PhysicalTableID]; previous != nil &&
				(!sameTableSchema(previous.dml.TableInfo, item.dml.TableInfo) || ((previous.sequential || item.sequential) && previous.dml.CommitTs != item.dml.CommitTs)) {
				blocked[id] = true
				continue
			}
			tables[item.dml.PhysicalTableID] = item
			rows += int(item.dml.Len())
			bytes += item.bytes
			items = append(items, item)
			if rows >= batchRows || bytes >= batchBytes {
				break
			}
		}
		if len(items) == 0 {
			break
		}
		for len(w.inFlight) != 0 && (w.inFlightEvents+len(items) > maxInFlightEvents || w.inFlightBytes+bytes > maxInFlightBytes) {
			if err := w.waitBatch(ctx, w.inFlight[0]); err != nil {
				return err
			}
		}
		batch := &writeBatch{items: items, bytes: bytes, done: make(chan bool)}
		for _, item := range batch.items {
			filtered, err := w.filterRows(ctx, item.dml, batch)
			if err != nil {
				return err
			}
			if filtered == nil {
				batch.dropped = append(batch.dropped, item.dml)
				continue
			}
			if filtered != item.dml {
				// The original chunk stays alive through its decoder callbacks.
				copyBytes := filtered.Rows.MemoryUsage() + int64(len(filtered.RowTypes))*64 + 256
				if err := w.memory.reserve(ctx, copyBytes); err != nil {
					return err
				}
				batch.bytes += copyBytes
			}
			batch.events = append(batch.events, filtered)
		}
		for len(w.inFlight) != 0 && w.inFlightBytes+batch.bytes > maxInFlightBytes {
			if err := w.waitBatch(ctx, w.inFlight[0]); err != nil {
				return err
			}
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		w.inFlight = append(w.inFlight, batch)
		w.inFlightBytes += batch.bytes
		w.inFlightEvents += len(batch.items)
		remaining, selected := w.pendingDML[:0], 0
		for _, item := range w.pendingDML {
			if selected < len(items) && item == items[selected] {
				selected++
				continue
			}
			remaining = append(remaining, item)
		}
		clear(w.pendingDML[len(remaining):])
		w.pendingDML = remaining
		if len(batch.events) == 0 {
			close(batch.done)
		}
		for _, dml := range batch.events {
			if err := context.Cause(ctx); err != nil {
				return err
			}
			var flushed atomic.Bool
			dml.AddPostFlushFunc(func() {
				if flushed.CAS(false, true) && batch.flushed.Add(1) == int64(len(batch.events)) {
					w.memory.release(batch.bytes)
					close(batch.done)
					select {
					case w.memory.completed <- struct{}{}:
					default:
					}
				}
			})
			w.downstream.AddDMLEvent(dml)
		}
		w.finishBatches()
	}
	return w.flushDDL(ctx)
}

func sameTableSchema(a, b *common.TableInfo) bool {
	return a == b || (a != nil && b != nil && a.GetUpdateTS() == b.GetUpdateTS() &&
		a.GetSchemaName() == b.GetSchemaName() && a.GetTableName() == b.GetTableName() &&
		reflect.DeepEqual(a.GetColumns(), b.GetColumns()) && reflect.DeepEqual(a.GetIndices(), b.GetIndices()))
}

func (w *writer) waitBatch(ctx context.Context, batch *writeBatch) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-batch.done:
			w.finishBatches()
			return nil
		case <-w.progressTick:
			log.Info("consumer waiting for batch",
				zap.Int64("receivedInputs", w.memory.received.Load()), zap.Int64("decodedRows", w.decodedRows),
				zap.Int64("writtenRows", w.writtenRows), zap.Int64("completedInputs", w.memory.confirmed.Load()),
				zap.Int("pendingDMLCount", len(w.pendingDML)),
				zap.Int("inFlightBatches", len(w.inFlight)), zap.Int64("inFlightBytes", w.inFlightBytes),
				zap.Int64("uncompletedInputs", w.memory.received.Load()-w.memory.confirmed.Load()), zap.Int64("bufferedBytes", w.memory.used()))
		}
	}
}

func (w *writer) finishBatches() {
	remaining := w.inFlight[:0]
	for _, batch := range w.inFlight {
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
			w.writtenRows += int64(dml.Len())
		}
		for _, key := range batch.keys {
			w.mutations[key] = nil
		}
		// Nonempty batches release memory in their final flush callback.
		if len(batch.events) == 0 {
			w.memory.release(batch.bytes)
		}
		w.inFlightBytes -= batch.bytes
		w.inFlightEvents -= len(batch.items)
		w.progressChanged = true
	}
	clear(w.inFlight[len(remaining):])
	w.inFlight = remaining
	w.advanceWatermarks()
}

func (w *writer) advanceWatermarks() {
	if !w.progressChanged {
		return
	}
	w.progressChanged = false
	if len(w.watermarks) == 0 && len(w.pendingWatermarks) == 0 {
		return
	}
	// One scan establishes the unfinished floor for every table and the topic.
	unfinished := make(map[int64]uint64)
	for _, control := range w.pendingDDL {
		if ts, ok := unfinished[0]; !ok || control.ddl.GetCommitTs() < ts {
			unfinished[0] = control.ddl.GetCommitTs()
		}
	}
	if control := w.ddlInFlight; control != nil {
		if ts, ok := unfinished[0]; !ok || control.ddl.GetCommitTs() < ts {
			unfinished[0] = control.ddl.GetCommitTs()
		}
	}
	for _, item := range w.pendingDML {
		for _, tableID := range []int64{0, item.dml.PhysicalTableID} {
			if ts, ok := unfinished[tableID]; !ok || item.dml.CommitTs < ts {
				unfinished[tableID] = item.dml.CommitTs
			}
		}
	}
	for _, batch := range w.inFlight {
		for _, item := range batch.items {
			for _, tableID := range []int64{0, item.dml.PhysicalTableID} {
				if ts, ok := unfinished[tableID]; !ok || item.dml.CommitTs < ts {
					unfinished[tableID] = item.dml.CommitTs
				}
			}
		}
	}
	remaining := w.pendingWatermarks[:0]
	for _, control := range w.pendingWatermarks {
		if ts, ok := unfinished[control.tableID]; ok && ts <= control.watermark {
			remaining = append(remaining, control)
			continue
		}
		control.onFlush()
	}
	clear(w.pendingWatermarks[len(remaining):])
	w.pendingWatermarks = remaining
	before, global := w.watermarks[0]
	if ts, ok := unfinished[0]; ok {
		before = min(before, ts)
	}
	if before <= w.writtenBefore && global && len(w.watermarks) == 1 {
		return
	}
	w.writtenBefore = max(w.writtenBefore, before)
	// Retire identities only after input progress and all earlier writes pass.
	released := int64(0)
	for key := range w.mutations {
		before := w.watermarks[key.tableID]
		if ts, ok := unfinished[key.tableID]; ok {
			before = min(before, ts)
		}
		if key.commitTs < max(w.writtenBefore, before) {
			released += int64(len(key.handle) + 192)
			delete(w.mutations, key)
		}
	}
	w.memory.release(released)
}

func (w *writer) filterRows(ctx context.Context, dml *event.DMLEvent, batch *writeBatch) (*event.DMLEvent, error) {
	if dml.CommitTs < w.writtenBefore {
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
			if previous, exists := w.mutations[key]; exists {
				if previous != nil && previous != batch {
					if err := w.waitBatch(ctx, previous); err != nil {
						return nil, err
					}
				}
				retain = false
			} else {
				bytes := int64(len(key.handle) + 192)
				if err := w.memory.reserve(ctx, bytes); err != nil {
					return nil, err
				}
				w.mutations[key] = batch
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
