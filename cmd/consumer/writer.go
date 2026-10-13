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
	"math"
	"reflect"
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	batchRows         = 1024
	batchBytes        = 2 << 20
	maxInFlightEvents = 4096
	maxInFlightBytes  = 32 << 20
)

func newWriter(ctx context.Context, downstreamURI string, replicaConfig *config.ReplicaConfig, memory *memoryUsage) *writer {
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	downstream, err := sink.New(ctx, &config.ChangefeedConfig{
		ChangefeedID:           changefeedID,
		SinkURI:                downstreamURI,
		SinkConfig:             replicaConfig.Sink,
		CaseSensitive:          putil.GetOrZero(replicaConfig.CaseSensitive),
		EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		log.Panic("consumer downstream sink initialization failed", zap.Error(err))
	}
	return &writer{
		downstream: downstream,
		memory:     memory,
		completed:  make(chan struct{}, 1),
		mutations:  make(map[mutationKey]*writeBatch),
		ddlJobs:    make(chan *writeEvent, 1),
		ddlDone:    make(chan struct{}, 1),
	}
}

func (w *writer) start(ctx context.Context, input <-chan []*writeEvent) error {
	w.g, ctx = errgroup.WithContext(ctx)
	w.g.Go(func() error {
		return w.executeDDL(ctx)
	})
	w.g.Go(func() error {
		err := w.downstream.Run(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if err != nil {
			log.Error("consumer downstream sink failed", zap.Error(err))
			return err
		}
		log.Panic("consumer downstream sink stopped unexpectedly")
		return nil
	})
	// Dispatch returns before close joins tasks blocked on sink queues.
	return w.dispatch(ctx, input)
}

func (w *writer) dispatch(ctx context.Context, input <-chan []*writeEvent) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case items, ok := <-input:
			if !ok {
				return context.Cause(ctx)
			}
			for _, item := range items {
				w.consume(item)
			}
		case <-w.completed:
		}
		if err := w.flushDML(ctx); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DML submission failed", zap.Error(err))
			}
			return err
		}
	}
}

func (w *writer) close() {
	// Close also wakes sinks whose input channel is blocked while idle.
	w.downstream.Close()
	if w.g != nil {
		_ = w.g.Wait()
	}
}

func (w *writer) executeDDL(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result := <-w.ddlJobs:
			if err := w.writeDDL(ctx, result); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer DDL write failed", zap.String("schema", result.ddl.SchemaName), zap.String("table", result.ddl.TableName), zap.Uint64("commitTs", result.ddl.GetCommitTs()), zap.Error(err))
				}
				return err
			}
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case w.ddlDone <- struct{}{}:
			}
			select {
			case w.completed <- struct{}{}:
			default:
			}
		}
	}
}

type writer struct {
	g                 *errgroup.Group
	downstream        sink.Sink
	memory            *memoryUsage
	completed         chan struct{}
	pendingDML        []*writeEvent
	pendingDDL        []*writeEvent
	ddlInFlight       *writeEvent
	ddlJobs           chan *writeEvent
	ddlDone           chan struct{}
	inFlight          []*writeBatch
	mutations         map[mutationKey]*writeBatch // nil batches mark durable mutations retained at the watermark boundary.
	ddlWrites         map[ddlKey]bool
	writtenBefore     uint64
	inFlightBytes     int64
	inFlightEvents    int
	pendingWatermarks []*writeEvent
	watermark         uint64
	progressChanged   bool
}
type writeBatch struct {
	items   []*writeEvent
	events  []*event.DMLEvent
	dropped []*event.DMLEvent
	keys    []mutationKey
	bytes   int64
	done    chan struct{}
	flushed atomic.Int64
}

type mutationKey struct {
	tableID  int64
	commitTs uint64
	rowType  common.RowType
	handle   string
}

type ddlKey struct {
	commitTs uint64
	schema   string
	table    string
	query    string
}

func (w *writer) consume(result *writeEvent) {
	if result.dml != nil {
		w.pendingDML = append(w.pendingDML, result)
		w.progressChanged = true
	}
	if result.ddl != nil {
		w.pendingDDL = append(w.pendingDDL, result)
		w.progressChanged = true
	}
	if result.hasWatermark {
		if result.watermark > w.watermark {
			w.watermark = result.watermark
			w.progressChanged = true
		}
		if result.onFlush != nil {
			w.pendingWatermarks = append(w.pendingWatermarks, result)
			w.progressChanged = true
		}
	}
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
	if w.ddlInFlight != nil {
		select {
		case <-w.ddlDone:
			w.ddlInFlight = nil
			w.progressChanged = true
		default:
		}
	}
	for w.ddlInFlight == nil && len(w.pendingDDL) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		result := w.pendingDDL[0]
		ddl := result.ddl
		// Failover can resend an old DDL after a newer one. Query is part of
		// its identity because split DDLs can share the timestamp and table.
		key := ddlKey{
			commitTs: ddl.GetCommitTs(),
			schema:   ddl.SchemaName,
			table:    ddl.TableName,
			query:    ddl.Query,
		}
		if w.ddlWrites[key] || key.commitTs < w.writtenBefore {
			// No DDL is in flight, so the original write has succeeded.
			if result.onFlush != nil {
				result.onFlush()
			}
			w.memory.release(result.bytes)
			w.pendingDDL[0] = nil
			w.pendingDDL = w.pendingDDL[1:]
			w.progressChanged = true
			continue
		}
		for _, item := range w.pendingDML {
			if item.dml.CommitTs <= ddl.GetCommitTs() && ddlBlocksTable(ddl, item) {
				return nil
			}
		}
		for _, batch := range w.inFlight {
			if slices.ContainsFunc(batch.items, func(item *writeEvent) bool {
				return ddlBlocksTable(ddl, item)
			}) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case w.ddlJobs <- result:
			if w.ddlWrites == nil {
				w.ddlWrites = make(map[ddlKey]bool)
			}
			w.ddlWrites[key] = true
			w.ddlInFlight = result
			w.pendingDDL[0] = nil
			w.pendingDDL = w.pendingDDL[1:]
			return nil
		}
	}
	w.advanceWatermarks()
	return nil
}

func (w *writer) flushDML(ctx context.Context) error {
	w.finishBatches()
	// Retire replayed DDLs before they can fence later rows of the table.
	if err := w.flushDDL(ctx); err != nil {
		return err
	}
	active := make(map[int64]*writeEvent)
	blocked := make(map[int64]bool)
	tables := make(map[int64]*writeEvent)
	pending := w.pendingDML
	for len(w.pendingDML) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		batch, end := w.selectBatch(active, blocked, tables)
		if batch == nil {
			break
		}
		if err := w.prepareBatch(ctx, batch); err != nil {
			return err
		}
		if err := w.submitBatch(ctx, batch, end); err != nil {
			return err
		}
		w.finishBatches()
	}
	if len(w.pendingDML) == 0 {
		w.pendingDML = pending[:0]
	}
	return w.flushDDL(ctx)
}

func (w *writer) selectBatch(active map[int64]*writeEvent, blocked map[int64]bool, tables map[int64]*writeEvent) (*writeBatch, int) {
	clear(active)
	clear(blocked)
	clear(tables)
	for _, batch := range w.inFlight {
		for _, item := range batch.items {
			active[item.dml.PhysicalTableID] = item
		}
	}
	var (
		rows  int
		bytes int64
		end   int
	)
	items := make([]*writeEvent, 0, min(batchRows, len(w.pendingDML)))
	for index, item := range w.pendingDML {
		id := item.dml.PhysicalTableID
		if blocked[id] {
			continue
		}
		fenced := w.ddlInFlight != nil && item.dml.CommitTs > w.ddlInFlight.ddl.GetCommitTs() && ddlBlocksTable(w.ddlInFlight.ddl, item)
		if fenced || slices.ContainsFunc(w.pendingDDL, func(control *writeEvent) bool {
			return item.dml.CommitTs > control.ddl.GetCommitTs() && ddlBlocksTable(control.ddl, item)
		}) {
			blocked[id] = true
			continue
		}
		if earlier := active[id]; earlier != nil && (earlier.sequential || item.sequential || !sameTableSchema(earlier.dml.TableInfo, item.dml.TableInfo)) {
			blocked[id] = true
			continue
		}
		if previous := tables[id]; previous != nil &&
			(!sameTableSchema(previous.dml.TableInfo, item.dml.TableInfo) || ((previous.sequential || item.sequential) && previous.dml.CommitTs != item.dml.CommitTs)) {
			blocked[id] = true
			continue
		}
		tables[id] = item
		rows += int(item.dml.Len())
		bytes += item.bytes
		items = append(items, item)
		end = index + 1
		if rows >= batchRows || bytes >= batchBytes {
			break
		}
	}
	if len(items) == 0 {
		return nil, 0
	}
	return &writeBatch{
		items:  items,
		events: make([]*event.DMLEvent, 0, len(items)),
		bytes:  bytes,
		done:   make(chan struct{}),
	}, end
}

func (w *writer) prepareBatch(ctx context.Context, batch *writeBatch) error {
	for len(w.inFlight) != 0 && (w.inFlightEvents+len(batch.items) > maxInFlightEvents || w.inFlightBytes+batch.bytes > maxInFlightBytes) {
		if err := w.waitBatch(ctx, w.inFlight[0]); err != nil {
			return err
		}
	}
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
			w.memory.reserve(copyBytes)
			batch.bytes += copyBytes
		}
		batch.events = append(batch.events, filtered)
	}
	for len(w.inFlight) != 0 && w.inFlightBytes+batch.bytes > maxInFlightBytes {
		if err := w.waitBatch(ctx, w.inFlight[0]); err != nil {
			return err
		}
	}
	return nil
}

func (w *writer) submitBatch(ctx context.Context, batch *writeBatch, end int) error {
	w.inFlight = append(w.inFlight, batch)
	w.inFlightBytes += batch.bytes
	w.inFlightEvents += len(batch.items)
	w.removeDML(batch.items, end)
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
				case w.completed <- struct{}{}:
				default:
				}
			}
		})
		w.downstream.AddDMLEvent(dml)
	}
	return nil
}

func (w *writer) removeDML(items []*writeEvent, end int) {
	if end == len(items) {
		// The common case consumes a prefix without moving the backlog.
		clear(w.pendingDML[:end])
		w.pendingDML = w.pendingDML[end:]
		return
	}
	var selected int
	w.pendingDML = slices.DeleteFunc(w.pendingDML, func(item *writeEvent) bool {
		if selected == len(items) || item != items[selected] {
			return false
		}
		selected++
		return true
	})
}

func sameTableSchema(a, b *common.TableInfo) bool {
	return a == b || (a != nil && b != nil && a.GetUpdateTS() == b.GetUpdateTS() &&
		a.GetSchemaName() == b.GetSchemaName() && a.GetTableName() == b.GetTableName() &&
		reflect.DeepEqual(a.GetColumns(), b.GetColumns()) && reflect.DeepEqual(a.GetIndices(), b.GetIndices()))
}

func (w *writer) waitBatch(ctx context.Context, batch *writeBatch) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-batch.done:
		w.finishBatches()
		return nil
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
	if w.watermark == 0 && len(w.pendingWatermarks) == 0 {
		return
	}
	// Readers publish global watermarks. Every unfinished write limits their
	// downstream completion, regardless of its table.
	var (
		unfinished uint64 = math.MaxUint64
		pending    bool
	)
	for _, control := range w.pendingDDL {
		unfinished, pending = min(unfinished, control.ddl.GetCommitTs()), true
	}
	if control := w.ddlInFlight; control != nil {
		unfinished, pending = min(unfinished, control.ddl.GetCommitTs()), true
	}
	for _, item := range w.pendingDML {
		unfinished, pending = min(unfinished, item.dml.CommitTs), true
	}
	for _, batch := range w.inFlight {
		for _, item := range batch.items {
			unfinished, pending = min(unfinished, item.dml.CommitTs), true
		}
	}
	remaining := w.pendingWatermarks[:0]
	for _, control := range w.pendingWatermarks {
		if pending && unfinished <= control.watermark {
			remaining = append(remaining, control)
			continue
		}
		control.onFlush()
	}
	clear(w.pendingWatermarks[len(remaining):])
	w.pendingWatermarks = remaining
	before := min(w.watermark, unfinished)
	if before <= w.writtenBefore {
		return
	}
	w.writtenBefore = max(w.writtenBefore, before)
	// Retire identities only after input progress and all earlier writes pass.
	for key := range w.mutations {
		if key.commitTs < w.writtenBefore {
			delete(w.mutations, key)
		}
	}
	for key := range w.ddlWrites {
		if key.commitTs < w.writtenBefore {
			delete(w.ddlWrites, key)
		}
	}
}

func (w *writer) filterRows(ctx context.Context, dml *event.DMLEvent, batch *writeBatch) (*event.DMLEvent, error) {
	if dml.CommitTs < w.writtenBefore {
		return nil, nil
	}
	if dml.CommitTs == 0 || len(dml.TableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		return dml, nil
	}
	keep := make([]bool, 0, dml.Len())
	var (
		kept         int
		physicalRows int
	)
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
			key := mutationKey{
				tableID:  dml.PhysicalTableID,
				commitTs: dml.CommitTs,
				rowType:  row.RowType,
				handle:   string(handle),
			}
			switch previous, exists := w.mutations[key]; {
			case exists:
				if previous != nil && previous != batch {
					if err := w.waitBatch(ctx, previous); err != nil {
						return nil, err
					}
				}
				retain = false
			default:
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
	var physicalOffset int
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
