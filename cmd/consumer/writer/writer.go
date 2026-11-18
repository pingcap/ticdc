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

// Package writer owns sink delivery: batching, merging, de-duplication and
// checkpoints.
package writer

import (
	"context"
	"encoding/binary"
	"reflect"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	batchRows  = 1024
	batchBytes = 2 << 20
)

// Options configures a writer.
type Options struct {
	DownstreamURI string
	ReplicaConfig *config.ReplicaConfig
	CodecConfig   *codecCommon.Config
}

// New creates a writer targeting the configured downstream sink.
func New(ctx context.Context, opts Options) *Writer {
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	target, err := sink.New(ctx, &config.ChangefeedConfig{
		ChangefeedID:           changefeedID,
		SinkURI:                opts.DownstreamURI,
		SinkConfig:             opts.ReplicaConfig.Sink,
		CaseSensitive:          util.GetOrZero(opts.ReplicaConfig.CaseSensitive),
		EnableTableAcrossNodes: util.GetOrZero(opts.ReplicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		log.Panic("consumer downstream sink initialization failed", zap.Error(err))
	}
	return &Writer{
		target:    target,
		merger:    newRowMerger(opts.CodecConfig),
		mutations: make(map[mutationKey]bool),
		written:   make(map[int64]uint64),
	}
}

// Start runs the downstream sink until ctx is cancelled.
func (w *Writer) Start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := w.target.Run(ctx)
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
	<-ctx.Done()
	// Close also wakes sinks whose input channel is blocked while idle.
	w.target.Close()
	return g.Wait()
}

// writer owns the downstream sink, batching and checkpoint bookkeeping.
type Writer struct {
	target     sink.Sink
	merger     rowMerger
	pendingDML []*dmlWrite
	pendingDDL []*ddlWrite
	mutations  map[mutationKey]bool
	ddlWrites  map[ddl.Key]bool
	// written is the newest commit timestamp already written per table. Rows
	// below it are replays of a table whose rows are written in commit order.
	written map[int64]uint64

	watermark       uint64
	pending         uint64
	checkpoint      uint64
	pendingPrepared bool
}

type mutationKey struct {
	tableID  int64
	commitTs uint64
	rowType  common.RowType
	handle   string
}

type dmlWrite struct {
	dml        *event.DMLEvent
	bytes      int64
	sequential bool
}

type ddlWrite struct {
	ddl *event.DDLEvent
}

// Progress reports the input watermark and the earliest decoded work the
// conductor still holds. A zero Pending means the conductor has nothing left.
type Progress struct {
	Watermark uint64
	Pending   uint64
}

// Advance records conductor progress used to compute safe checkpoints.
func (w *Writer) Advance(progress Progress) {
	w.watermark = max(w.watermark, progress.Watermark)
	w.pending = progress.Pending
}

// Checkpoint returns the last durable checkpoint.
func (w *Writer) Checkpoint() uint64 {
	return w.checkpoint
}

// HasPendingDML reports whether decoded rows are still waiting for the sink.
func (w *Writer) HasPendingDML() bool {
	return len(w.pendingDML) != 0
}

// AddDML queues one decoded row.
func (w *Writer) AddDML(dml *event.DMLEvent) {
	w.pendingDML = append(w.pendingDML, &dmlWrite{
		dml:        dml,
		bytes:      dml.Rows.MemoryUsage(),
		sequential: w.merger.sequential(),
	})
	w.pendingPrepared = false
}

// AddDDL queues a block event.
func (w *Writer) AddDDL(block *event.DDLEvent) {
	w.pendingDDL = append(w.pendingDDL, &ddlWrite{ddl: block})
}

func (w *Writer) writeDDL(ctx context.Context, result *ddlWrite) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := w.target.FlushDMLBeforeBlock(result.ddl); err != nil {
		return err
	}
	return w.target.WriteBlockEvent(result.ddl)
}

func (w *Writer) flushDDL(ctx context.Context) error {
	for len(w.pendingDDL) != 0 {
		result := w.pendingDDL[0]
		block := result.ddl
		// Failover can resend an old DDL after a newer one. Query is part of
		// its identity because split DDLs can share the timestamp and table.
		// The global checkpoint covers rows, not DDL visibility: a schema file
		// or Kafka DDL can become visible after progress has passed it, so only
		// the in-process write record can prove it was applied.
		key := ddl.KeyOf(block)
		if w.ddlWrites[key] {
			w.pendingDDL[0] = nil
			w.pendingDDL = w.pendingDDL[1:]
			continue
		}
		if w.pendingDMLBlocks(block) {
			return nil
		}
		w.pendingDDL[0] = nil
		w.pendingDDL = w.pendingDDL[1:]
		if w.ddlWrites == nil {
			w.ddlWrites = make(map[ddl.Key]bool)
		}
		w.ddlWrites[key] = true
		if err := w.writeDDL(ctx, result); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DDL write failed", zap.String("schema", block.SchemaName), zap.String("table", block.TableName), zap.Uint64("commitTs", block.GetCommitTs()), zap.Error(err))
			}
			return err
		}
	}
	return nil
}

func (w *Writer) pendingDMLBlocks(block *event.DDLEvent) bool {
	for _, item := range w.pendingDML {
		if item.dml.CommitTs <= block.GetCommitTs() && ddl.BlocksTable(block, item.dml) {
			return true
		}
	}
	return false
}

func (w *Writer) mergePendingDML() {
	if len(w.pendingDML) == 0 || w.pendingPrepared {
		return
	}
	w.pendingDML = w.merger.merge(w.pendingDML)
	w.pendingPrepared = true
}

// Flush writes every row and DDL the ordering rules release, waits for the sink
// to accept them, and advances the checkpoint.
func (w *Writer) Flush(ctx context.Context) error {
	w.mergePendingDML()
	if err := w.flushDDL(ctx); err != nil {
		return err
	}
	for len(w.pendingDML) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		items := w.selectBatch()
		if len(items) == 0 {
			break
		}
		if err := w.writeBatch(ctx, items); err != nil {
			return err
		}
		w.removeDML(items)
	}
	if err := w.flushDDL(ctx); err != nil {
		return err
	}
	w.advanceCheckpoint()
	return nil
}

// selectBatch takes the rows whose table order and DDL fences allow writing now.
// A blocked table keeps its later rows back so batch order stays table order.
func (w *Writer) selectBatch() []*dmlWrite {
	blocked := make(map[int64]bool)
	tables := make(map[int64]*dmlWrite)
	items := make([]*dmlWrite, 0, min(batchRows, len(w.pendingDML)))
	var (
		rows  int
		bytes int64
	)
	for _, item := range w.pendingDML {
		id := item.dml.PhysicalTableID
		if blocked[id] {
			continue
		}
		if w.blockedByDDL(item.dml) {
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
		if rows >= batchRows || bytes >= batchBytes {
			break
		}
	}
	return items
}

// blockedByDDL reports whether a pending DDL must be written before the row.
func (w *Writer) blockedByDDL(dml *event.DMLEvent) bool {
	for _, control := range w.pendingDDL {
		if dml.CommitTs > control.ddl.GetCommitTs() && ddl.BlocksTable(control.ddl, dml) {
			return true
		}
	}
	return false
}

func (w *Writer) removeDML(items []*dmlWrite) {
	selected := make(map[*dmlWrite]bool, len(items))
	for _, item := range items {
		selected[item] = true
	}
	remaining := w.pendingDML[:0]
	for _, item := range w.pendingDML {
		if selected[item] {
			continue
		}
		remaining = append(remaining, item)
	}
	clear(w.pendingDML[len(remaining):])
	w.pendingDML = remaining
	w.pendingPrepared = false
}

func sameTableSchema(a, b *common.TableInfo) bool {
	return a == b || (a != nil && b != nil && a.GetUpdateTS() == b.GetUpdateTS() &&
		a.GetSchemaName() == b.GetSchemaName() && a.GetTableName() == b.GetTableName() &&
		reflect.DeepEqual(a.GetColumns(), b.GetColumns()) && reflect.DeepEqual(a.GetIndices(), b.GetIndices()))
}

// writeBatch submits the batch and waits until the sink has flushed every event.
func (w *Writer) writeBatch(ctx context.Context, items []*dmlWrite) error {
	var events []*event.DMLEvent
	var dropped []*event.DMLEvent
	for _, item := range items {
		filtered := w.filterRows(item.dml)
		if filtered == nil {
			dropped = append(dropped, item.dml)
			continue
		}
		events = append(events, filtered)
	}
	defer func() {
		for _, dml := range dropped {
			dml.PostFlush()
		}
	}()
	if len(events) == 0 {
		return nil
	}
	flushed := make(chan struct{})
	remaining := &atomic.Int64{}
	remaining.Store(int64(len(events)))
	for _, dml := range events {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		dml.AddPostFlushFunc(func() {
			if remaining.Add(-1) == 0 {
				close(flushed)
			}
		})
		w.target.AddDMLEvent(dml)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-flushed:
	}
	for _, dml := range events {
		w.written[dml.PhysicalTableID] = max(w.written[dml.PhysicalTableID], dml.CommitTs)
	}
	return nil
}

// advanceCheckpoint publishes progress only through work the sink has written.
func (w *Writer) advanceCheckpoint() {
	through := w.watermark
	if w.pending != 0 {
		through = min(w.watermark, w.pending-1)
	}
	for _, item := range w.pendingDML {
		if item.dml.CommitTs == 0 {
			continue
		}
		through = min(through, item.dml.CommitTs-1)
	}
	for _, item := range w.pendingDDL {
		if ts := item.ddl.GetCommitTs(); ts != 0 {
			through = min(through, ts-1)
		}
	}
	w.checkpoint = max(w.checkpoint, through)
	for key := range w.mutations {
		if key.commitTs > 0 && key.commitTs <= w.checkpoint {
			delete(w.mutations, key)
		}
	}
	for key := range w.ddlWrites {
		if key.CommitTs > 0 && key.CommitTs <= w.checkpoint {
			delete(w.ddlWrites, key)
		}
	}
}

// filterRows drops replays whose mutation has already been written. Rows of a
// table without a handle cannot be matched and pass through.
func (w *Writer) filterRows(dml *event.DMLEvent) *event.DMLEvent {
	if dml.CommitTs == 0 {
		return dml
	}
	if dml.CommitTs < w.written[dml.PhysicalTableID] {
		return nil
	}
	if len(dml.TableInfo.GetOrderedHandleKeyColumnIDs()) == 0 {
		return dml
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
			if _, exists := w.mutations[key]; exists {
				retain = false
			} else {
				w.mutations[key] = true
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
		return dml
	}
	if kept == 0 {
		return nil
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
	return filtered
}
