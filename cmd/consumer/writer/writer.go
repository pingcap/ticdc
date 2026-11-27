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

// Package writer owns sink delivery: batching, de-duplication and
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
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
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

// Writer owns the downstream sink, batching and checkpoint bookkeeping.
//
// AddDML, AddDDL, Advance, and Flush are single-goroutine owned by conductor
// (Start only runs the sink loop), so pendingDML entries mutated in AddDML have
// not entered selectBatch/writeBatch yet.
type Writer struct {
	target     sink.Sink
	pendingDML []*event.DMLEvent
	pendingDDL []*event.DDLEvent
	mutations  map[mutationKey]bool
	ddlWrites  map[ddl.Key]bool

	watermark  uint64
	pending    uint64
	checkpoint uint64
}

type mutationKey struct {
	tableID  int64
	commitTs uint64
	rowType  common.RowType
	handle   string
}

// Progress reports the input watermark and the earliest decoded event the
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

// AddDML queues one decoded row, merging it into the table's latest pending
// DMLEvent when CommitTs is non-zero and canMergeDML holds.
func (w *Writer) AddDML(dml *event.DMLEvent) {
	if dml == nil {
		return
	}
	// The MySQL sink routes events through downstreamadapter/sink/mysql/causality,
	// which hashes DispatcherID.GetLow() into conflict keys and uses it as the only
	// key for tables without a primary or unique key. Codecs do not carry the field,
	// so derive a table-scoped value; otherwise same-value rows of different tables
	// collide onto one worker.
	if dml.DispatcherID.IsZero() && dml.PhysicalTableID != 0 {
		dml.DispatcherID = common.DispatcherID{Low: uint64(dml.PhysicalTableID)}
	}
	if dml.CommitTs != 0 {
		// Scan backwards for this table's latest pending event and stop at the
		// first match so an older CommitTs/schema never merges past a newer one.
		for i := len(w.pendingDML) - 1; i >= 0; i-- {
			prev := w.pendingDML[i]
			if prev.PhysicalTableID != dml.PhysicalTableID {
				continue
			}
			if canMergeDML(prev, dml) {
				mergeDMLEvent(prev, dml)
				return
			}
			break
		}
	}

	w.pendingDML = append(w.pendingDML, dml)
}

func canMergeDML(prev, dml *event.DMLEvent) bool {
	return prev != nil && dml != nil &&
		prev.CommitTs == dml.CommitTs &&
		prev.DispatcherID == dml.DispatcherID &&
		// mergeDMLEvent appends src.Rows from index 0 and filterRows indexes Rows
		// via PreviousTotalOffset + physicalOffset, which requires standalone chunks.
		prev.PreviousTotalOffset == 0 &&
		dml.PreviousTotalOffset == 0 &&
		sameTableSchema(prev.TableInfo, dml.TableInfo)
}

func mergeDMLEvent(dst, src *event.DMLEvent) {
	dstRowTypeCount := len(dst.RowTypes)
	srcRowTypeCount := len(src.RowTypes)
	if src.Rows != nil && src.Rows.NumRows() > 0 {
		if dst.Rows == nil {
			dst.Rows = src.Rows
		} else {
			dst.Rows.Append(src.Rows, 0, src.Rows.NumRows())
		}
	}
	dst.RowTypes = append(dst.RowTypes, src.RowTypes...)
	dst.RowKeys = appendOptionalDMLValues(dst.RowKeys, src.RowKeys, dstRowTypeCount, srcRowTypeCount)
	// Checksums track logical rows (Length), whereas updates occupy two physical rows.
	dst.Checksum = appendOptionalDMLValues(dst.Checksum, src.Checksum, int(dst.Length), int(src.Length))
	dst.Length += src.Length
	dst.ApproximateSize += src.ApproximateSize
	dst.PostTxnEnqueued = append(dst.PostTxnEnqueued, src.PostTxnEnqueued...)
	dst.PostTxnFlushed = append(dst.PostTxnFlushed, src.PostTxnFlushed...)
}

func appendOptionalDMLValues[T any](dst, src []T, dstCount, srcCount int) []T {
	if len(dst) == 0 && len(src) == 0 {
		return nil
	}
	if len(dst) == 0 && len(src) != 0 {
		dst = make([]T, dstCount)
	} else if len(dst) != 0 && len(src) == 0 {
		src = make([]T, srcCount)
	}
	return append(dst, src...)
}

// AddDDL queues a block event.
func (w *Writer) AddDDL(block *event.DDLEvent) {
	w.pendingDDL = append(w.pendingDDL, block)
}

func (w *Writer) writeDDL(ctx context.Context, block *event.DDLEvent) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}

	if err := w.target.FlushDMLBeforeBlock(block); err != nil {
		return err
	}

	return w.target.WriteBlockEvent(block)
}

func (w *Writer) flushDDL(ctx context.Context) error {
	for len(w.pendingDDL) != 0 {
		block := w.pendingDDL[0]
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

		w.pendingDDL[0] = nil
		w.pendingDDL = w.pendingDDL[1:]

		if err := w.writeDDL(ctx, block); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DDL write failed",
					zap.String("schema", block.SchemaName),
					zap.String("table", block.TableName),
					zap.Uint64("commitTs", block.GetCommitTs()),
					zap.Error(err))
			}
			return err
		}

		if w.ddlWrites == nil {
			w.ddlWrites = make(map[ddl.Key]bool)
		}
		w.ddlWrites[key] = true
	}

	return nil
}

// Flush drains all pending DML batches before executing pending DDLs, waits
// for sink acceptance, and advances the checkpoint.
func (w *Writer) Flush(ctx context.Context) error {
	for len(w.pendingDML) != 0 {
		if err := context.Cause(ctx); err != nil {
			return err
		}

		items := w.selectBatch()
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

// selectBatch takes pending events whose table schema consistency allows
// batching. Merged CommitTs groups remain whole DMLEvents even if they exceed
// batchRows or batchBytes, matching legacy storage consumer transaction
// semantics. The price is that one such group is applied as one downstream
// transaction, which the sink degrades to statement-by-statement execution once
// it exceeds about half of max_allowed_packet.
func (w *Writer) selectBatch() []*event.DMLEvent {
	blocked := make(map[int64]bool)
	tables := make(map[int64]*event.DMLEvent)
	items := make([]*event.DMLEvent, 0, min(batchRows, len(w.pendingDML)))

	var (
		rows  int
		bytes int64
	)
	for _, dml := range w.pendingDML {
		id := dml.PhysicalTableID
		if blocked[id] {
			continue
		}

		if previous := tables[id]; previous != nil && !sameTableSchema(previous.TableInfo, dml.TableInfo) {
			blocked[id] = true
			continue
		}

		tables[id] = dml
		rows += int(dml.Len())
		bytes += dml.Rows.MemoryUsage()
		items = append(items, dml)

		if rows >= batchRows || bytes >= batchBytes {
			break
		}
	}

	return items
}

func (w *Writer) removeDML(items []*event.DMLEvent) {
	remaining := w.pendingDML[:0]

	dropped := 0
	for _, dml := range w.pendingDML {
		if dropped < len(items) && dml == items[dropped] {
			dropped++
			continue
		}

		remaining = append(remaining, dml)
	}

	clear(w.pendingDML[len(remaining):])
	w.pendingDML = remaining
}

func sameTableSchema(a, b *common.TableInfo) bool {
	return a == b || (a != nil && b != nil &&
		a.GetUpdateTS() == b.GetUpdateTS() &&
		a.GetSchemaName() == b.GetSchemaName() &&
		a.GetTableName() == b.GetTableName() &&
		reflect.DeepEqual(a.GetColumns(), b.GetColumns()) &&
		reflect.DeepEqual(a.GetIndices(), b.GetIndices()))
}

// writeBatch submits the batch and waits until the sink has flushed every event.
func (w *Writer) writeBatch(ctx context.Context, items []*event.DMLEvent) error {
	var events []*event.DMLEvent
	var dropped []*event.DMLEvent
	for _, dml := range items {
		filtered := w.filterRows(dml)
		if filtered == nil {
			dropped = append(dropped, dml)
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

	return nil
}

// advanceCheckpoint publishes progress only through events the sink has written.
func (w *Writer) advanceCheckpoint() {
	through := w.watermark
	if w.pending != 0 {
		through = min(w.watermark, w.pending-1)
	}

	lower := func(ts uint64) {
		if ts != 0 {
			through = min(through, ts-1)
		}
	}
	for _, dml := range w.pendingDML {
		lower(dml.CommitTs)
	}
	for _, block := range w.pendingDDL {
		lower(block.GetCommitTs())
	}

	previous := w.checkpoint
	w.checkpoint = max(w.checkpoint, through)
	if w.checkpoint > previous && w.target != nil {
		w.target.AddCheckpointTs(w.checkpoint)
	}

	for key := range w.mutations {
		if key.commitTs > 0 && key.commitTs <= w.checkpoint {
			delete(w.mutations, key)
		}
	}
}

// filterRows drops replays covered by durable checkpoints or already written in
// the active window. Rows of a table without a handle cannot be matched by key
// once past checkpoint and pass through.
func (w *Writer) filterRows(dml *event.DMLEvent) *event.DMLEvent {
	if dml.CommitTs == 0 {
		return dml
	}
	if dml.CommitTs <= w.checkpoint {
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
				if w.mutations == nil {
					w.mutations = make(map[mutationKey]bool)
				}
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
