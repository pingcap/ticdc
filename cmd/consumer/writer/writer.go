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

// Package writer owns sink delivery: admission, batching, merging,
// de-duplication, checkpoints, and conductor memory release.
package writer

import (
	"context"
	"encoding/binary"
	"maps"
	"math"
	"reflect"
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
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

// Options configures a Writer.
type Options struct {
	DownstreamURI string
	ReplicaConfig *config.ReplicaConfig
	CodecConfig   *codecCommon.Config
	Budget        *budget.Budget
}

// New creates a Writer targeting the configured downstream sink.
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
		memory:    opts.Budget,
		merger:    newRowMerger(opts.CodecConfig, opts.Budget),
		completed: make(chan struct{}, 1),
		mutations: make(map[mutationKey]*writeBatch),
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

// Writer owns the downstream sink, admission, batching and checkpoint
// bookkeeping.
type Writer struct {
	target            sink.Sink
	memory            *budget.Budget
	merger            rowMerger
	completed         chan struct{}
	pendingDML        []*dmlWrite
	pendingDDL        []*ddlWrite
	inFlight          []*writeBatch
	mutations         map[mutationKey]*writeBatch // nil batches mark durable mutations awaiting checkpoint advancement.
	ddlWrites         map[ddl.Key]bool
	checkpoint        uint64
	tableCheckpoints  map[int64]uint64 // Inclusive DML checkpoints ahead of the global checkpoint.
	unsubmitted       uint64
	hasUnsubmitted    bool
	unsubmittedTables map[int64]uint64
	inFlightBytes     int64
	inFlightEvents    int
	watermark         uint64
	progressChanged   bool
	pendingPrepared   bool
}

type writeBatch struct {
	items    []*dmlWrite
	events   []*event.DMLEvent
	dropped  []*event.DMLEvent
	keys     []mutationKey
	bytes    int64
	done     chan struct{}
	flushed  atomic.Int64
	finished []atomic.Bool
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
	memory     *budget.Budget
}

func (r *dmlWrite) release() {
	r.memory.Release(r.bytes)
}

type ddlWrite struct {
	ddl     *event.DDLEvent
	release func()
}

// AddDML queues one decoded row when the in-flight limits and the per-table
// ordering rules allow it. The caller keeps ownership when it returns false.
func (w *Writer) AddDML(dml *event.DMLEvent) bool {
	bytes := dml.Rows.MemoryUsage()
	if w.inFlightEvents != 0 && (w.inFlightEvents+1 > maxInFlightEvents || w.inFlightBytes+bytes > maxInFlightBytes) {
		return false
	}
	sequential := w.merger.sequential()
	if !w.canWrite(dml, sequential, w.lastInFlight(dml.PhysicalTableID), w.lastPending(dml.PhysicalTableID)) {
		return false
	}
	row := &dmlWrite{
		dml:        dml,
		bytes:      bytes,
		sequential: sequential,
		memory:     w.memory,
	}
	if len(w.pendingDML) == 0 {
		w.pendingPrepared = false
	}
	w.pendingDML = append(w.pendingDML, row)
	w.progressChanged = true
	return true
}

// AddDDL queues a block event. release is called once the event is written or
// proven unnecessary.
func (w *Writer) AddDDL(block *event.DDLEvent, release func()) {
	w.pendingDDL = append(w.pendingDDL, &ddlWrite{ddl: block, release: release})
	w.progressChanged = true
}

// lastInFlight returns the newest in-flight write for tableID. Admission only
// needs that write, so scanning avoids building a per-table map for every row.
func (w *Writer) lastInFlight(tableID int64) *dmlWrite {
	for _, batch := range slices.Backward(w.inFlight) {
		for _, item := range slices.Backward(batch.items) {
			if item.dml.PhysicalTableID == tableID {
				return item
			}
		}
	}
	return nil
}

// lastPending returns the newest pending write for tableID.
func (w *Writer) lastPending(tableID int64) *dmlWrite {
	for _, item := range slices.Backward(w.pendingDML) {
		if item.dml.PhysicalTableID == tableID {
			return item
		}
	}
	return nil
}

// Progress reports the input watermark and the earliest decoded work that has
// not reached the writer yet.
type Progress struct {
	Watermark     uint64
	PendingTables map[int64]uint64
	PendingDDL    uint64
}

// Advance records scheduler progress used to compute safe checkpoints.
func (w *Writer) Advance(progress Progress) {
	w.watermark = max(w.watermark, progress.Watermark)
	w.resetUnsubmitted()
	for table, ts := range progress.PendingTables {
		if w.unsubmittedTables == nil {
			w.unsubmittedTables = make(map[int64]uint64)
		}
		if previous, ok := w.unsubmittedTables[table]; !ok || ts < previous {
			w.unsubmittedTables[table] = ts
		}
	}
	if progress.PendingDDL != 0 {
		w.trackUnsubmitted(progress.PendingDDL)
	}
	w.progressChanged = true
}

// Checkpoint returns the last durable checkpoint.
func (w *Writer) Checkpoint() uint64 {
	return w.checkpoint
}

// Done signals that a batch has finished flushing.
func (w *Writer) Done() <-chan struct{} {
	return w.completed
}

// Idle reports that no decoded work is waiting for the sink.
func (w *Writer) Idle() bool {
	return len(w.pendingDML) == 0 && len(w.pendingDDL) == 0 && len(w.inFlight) == 0
}

// HasPendingDML reports whether decoded rows are still waiting for the sink.
func (w *Writer) HasPendingDML() bool {
	return len(w.pendingDML) != 0
}

func (w *Writer) pendingDMLBlocks(block *event.DDLEvent) bool {
	for _, item := range w.pendingDML {
		if item.dml.CommitTs <= block.GetCommitTs() && ddl.BlocksTable(block, item.dml) {
			return true
		}
	}
	return false
}

func (w *Writer) inFlightBlocks(block *event.DDLEvent) bool {
	for _, batch := range w.inFlight {
		if batchBlocks(batch, block) {
			return true
		}
	}
	return false
}

func batchBlocks(batch *writeBatch, block *event.DDLEvent) bool {
	for _, item := range batch.items {
		if ddl.BlocksTable(block, item.dml) {
			return true
		}
	}
	return false
}

func (w *Writer) writeDDL(ctx context.Context, result *ddlWrite) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := w.target.FlushDMLBeforeBlock(result.ddl); err != nil {
		return err
	}
	if err := w.target.WriteBlockEvent(result.ddl); err != nil {
		return err
	}
	if result.release != nil {
		result.release()
	}
	return nil
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
			if result.release != nil {
				result.release()
			}
			w.pendingDDL[0] = nil
			w.pendingDDL = w.pendingDDL[1:]
			w.progressChanged = true
			continue
		}
		if w.pendingDMLBlocks(block) || w.inFlightBlocks(block) {
			return nil
		}
		w.pendingDDL[0] = nil
		w.pendingDDL = w.pendingDDL[1:]
		w.progressChanged = true
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
	w.advanceCheckpoint()
	return nil
}

func (w *Writer) mergePendingDML() {
	if len(w.pendingDML) == 0 || w.pendingPrepared {
		return
	}
	w.pendingDML = w.merger.merge(w.pendingDML)
	for _, item := range w.pendingDML {
		item.memory = w.memory
		item.dml.AddPostFlushFunc(item.release)
	}
	w.pendingPrepared = true
}

func (w *Writer) Flush(ctx context.Context) error {
	w.finishBatches()
	w.mergePendingDML()
	// Retire replayed DDLs before they can fence later rows of the table.
	if err := w.flushDDL(ctx); err != nil {
		return err
	}
	active := make(map[int64]*dmlWrite)
	blocked := make(map[int64]bool)
	tables := make(map[int64]*dmlWrite)
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

func (w *Writer) selectBatch(active map[int64]*dmlWrite, blocked map[int64]bool, tables map[int64]*dmlWrite) (*writeBatch, int) {
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
	items := make([]*dmlWrite, 0, min(batchRows, len(w.pendingDML)))
	for index, item := range w.pendingDML {
		id := item.dml.PhysicalTableID
		if blocked[id] {
			continue
		}
		if !w.canWrite(item.dml, item.sequential, active[id], tables[id]) {
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

func (w *Writer) canWrite(dml *event.DMLEvent, sequential bool, earlier, previous *dmlWrite) bool {
	for _, control := range w.pendingDDL {
		if dml.CommitTs > control.ddl.GetCommitTs() && ddl.BlocksTable(control.ddl, dml) {
			return false
		}
	}
	if earlier != nil && (earlier.sequential || sequential || !sameTableSchema(earlier.dml.TableInfo, dml.TableInfo)) {
		return false
	}
	if previous != nil &&
		(!sameTableSchema(previous.dml.TableInfo, dml.TableInfo) || ((previous.sequential || sequential) && previous.dml.CommitTs != dml.CommitTs)) {
		return false
	}
	return true
}

func (w *Writer) prepareBatch(ctx context.Context, batch *writeBatch) error {
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
			copyBytes := filtered.Rows.MemoryUsage()
			w.memory.ForceAcquire(copyBytes)
			filtered.AddPostFlushFunc(func() {
				w.memory.Release(copyBytes)
			})
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

func (w *Writer) submitBatch(ctx context.Context, batch *writeBatch, end int) error {
	batch.finished = make([]atomic.Bool, len(batch.events))
	w.inFlight = append(w.inFlight, batch)
	w.inFlightBytes += batch.bytes
	w.inFlightEvents += len(batch.items)
	w.removeDML(batch.items, end)
	if len(batch.events) == 0 {
		close(batch.done)
	}
	for index, dml := range batch.events {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		dml.AddPostFlushFunc(func() {
			w.completeEvent(batch, index)
		})
		w.target.AddDMLEvent(dml)
	}
	return nil
}

func (w *Writer) completeEvent(batch *writeBatch, index int) {
	if !batch.finished[index].CAS(false, true) {
		return
	}
	if batch.flushed.Add(1) == int64(len(batch.events)) {
		close(batch.done)
	}
	select {
	case w.completed <- struct{}{}:
	default:
	}
}

func (w *Writer) removeDML(items []*dmlWrite, end int) {
	if end == len(items) {
		// The common case consumes a prefix without moving the backlog.
		clear(w.pendingDML[:end])
		w.pendingDML = w.pendingDML[end:]
		return
	}
	var selected, retained int
	for _, item := range w.pendingDML {
		if selected < len(items) && item == items[selected] {
			selected++
			continue
		}
		w.pendingDML[retained] = item
		retained++
	}
	clear(w.pendingDML[retained:])
	w.pendingDML = w.pendingDML[:retained]
}

func sameTableSchema(a, b *common.TableInfo) bool {
	return a == b || (a != nil && b != nil && a.GetUpdateTS() == b.GetUpdateTS() &&
		a.GetSchemaName() == b.GetSchemaName() && a.GetTableName() == b.GetTableName() &&
		reflect.DeepEqual(a.GetColumns(), b.GetColumns()) && reflect.DeepEqual(a.GetIndices(), b.GetIndices()))
}

func (w *Writer) waitBatch(ctx context.Context, batch *writeBatch) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-batch.done:
		w.finishBatches()
		return nil
	}
}

func (w *Writer) finishBatches() {
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
			if w.mutations[key] == batch {
				w.mutations[key] = nil
			}
		}
		w.inFlightBytes -= batch.bytes
		w.inFlightEvents -= len(batch.items)
		w.progressChanged = true
	}
	clear(w.inFlight[len(remaining):])
	w.inFlight = remaining
	w.advanceCheckpoint()
}

func (w *Writer) resetUnsubmitted() {
	w.unsubmitted, w.hasUnsubmitted = math.MaxUint64, false
	clear(w.unsubmittedTables)
}

func (w *Writer) trackUnsubmitted(ts uint64) {
	w.unsubmitted, w.hasUnsubmitted = min(w.unsubmitted, ts), true
}

func trackUnfinishedTable(tables map[int64]uint64, dml *event.DMLEvent) {
	if tables == nil {
		return
	}
	if before, ok := tables[dml.PhysicalTableID]; !ok || dml.CommitTs < before {
		tables[dml.PhysicalTableID] = dml.CommitTs
	}
}

func (w *Writer) advanceCheckpoint() {
	if !w.progressChanged {
		return
	}
	w.progressChanged = false
	if w.watermark == 0 {
		return
	}
	// A watermark covers every input through its timestamp. Checkpoints also
	// account for decoded events still awaiting delivery in the conductor.
	var (
		unfinished uint64 = math.MaxUint64
		pending    bool
	)
	if w.hasUnsubmitted {
		unfinished, pending = w.unsubmitted, true
	}
	var unfinishedTables map[int64]uint64
	if len(w.mutations) != 0 {
		unfinishedTables = make(map[int64]uint64, len(w.unsubmittedTables))
		maps.Copy(unfinishedTables, w.unsubmittedTables)
	}
	for _, control := range w.pendingDDL {
		unfinished, pending = min(unfinished, control.ddl.GetCommitTs()), true
	}
	for _, item := range w.pendingDML {
		unfinished, pending = min(unfinished, item.dml.CommitTs), true
		trackUnfinishedTable(unfinishedTables, item.dml)
	}
	for _, batch := range w.inFlight {
		for index, dml := range batch.events {
			if !batch.finished[index].Load() {
				unfinished, pending = min(unfinished, dml.CommitTs), true
				trackUnfinishedTable(unfinishedTables, dml)
			}
		}
	}
	through := w.watermark
	if pending {
		through = 0
		if unfinished > 0 {
			through = min(w.watermark, unfinished-1)
		}
	}
	w.checkpoint = max(w.checkpoint, through)
	// Reclaim each table after input progress and its own earlier DML pass.
	// Keep a table boundary to reject replays while unrelated writes still
	// hold back the global checkpoint and DDL history.
	for key := range w.mutations {
		through := w.watermark
		if unfinished, exists := unfinishedTables[key.tableID]; exists {
			through = 0
			if unfinished > 0 {
				through = min(w.watermark, unfinished-1)
			}
		}
		if key.commitTs > 0 && key.commitTs <= through {
			delete(w.mutations, key)
			if through > w.checkpoint {
				if w.tableCheckpoints == nil {
					w.tableCheckpoints = make(map[int64]uint64)
				}
				w.tableCheckpoints[key.tableID] = max(w.tableCheckpoints[key.tableID], through)
			}
		}
	}
	for id, checkpoint := range w.tableCheckpoints {
		if checkpoint <= w.checkpoint {
			delete(w.tableCheckpoints, id)
		}
	}
	for key := range w.ddlWrites {
		if key.CommitTs > 0 && key.CommitTs <= w.checkpoint {
			delete(w.ddlWrites, key)
		}
	}
}

func (w *Writer) filterRows(ctx context.Context, dml *event.DMLEvent, batch *writeBatch) (*event.DMLEvent, error) {
	if dml.CommitTs > 0 && dml.CommitTs <= max(w.checkpoint, w.tableCheckpoints[dml.PhysicalTableID]) {
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
