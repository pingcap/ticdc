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

// Package conductor orders decoded events, resolves DDL dependencies, and
// turns both into sink writes.
package conductor

import (
	"cmp"
	"container/heap"
	"context"
	"maps"
	"math"
	"slices"
	"sort"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// dmlTask holds a decoded row and the source position it was read from.
type dmlTask struct {
	dml       *event.DMLEvent
	partition int32
	offset    int64
}

// sourcePosition is one confirmable input position.
type sourcePosition struct {
	through uint64
	ack     func(context.Context) error
}

const (
	// roundRows bounds the rows handed to the writer in one round.
	roundRows = 1024
	// roundBytes bounds the encoded bytes handed to the writer in one round.
	roundBytes = 2 << 20
)

// Start runs the ordering loop and the confirmation loop.
func (c *Conductor) Start(ctx context.Context, tasks <-chan *decode.Future, w *writer.Writer) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.run(ctx, tasks, w)
	})
	g.Go(func() error {
		return c.confirm(ctx)
	})
	return g.Wait()
}

func (c *Conductor) run(ctx context.Context, tasks <-chan *decode.Future, w *writer.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case future := <-tasks:
			if err := future.Ready(ctx); err != nil {
				return err
			}
			if err := c.acceptFuture(future); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer task handling failed", zap.Error(err))
				}
				return err
			}
			if err := c.flush(ctx, w); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer DML submission failed", zap.Error(err))
				}
				return err
			}
			c.observeCheckpoint(w.Checkpoint())
		}
	}
}

func (c *Conductor) acceptFuture(future *decode.Future) error {
	c.marker = max(c.marker, future.Marker)
	for _, pending := range future.Pending {
		c.addPendingDecode(pending.ID, pending.CommitTs)
	}
	for _, resolved := range future.Resolved {
		c.resolvePendingDecode(resolved)
	}
	return c.acceptBatch(future.Events, future.Payload)
}

func (c *Conductor) addPendingDecode(id, commitTs uint64) {
	c.marker = max(c.marker, commitTs)
	if _, alreadyResolved := c.resolvedDecodes[id]; alreadyResolved {
		delete(c.resolvedDecodes, id)
		return
	}
	c.pendingDecodes[id] = commitTs
	if commitTs != 0 && (c.minPendingDecode == 0 || commitTs < c.minPendingDecode) {
		c.minPendingDecode = commitTs
	}
}

func (c *Conductor) resolvePendingDecode(resolved decode.ResolvedDML) {
	c.queueDML(&dmlTask{dml: resolved.DML, partition: resolved.Partition, offset: resolved.Offset})
	ts, ok := c.pendingDecodes[resolved.ID]
	if !ok {
		c.resolvedDecodes[resolved.ID] = struct{}{}
		return
	}
	delete(c.pendingDecodes, resolved.ID)
	if len(c.pendingDecodes) == 0 {
		c.minPendingDecode = 0
		c.releaseSchemas()
		return
	}
	if ts == c.minPendingDecode {
		var minTs uint64
		for _, pendingTs := range c.pendingDecodes {
			if pendingTs != 0 {
				minTs = earliest(minTs, pendingTs)
			}
		}
		c.minPendingDecode = minTs
		c.releaseSchemas()
	}
}

func (c *Conductor) acceptBatch(events []event.Event, payload *source.Payload) error {
	for _, e := range events {
		if err := c.accept(e, payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conductor) accept(e event.Event, payload *source.Payload) error {
	switch e := e.(type) {
	case *event.DMLEvent:
		c.marker = max(c.marker, e.CommitTs)
		c.queueDML(&dmlTask{dml: e, partition: payload.Partition, offset: payload.Offset})
	case *event.ResolvedEvent:
		c.advanceWatermark(e.ResolvedTs, payload.Partition)
	case *event.DDLEvent:
		c.marker = max(c.marker, e.GetCommitTs())
		if err := setDDLTableNames(e); err != nil {
			return err
		}
		if c.notifier != nil {
			if tables := ddlProcessedTables(e); len(tables) != 0 {
				c.notifier.MarkProcessed(tables, e.GetCommitTs())
			}
		}
		c.queueDDL(e)
	case nil:
		// A source position becomes confirmable once all decoded events from
		// this payload are registered, including idle resolved/skipped-DDL payloads.
		if payload.Ack != nil {
			c.positionsMu.Lock()
			through := c.marker
			if pending := c.positions[payload.Partition]; len(pending) != 0 {
				through = max(through, pending[len(pending)-1].through)
			}
			c.positions[payload.Partition] = append(c.positions[payload.Partition], sourcePosition{through: through, ack: payload.Ack})
			c.positionsMu.Unlock()
			notifyConfirmation(c.confirmable)
		}
		c.marker = 0
	}
	return nil
}

// tableQueue holds one table's pending DMLs ordered by (CommitTs, dmlRowOrder).
type tableQueue struct {
	tableID   int64
	rows      []*dmlTask
	blocked   bool
	heapIndex int
}

type tableHeap []*tableQueue

func (h tableHeap) Len() int { return len(h) }

func (h tableHeap) Less(i, j int) bool {
	a, b := h[i], h[j]
	if a.rows[0].dml.CommitTs != b.rows[0].dml.CommitTs {
		return a.rows[0].dml.CommitTs < b.rows[0].dml.CommitTs
	}
	if oa, ob := dmlRowOrder(a.rows[0].dml), dmlRowOrder(b.rows[0].dml); oa != ob {
		return oa < ob
	}
	return a.tableID < b.tableID
}

func (h tableHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *tableHeap) Push(x any) {
	q := x.(*tableQueue)
	q.heapIndex = len(*h)
	*h = append(*h, q)
}

func (h *tableHeap) Pop() any {
	old := *h
	n := len(old)
	q := old[n-1]
	old[n-1] = nil
	q.heapIndex = -1
	*h = old[:n-1]
	return q
}

// dmlRowOrder orders same-table, same-CommitTs events when a split UPDATE
// (DELETE + INSERT) or same-transaction DELETE/UPDATE/INSERT pair arrives out of
// order across partitions or storage files.
func dmlRowOrder(dml *event.DMLEvent) int {
	if len(dml.RowTypes) == 0 {
		return 1
	}
	first := dml.RowTypes[0]
	for _, rt := range dml.RowTypes[1:] {
		if rt != first {
			return 1
		}
	}
	switch first {
	case common.RowTypeDelete:
		return 0
	case common.RowTypeInsert:
		return 2
	default:
		return 1
	}
}

func dmlBefore(a, b *dmlTask) bool {
	if a.dml.CommitTs != b.dml.CommitTs {
		return a.dml.CommitTs < b.dml.CommitTs
	}
	return dmlRowOrder(a.dml) < dmlRowOrder(b.dml)
}

func insertDML(rows []*dmlTask, item *dmlTask) ([]*dmlTask, bool) {
	if len(rows) == 0 || !dmlBefore(item, rows[len(rows)-1]) {
		return append(rows, item), len(rows) == 0
	}
	index := sort.Search(len(rows), func(i int) bool {
		return dmlBefore(item, rows[i])
	})
	return slices.Insert(rows, index, item), index == 0
}

func (c *Conductor) rowBlockedByDDL(item *dmlTask) bool {
	for _, block := range c.pendingDDL {
		if item.dml.CommitTs > block.GetCommitTs() && ddl.BlocksTable(block, item.dml) {
			return true
		}
	}
	return false
}

func (c *Conductor) syncTable(q *tableQueue) {
	blocked := c.rowBlockedByDDL(q.rows[0])
	if blocked == q.blocked {
		if blocked {
			heap.Fix(&c.blockedHeap, q.heapIndex)
		} else {
			heap.Fix(&c.readyHeap, q.heapIndex)
		}
		return
	}
	if q.blocked {
		heap.Remove(&c.blockedHeap, q.heapIndex)
		q.blocked = false
		heap.Push(&c.readyHeap, q)
		return
	}
	heap.Remove(&c.readyHeap, q.heapIndex)
	q.blocked = true
	heap.Push(&c.blockedHeap, q)
}

// queueDML records a decoded row until the writer can accept it.
func (c *Conductor) queueDML(item *dmlTask) {
	tableID := item.dml.PhysicalTableID
	q := c.pendingDML[tableID]
	if q == nil {
		q = &tableQueue{
			tableID: tableID,
			rows:    []*dmlTask{item},
			blocked: c.rowBlockedByDDL(item),
		}
		c.pendingDML[tableID] = q
		if q.blocked {
			heap.Push(&c.blockedHeap, q)
		} else {
			heap.Push(&c.readyHeap, q)
		}
		return
	}
	var headChanged bool
	// A source may deliver an earlier timestamp or split DELETE before resolution.
	q.rows, headChanged = insertDML(q.rows, item)
	if headChanged {
		c.syncTable(q)
	}
}

// queueDDL records a DDL once. Replicas of the same DDL are dropped.
func (c *Conductor) queueDDL(ddlEvent *event.DDLEvent) {
	key := ddl.KeyOf(ddlEvent)
	if _, ok := c.queuedDDLs[key]; ok {
		return
	}
	c.queuedDDLs[key] = struct{}{}
	c.pendingDDL = insertSorted(c.pendingDDL, ddlEvent.GetCommitTs(), ddlEvent, func(block *event.DDLEvent) uint64 {
		return block.GetCommitTs()
	})
	var blocked []*tableQueue
	for _, q := range c.readyHeap {
		if q.rows[0].dml.CommitTs > ddlEvent.GetCommitTs() && ddl.BlocksTable(ddlEvent, q.rows[0].dml) {
			blocked = append(blocked, q)
		}
	}
	for _, q := range blocked {
		heap.Remove(&c.readyHeap, q.heapIndex)
		q.blocked = true
		heap.Push(&c.blockedHeap, q)
	}
}

// insertSorted appends item unless it precedes the last element, in which case
// it is inserted after every element with an earlier timestamp.
func insertSorted[T any](items []T, ts uint64, item T, timestamp func(T) uint64) []T {
	if len(items) == 0 || ts >= timestamp(items[len(items)-1]) {
		return append(items, item)
	}
	index := sort.Search(len(items), func(i int) bool {
		return timestamp(items[i]) > ts
	})
	return slices.Insert(items, index, item)
}

// nextReady returns the DDL at the head of the pending order and the next row
// that can be written. The DDL wins whenever it is not blocked by rows.
func (c *Conductor) nextReady() (*event.DDLEvent, *dmlTask) {
	if len(c.pendingDDL) != 0 {
		head := c.pendingDDL[0]
		if c.ddlReady(head) && !c.rowsBefore(head) {
			return head, nil
		}
	}
	return nil, c.nextDML()
}

// ddlReady reports whether every row before the DDL has been read.
func (c *Conductor) ddlReady(ddl *event.DDLEvent) bool {
	if ddl.GetCommitTs() <= c.effectiveWatermark() {
		return true
	}
	// Creating a schema or standalone table cannot depend on earlier rows.
	switch model.ActionType(ddl.Type) {
	case model.ActionCreateSchema:
		return true
	case model.ActionCreateTable:
		return len(ddl.BlockedTableNames) == 0
	}
	return false
}

// rowsBefore reports whether a ready row must be written before the DDL.
func (c *Conductor) rowsBefore(block *event.DDLEvent) bool {
	return c.hasRowBefore(0, min(c.effectiveWatermark(), block.GetCommitTs()), block)
}

func (c *Conductor) hasRowBefore(index int, limit uint64, block *event.DDLEvent) bool {
	if index >= len(c.readyHeap) {
		return false
	}
	row := c.readyHeap[index].rows[0]
	if row.dml.CommitTs > limit {
		return false
	}
	if ddl.BlocksTable(block, row.dml) {
		return true
	}
	return c.hasRowBefore(2*index+1, limit, block) || c.hasRowBefore(2*index+2, limit, block)
}

// nextDML returns the earliest row that can be written.
func (c *Conductor) nextDML() *dmlTask {
	if len(c.readyHeap) == 0 {
		return nil
	}
	head := c.readyHeap[0].rows[0]
	if head.dml.CommitTs > c.effectiveWatermark() {
		return nil
	}
	return head
}

func (c *Conductor) popDML(row *dmlTask) {
	q := c.pendingDML[row.dml.PhysicalTableID]
	q.rows[0] = nil
	q.rows = q.rows[1:]
	if len(q.rows) == 0 {
		if q.blocked {
			heap.Remove(&c.blockedHeap, q.heapIndex)
		} else {
			heap.Remove(&c.readyHeap, q.heapIndex)
		}
		delete(c.pendingDML, q.tableID)
		return
	}
	c.syncTable(q)
}

func (c *Conductor) popDDL() *event.DDLEvent {
	head := c.pendingDDL[0]
	delete(c.queuedDDLs, ddl.KeyOf(head))
	c.pendingDDL[0] = nil
	c.pendingDDL = c.pendingDDL[1:]
	var unblocked []*tableQueue
	for _, q := range c.blockedHeap {
		if !c.rowBlockedByDDL(q.rows[0]) {
			unblocked = append(unblocked, q)
		}
	}
	for _, q := range unblocked {
		heap.Remove(&c.blockedHeap, q.heapIndex)
		q.blocked = false
		heap.Push(&c.readyHeap, q)
	}
	return head
}

// flush hands every ready task to the writer and lets it write.
func (c *Conductor) flush(ctx context.Context, w *writer.Writer) error {
	for {
		c.advanceWriter(w)
		if err := w.Flush(ctx); err != nil {
			return err
		}
		if w.HasPendingDML() {
			return nil
		}
		var (
			rows     int
			bytes    int64
			addedDDL bool
		)
		for rows < roundRows && bytes < roundBytes {
			ddl, row := c.nextReady()
			if ddl != nil {
				c.popDDL()
				w.AddDDL(ddl)
				addedDDL = true
				break
			}
			if row == nil {
				break
			}
			c.popDML(row)
			if c.validator != nil {
				c.validator.CheckDML(row.dml, row.partition, row.offset)
			}
			w.AddDML(row.dml)
			rows += int(row.dml.Len())
			bytes += row.dml.Rows.MemoryUsage()
		}
		if rows == 0 && !addedDDL {
			return nil
		}
	}
}

// conductor coordinates write ordering and completion progress.
type Conductor struct {
	marker uint64

	positions   map[int32][]sourcePosition
	positionsMu sync.Mutex

	reader    source.Reader
	schemas   *codecCommon.TableInfoProvider
	validator source.Validator
	notifier  source.SchemaNotifier

	confirmable chan struct{}
	checkpoint  *atomic.Uint64

	partitionWatermarks []uint64
	watermark           uint64
	pendingDecodes      map[uint64]uint64
	resolvedDecodes     map[uint64]struct{}
	minPendingDecode    uint64

	pendingDML  map[int64]*tableQueue
	readyHeap   tableHeap
	blockedHeap tableHeap
	pendingDDL  []*event.DDLEvent
	queuedDDLs  map[ddl.Key]struct{}
}

func New(reader source.Reader, schemas *codecCommon.TableInfoProvider) *Conductor {
	c := &Conductor{
		reader:              reader,
		schemas:             schemas,
		confirmable:         make(chan struct{}, 1),
		checkpoint:          new(atomic.Uint64),
		positions:           make(map[int32][]sourcePosition),
		partitionWatermarks: make([]uint64, reader.Partitions()),
		pendingDecodes:      make(map[uint64]uint64),
		resolvedDecodes:     make(map[uint64]struct{}),
		pendingDML:          make(map[int64]*tableQueue),
		queuedDDLs:          make(map[ddl.Key]struct{}),
	}
	if validator, ok := reader.(source.Validator); ok {
		c.validator = validator
	}
	if notifier, ok := reader.(source.SchemaNotifier); ok {
		c.notifier = notifier
	}
	return c
}

func (c *Conductor) effectiveWatermark() uint64 {
	if c.minPendingDecode != 0 {
		return min(c.watermark, c.minPendingDecode-1)
	}
	return c.watermark
}

func (c *Conductor) releaseSchemas() {
	if effective := c.effectiveWatermark(); effective != 0 {
		c.schemas.ReleaseBefore(effective)
	}
}

// advanceWatermark records resolved progress. Sources without partitions
// publish a global watermark; partitioned sources advance only when every
// partition has been resolved.
func (c *Conductor) advanceWatermark(ts uint64, partition int32) {
	if len(c.partitionWatermarks) == 0 {
		c.watermark = max(c.watermark, ts)
		c.releaseSchemas()
		return
	}
	if int(partition) >= len(c.partitionWatermarks) {
		return
	}
	c.partitionWatermarks[partition] = max(c.partitionWatermarks[partition], ts)
	watermark := uint64(math.MaxUint64)
	for _, resolved := range c.partitionWatermarks {
		if resolved == 0 {
			return
		}
		watermark = min(watermark, resolved)
	}
	c.watermark = max(c.watermark, watermark)
	c.releaseSchemas()
}

// source.Payload progress can pass decoded events still awaiting delivery. Publish
// the earliest pending timestamp so the writer never checkpoints past it.
func (c *Conductor) advanceWriter(w *writer.Writer) {
	var pending uint64
	if len(c.readyHeap) != 0 {
		pending = earliest(pending, c.readyHeap[0].rows[0].dml.CommitTs)
	}
	if len(c.blockedHeap) != 0 {
		pending = earliest(pending, c.blockedHeap[0].rows[0].dml.CommitTs)
	}
	if len(c.pendingDDL) != 0 {
		pending = earliest(pending, c.pendingDDL[0].GetCommitTs())
	}
	w.Advance(writer.Progress{Watermark: c.effectiveWatermark(), Pending: pending})
}

// observeCheckpoint publishes a durable checkpoint and wakes the confirm loop.
func (c *Conductor) observeCheckpoint(checkpoint uint64) {
	previous := c.checkpoint.Swap(checkpoint)
	if previous == checkpoint {
		return
	}
	notifyConfirmation(c.confirmable)
}

func (c *Conductor) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.confirmable:
			checkpoint := c.checkpoint.Load()
			if checkpoint == 0 {
				continue
			}
			c.confirmPositions(ctx, checkpoint)
		}
	}
}

// confirmPositions acknowledges the checkpoint-covered prefix of every
// partition. Offsets are committed cumulatively, so a failed acknowledgement is
// logged and dropped: the next acknowledgement commits a higher offset.
func (c *Conductor) confirmPositions(ctx context.Context, checkpoint uint64) {
	c.positionsMu.Lock()
	positions := maps.Clone(c.positions)
	c.positionsMu.Unlock()
	for partition, pending := range positions {
		count := confirmablePrefix(pending, checkpoint)
		if count == 0 {
			continue
		}
		if err := pending[count-1].ack(ctx); err != nil && ctx.Err() == nil {
			log.Warn("consumer input confirmation failed", zap.Error(err))
		}
		c.dropPositions(partition, count)
	}
}

func confirmablePrefix(pending []sourcePosition, checkpoint uint64) int {
	return sort.Search(len(pending), func(i int) bool {
		return pending[i].through > checkpoint
	})
}

func (c *Conductor) dropPositions(partition int32, count int) {
	c.positionsMu.Lock()
	defer c.positionsMu.Unlock()
	pending := c.positions[partition]
	if count <= 0 || len(pending) == 0 {
		return
	}
	count = min(count, len(pending))
	remaining := len(pending) - count
	if remaining == 0 {
		clear(pending)
		if cap(pending) > 1024 {
			c.positions[partition] = nil
			return
		}
		c.positions[partition] = pending[:0]
		return
	}
	copy(pending, pending[count:])
	clear(pending[remaining:])
	c.positions[partition] = pending[:remaining]
}

func earliest(current, ts uint64) uint64 {
	if current == 0 || ts < current {
		return ts
	}
	return current
}

func setDDLTableNames(ddl *event.DDLEvent) error {
	// Decoder table IDs describe metadata already observed, not the complete
	// scope of rename, exchange, or CREATE TABLE LIKE. Extract their logical names.
	switch model.ActionType(ddl.Type) {
	case model.ActionRenameTable, model.ActionRenameTables, model.ActionExchangeTablePartition, model.ActionCreateTable:
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "read consumer DDL scope")
		}
		switch stmt := stmt.(type) {
		case *ast.CreateTableStmt:
			// CREATE fences its new table and, for LIKE, the source table.
			ddl.BlockedTables = &event.InfluencedTables{InfluenceType: event.InfluenceTypeNormal}
			if stmt.ReferTable != nil {
				ddl.BlockedTableNames = appendDDLTableNames(ddl.BlockedTableNames, ddl.SchemaName, stmt.Table, stmt.ReferTable)
			}
		case *ast.RenameTableStmt:
			for _, pair := range stmt.TableToTables {
				ddl.BlockedTableNames = appendDDLTableNames(ddl.BlockedTableNames, ddl.SchemaName, pair.OldTable, pair.NewTable)
			}
		case *ast.AlterTableStmt:
			ddl.BlockedTableNames = appendDDLTableNames(ddl.BlockedTableNames, ddl.SchemaName, stmt.Table)
			for _, spec := range stmt.Specs {
				if spec.Tp == ast.AlterTableExchangePartition || spec.Tp == ast.AlterTableRenameTable {
					ddl.BlockedTableNames = appendDDLTableNames(ddl.BlockedTableNames, ddl.SchemaName, spec.NewTable)
				}
			}
		}
	}
	return nil
}

func ddlProcessedTables(ddlEvent *event.DDLEvent) []event.SchemaTableName {
	var tables []event.SchemaTableName
	add := func(schema, table string) {
		if schema == "" && table == "" {
			return
		}
		entry := event.SchemaTableName{SchemaName: schema, TableName: table}
		if !slices.Contains(tables, entry) {
			tables = append(tables, entry)
		}
	}
	add(ddlEvent.SchemaName, ddlEvent.TableName)
	add(ddlEvent.ExtraSchemaName, ddlEvent.ExtraTableName)
	for _, name := range ddlEvent.BlockedTableNames {
		add(name.SchemaName, name.TableName)
	}
	for _, info := range ddlEvent.MultipleTableInfos {
		if info != nil {
			add(info.GetSchemaName(), info.GetTableName())
		}
	}
	return tables
}

func appendDDLTableNames(names []event.SchemaTableName, defaultSchema string, tables ...*ast.TableName) []event.SchemaTableName {
	for _, table := range tables {
		if table == nil {
			continue
		}
		names = append(names, event.SchemaTableName{
			SchemaName: cmp.Or(table.Schema.O, defaultSchema),
			TableName:  table.Name.O,
		})
	}
	return names
}

func notifyConfirmation(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
