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
// turns both into sink work.
package conductor

import (
	"cmp"
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
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
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

// ddlTask holds a decoded DDL waiting for every earlier change.
type ddlTask struct {
	ddl *event.DDLEvent
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
			if err := c.acceptBatch(future.Events, future.Payload); err != nil {
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

func (c *Conductor) acceptBatch(batch []event.Event, input *source.Payload) error {
	var rows int
	for _, work := range batch {
		if dml, ok := work.(*event.DMLEvent); ok {
			rows += int(dml.Len())
		}
	}
	if rows > 0 {
		c.inputRows += rows
		if c.validator != nil {
			c.validator.CheckInput(input, c.inputRows)
		}
	}
	for _, work := range batch {
		if err := c.accept(work, input); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conductor) accept(work event.Event, input *source.Payload) error {
	switch work := work.(type) {
	case *event.DMLEvent:
		c.inputMarker, c.hasInputMarker = max(c.inputMarker, work.CommitTs), true
		c.queueDML(&dmlTask{dml: work, partition: input.Partition, offset: input.Offset})
	case *event.ResolvedEvent:
		c.advanceWatermark(work.ResolvedTs, input.Partition)
	case *event.DDLEvent:
		c.inputMarker, c.hasInputMarker = max(c.inputMarker, work.GetCommitTs()), true
		if err := setDDLTableNames(work); err != nil {
			return err
		}
		if c.notifier != nil {
			tables := []event.SchemaTableName{{SchemaName: work.SchemaName, TableName: work.TableName}}
			if work.Type == byte(model.ActionRenameTable) && len(work.BlockedTableNames) > 0 {
				tables = append(tables, work.BlockedTableNames[0])
			}
			c.notifier.MarkProcessed(tables, work.GetCommitTs())
		}
		c.queueDDL(work)
	case nil:
		// A source position becomes confirmable only after all its events
		// have been decoded and registered.
		if input.Ack != nil && c.hasInputMarker {
			c.positionsMu.Lock()
			c.positions[input.Partition] = append(c.positions[input.Partition], sourcePosition{through: c.inputMarker, ack: input.Ack})
			c.positionsMu.Unlock()
			notifyConfirmation(c.confirmable)
		}
		c.inputRows, c.inputMarker, c.hasInputMarker = 0, 0, false
	}
	return nil
}

// queueDML records a decoded row until the writer can accept it.
func (c *Conductor) queueDML(item *dmlTask) {
	rows := append(c.pendingDML[item.dml.PhysicalTableID], item)
	// A source may deliver an earlier timestamp before resolution.
	if len(rows) > 1 && item.dml.CommitTs < rows[len(rows)-2].dml.CommitTs {
		rows = rows[:len(rows)-1]
		index := sort.Search(len(rows), func(i int) bool {
			return rows[i].dml.CommitTs > item.dml.CommitTs
		})
		rows = slices.Insert(rows, index, item)
	}
	c.pendingDML[item.dml.PhysicalTableID] = rows
}

// queueDDL records a DDL once. Replicas of the same DDL are dropped.
func (c *Conductor) queueDDL(block *event.DDLEvent) {
	key := ddl.KeyOf(block)
	if _, ok := c.queuedDDLs[key]; ok {
		return
	}
	c.queuedDDLs[key] = struct{}{}
	task := &ddlTask{ddl: block}
	c.pendingDDL = append(c.pendingDDL, task)
	if len(c.pendingDDL) > 1 && task.ddl.GetCommitTs() < c.pendingDDL[len(c.pendingDDL)-2].ddl.GetCommitTs() {
		c.pendingDDL = c.pendingDDL[:len(c.pendingDDL)-1]
		index := sort.Search(len(c.pendingDDL), func(i int) bool {
			return c.pendingDDL[i].ddl.GetCommitTs() > task.ddl.GetCommitTs()
		})
		c.pendingDDL = slices.Insert(c.pendingDDL, index, task)
	}
}

// nextReady returns the DDL at the head of the pending order and the next row
// that can be written. The DDL wins whenever it is not blocked by rows.
func (c *Conductor) nextReady() (*ddlTask, *dmlTask) {
	if len(c.pendingDDL) != 0 {
		head := c.pendingDDL[0]
		if c.ddlReady(head) && !c.rowsBefore(head.ddl) {
			return head, nil
		}
	}
	return nil, c.nextDML()
}

// ddlReady reports whether every row before the DDL has been read.
func (c *Conductor) ddlReady(task *ddlTask) bool {
	if len(c.partitionWatermarks) == 0 || task.ddl.GetCommitTs() <= c.watermark {
		return true
	}
	// Creating a schema or a table cannot depend on earlier rows.
	switch model.ActionType(task.ddl.Type) {
	case model.ActionCreateSchema, model.ActionCreateTable:
		return true
	}
	return false
}

// rowsBefore reports whether a ready row must be written before the DDL.
func (c *Conductor) rowsBefore(block *event.DDLEvent) bool {
	for _, rows := range c.pendingDML {
		if len(rows) == 0 {
			continue
		}
		row := rows[0]
		if row.dml.CommitTs > block.GetCommitTs() || !ddl.BlocksTable(block, row.dml) {
			continue
		}
		if c.rowReady(row) {
			return true
		}
	}
	return false
}

// rowReady reports whether the watermark covers the row and no pending DDL
// changes its table. A source without partitions orders its inputs itself.
func (c *Conductor) rowReady(item *dmlTask) bool {
	if len(c.partitionWatermarks) != 0 && item.dml.CommitTs > c.watermark {
		return false
	}
	for _, block := range c.pendingDDL {
		if item.dml.CommitTs > block.ddl.GetCommitTs() && ddl.BlocksTable(block.ddl, item.dml) {
			return false
		}
	}
	return true
}

// nextDML returns the earliest row that can be written.
func (c *Conductor) nextDML() *dmlTask {
	var first *dmlTask
	for _, rows := range c.pendingDML {
		if len(rows) == 0 {
			continue
		}
		row := rows[0]
		if !c.rowReady(row) {
			continue
		}
		if first == nil || row.dml.CommitTs < first.dml.CommitTs ||
			(row.dml.CommitTs == first.dml.CommitTs && row.dml.PhysicalTableID < first.dml.PhysicalTableID) {
			first = row
		}
	}
	return first
}

func (c *Conductor) popDML(row *dmlTask) {
	table := row.dml.PhysicalTableID
	rows := c.pendingDML[table]
	rows[0] = nil
	if len(rows) > 1 {
		c.pendingDML[table] = rows[1:]
		return
	}
	delete(c.pendingDML, table)
}

func (c *Conductor) popDDL() *ddlTask {
	task := c.pendingDDL[0]
	delete(c.queuedDDLs, ddl.KeyOf(task.ddl))
	c.pendingDDL[0] = nil
	c.pendingDDL = c.pendingDDL[1:]
	return task
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
			rows  int
			bytes int64
		)
		for rows < roundRows && bytes < roundBytes {
			ddl, row := c.nextReady()
			if ddl != nil {
				c.popDDL()
				w.AddDDL(ddl.ddl)
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
		if rows == 0 {
			return nil
		}
	}
}

// conductor coordinates write ordering and completion progress.
type Conductor struct {
	inputRows      int
	inputMarker    uint64
	hasInputMarker bool

	positions   map[int32][]sourcePosition
	positionsMu sync.Mutex

	reader    source.Reader
	validator source.Validator
	notifier  source.SchemaNotifier

	confirmable  chan struct{}
	checkpoint   *atomic.Uint64
	onCheckpoint func(uint64)

	partitionWatermarks []uint64
	watermark           uint64

	pendingDML map[int64][]*dmlTask
	pendingDDL []*ddlTask
	queuedDDLs map[ddl.Key]struct{}
}

func New(reader source.Reader) *Conductor {
	c := &Conductor{
		reader:              reader,
		confirmable:         make(chan struct{}, 1),
		checkpoint:          new(atomic.Uint64),
		positions:           make(map[int32][]sourcePosition),
		partitionWatermarks: make([]uint64, reader.Partitions()),
		pendingDML:          make(map[int64][]*dmlTask),
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

// advanceWatermark records resolved progress. Sources without partitions
// publish a global watermark; partitioned sources advance only when every
// partition has been resolved.
func (c *Conductor) advanceWatermark(ts uint64, partition int32) {
	if len(c.partitionWatermarks) == 0 {
		c.watermark = max(c.watermark, ts)
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
}

// source.Payload progress can pass decoded work still awaiting delivery. Publish
// the earliest pending timestamp so the writer never checkpoints past it.
func (c *Conductor) advanceWriter(w *writer.Writer) {
	var pending uint64
	for _, rows := range c.pendingDML {
		if len(rows) == 0 {
			continue
		}
		if ts := rows[0].dml.CommitTs; pending == 0 || ts < pending {
			pending = ts
		}
	}
	for _, task := range c.pendingDDL {
		if ts := task.ddl.GetCommitTs(); pending == 0 || ts < pending {
			pending = ts
		}
	}
	w.Advance(writer.Progress{Watermark: c.watermark, Pending: pending})
}

// OnCheckpoint registers a callback for durable checkpoint advances.
func (c *Conductor) OnCheckpoint(fn func(uint64)) {
	c.onCheckpoint = fn
}

// observeCheckpoint publishes a durable checkpoint and wakes the confirm loop.
func (c *Conductor) observeCheckpoint(checkpoint uint64) {
	previous := c.checkpoint.Swap(checkpoint)
	if previous == checkpoint {
		return
	}
	notifyConfirmation(c.confirmable)
	if checkpoint > previous && c.onCheckpoint != nil {
		c.onCheckpoint(checkpoint)
	}
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
	count := 0
	for _, position := range pending {
		if position.through > checkpoint {
			break
		}
		count++
	}
	return count
}

func (c *Conductor) dropPositions(partition int32, count int) {
	c.positionsMu.Lock()
	current := c.positions[partition]
	clear(current[:count])
	c.positions[partition] = current[count:]
	c.positionsMu.Unlock()
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
				if spec.Tp == ast.AlterTableExchangePartition {
					ddl.BlockedTableNames = appendDDLTableNames(ddl.BlockedTableNames, ddl.SchemaName, spec.NewTable)
				}
			}
		}
	}
	return nil
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
