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
	"math"
	"slices"
	"sort"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// pendingTask is private scheduling state. Only decodeTask has a conversion
// result; DDL and checkpoint tasks never carry row-conversion fields.
type pendingTask interface{ pendingTask() }

func (*decodeTask) pendingTask() {}
func (*ddlTask) pendingTask()    {}

type ddlTask struct {
	ddl         *event.DDLEvent
	memory      *memoryBudget
	bytes       int64
	boundary    *readBoundary
	awaitingDDL bool
}

func (t *ddlTask) release() {
	t.memory.release(t.bytes)
}

type watermark struct {
	// All input through watermark has been read and registered.
	watermark uint64
}

// One loop owns dependency selection, conversion collection and writer queues.
// Protocol decoding and sink execution run independently of that state.
func (c *conductor) start(ctx context.Context, tasks <-chan []task, w *writer) error {
	c.g, ctx = errgroup.WithContext(ctx)
	c.g.Go(func() error { return c.decoder.start(ctx) })
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case batch, ok := <-tasks:
			if !ok {
				tasks = nil
				continue
			}
			for _, work := range batch {
				if err := c.accept(ctx, work); err != nil {
					if ctx.Err() == nil {
						log.Error("consumer task handling failed", zap.Error(err))
					}
					return err
				}
			}
			c.collectWatermarks()
		case <-c.decoder.completed:
		case <-w.completed:
		}
		if err := c.flush(ctx, w); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DML submission failed", zap.Error(err))
			}
			return err
		}
		if c.checkpoint.Swap(w.checkpoint) != w.checkpoint {
			notifyConfirmation(c.confirmable)
		}
		c.cleanHistory()
		c.advanceReading(w)
	}
}

func (c *conductor) close() {
	if c.g != nil {
		_ = c.g.Wait()
	}
	c.decoder.close()
}

// Resolve only a bounded, converted prefix into the writer. Unselected work
// stays in its input/table group and contributes its head to progress.
func (c *conductor) flush(ctx context.Context, w *writer) error {
	for {
		c.updateWatermark(w)
		if err := w.flushDML(ctx); err != nil {
			return err
		}
		if len(w.pendingDML) != 0 {
			return nil
		}
		delivery := newDelivery(c, w)
		var control *ddlTask
		for delivery.events < batchRows && delivery.rows < batchRows && delivery.bytes < batchBytes {
			item := c.next(delivery)
			if item == nil {
				break
			}
			if ddl, ok := item.(*ddlTask); ok {
				control = ddl
				break
			}
		}
		if delivery.events == 0 && control == nil {
			// Selection can retire a covered DDL replica without delivering work.
			c.updateWatermark(w)
			return w.flushDML(ctx)
		}
		delivery.flush()
		if control != nil {
			w.addDDL(&ddlWrite{ddl: control.ddl, task: control})
		}
	}
}

// delivery admits the rows for one round and buffers the accepted rows for the
// writer. The writer budget, the in-flight rows of the same table, and the DDL
// fence must all allow a task before it is buffered.
type delivery struct {
	conductor *conductor
	writer    *writer
	active    map[int64]*dmlWrite
	tables    map[int64]*dmlWrite
	pending   []*dmlWrite
	merge     bool
	rows      int
	events    int
	bytes     int64
}

func newDelivery(c *conductor, w *writer) *delivery {
	active := make(map[int64]*dmlWrite)
	for _, batch := range w.inFlight {
		for _, row := range batch.items {
			active[row.dml.PhysicalTableID] = row
		}
	}
	return &delivery{
		conductor: c,
		writer:    w,
		active:    active,
		tables:    make(map[int64]*dmlWrite),
		merge:     c.merger.sequential(),
	}
}

func (d *delivery) admit(task *decodeTask) bool {
	if d.writer.inFlightEvents+d.events != 0 &&
		(d.writer.inFlightEvents+d.events+1 > maxInFlightEvents || d.writer.inFlightBytes+d.bytes+task.bytes > maxInFlightBytes) {
		return false
	}
	row := &dmlWrite{dml: task.dml, bytes: task.bytes, sequential: d.merge}
	if !d.writer.canWrite(row, d.active, d.tables) {
		return false
	}
	d.tables[row.dml.PhysicalTableID] = row
	d.pending = append(d.pending, row)
	d.rows += int(task.dml.Len())
	d.events++
	d.bytes += task.bytes
	return true
}

func (d *delivery) flush() {
	for _, row := range d.conductor.merger.merge(d.pending) {
		d.writer.addDML(row)
	}
	d.pending = d.pending[:0]
	d.bytes = 0
}

// conductor coordinates row conversion, write ordering and completion progress.
type conductor struct {
	reader            Reader
	confirmable       chan<- struct{}
	readThrough       uint64 // Largest observed source marker still needing global coverage.
	ddlInputs         map[ddlKey]*ddlInput
	boundary          *readBoundary
	boundaryTs        uint64
	boundaryDDL       *ddlTask
	checkpoint        *atomic.Uint64
	cleanedCheckpoint uint64
	g                 *errgroup.Group
	memory            *memoryBudget
	streams           map[int32]*streamWatermark
	decoder           *decoderGroup
	watermark         uint64
	merger            rowMerger
	pendingDML        map[int64]map[int32][]*decodeTask
	orderedDML        bool
	pendingDDL        []*ddlTask
	controlTs         uint64
}

type ddlInput struct {
	event   *ddlTask // Nil once the conductor emits or retires the canonical effect.
	streams []int32  // Inputs that have crossed this exact DDL.
}

// queueDML takes ownership of the captured message's quota from the source decoder.
func (c *conductor) queueDML(item *decodeTask) {
	if c.pendingDML == nil {
		c.pendingDML = make(map[int64]map[int32][]*decodeTask)
	}
	groups := c.pendingDML[item.tableID]
	if groups == nil {
		groups = make(map[int32][]*decodeTask)
		c.pendingDML[item.tableID] = groups
	}
	rows := append(groups[item.stream], item)
	// Kafka preserves commit order within a table/partition. Other sources
	// retain the ability to receive an earlier timestamp before resolution.
	if !c.orderedDML && len(rows) > 1 && item.commitTs < rows[len(rows)-2].commitTs {
		slices.SortStableFunc(rows, func(a, b *decodeTask) int {
			return cmp.Compare(a.commitTs, b.commitTs)
		})
	}
	groups[item.stream] = rows
}

// The earliest head across inputs preserves table order while conversions run
// concurrently. Within each group, consuming a prefix just shortens the slice.
func firstDML(groups map[int32][]*decodeTask) *decodeTask {
	var first *decodeTask
	for _, rows := range groups {
		if first == nil || rows[0].commitTs < first.commitTs {
			first = rows[0]
		}
	}
	return first
}

func setDDLTableNames(ddl *event.DDLEvent) error {
	// Decoder table IDs describe metadata already observed, not the complete
	// scope of rename, exchange, or CREATE TABLE LIKE. Extract their logical names.
	switch timodel.ActionType(ddl.Type) {
	case timodel.ActionRenameTable, timodel.ActionRenameTables, timodel.ActionExchangeTablePartition, timodel.ActionCreateTable:
		stmt, err := parser.New().ParseOneStmt(ddl.Query, "", "")
		if err != nil {
			return errors.WrapError(errors.ErrCodecDecode, err, "read consumer DDL scope")
		}
		switch stmt := stmt.(type) {
		case *ast.CreateTableStmt:
			// CREATE fences its new table and, for LIKE, the source table.
			ddl.BlockedTables = &event.InfluencedTables{InfluenceType: event.InfluenceTypeNormal}
			if stmt.ReferTable != nil {
				for _, table := range []*ast.TableName{stmt.Table, stmt.ReferTable} {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{
						SchemaName: cmp.Or(table.Schema.O, ddl.SchemaName),
						TableName:  table.Name.O,
					})
				}
			}
		case *ast.RenameTableStmt:
			for _, pair := range stmt.TableToTables {
				for _, table := range []*ast.TableName{pair.OldTable, pair.NewTable} {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{
						SchemaName: cmp.Or(table.Schema.O, ddl.SchemaName),
						TableName:  table.Name.O,
					})
				}
			}
		case *ast.AlterTableStmt:
			ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{
				SchemaName: cmp.Or(stmt.Table.Schema.O, ddl.SchemaName),
				TableName:  stmt.Table.Name.O,
			})
			for _, spec := range stmt.Specs {
				if spec.Tp == ast.AlterTableExchangePartition && spec.NewTable != nil {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{
						SchemaName: cmp.Or(spec.NewTable.Schema.O, ddl.SchemaName),
						TableName:  spec.NewTable.Name.O,
					})
				}
			}
		}
	}
	return nil
}

func (c *conductor) queueDDL(ddl *event.DDLEvent) *ddlInput {
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName))
	c.memory.reserve(bytes)
	task := &ddlTask{
		ddl:    ddl,
		memory: c.memory,
		bytes:  bytes,
	}
	c.pendingDDL = append(c.pendingDDL, task)
	if c.ddlInputs == nil {
		c.ddlInputs = make(map[ddlKey]*ddlInput)
	}
	key := ddlKey{
		commitTs: ddl.GetCommitTs(),
		schema:   ddl.SchemaName,
		table:    ddl.TableName,
		query:    ddl.Query,
	}
	input := &ddlInput{
		event: task,
	}
	c.ddlInputs[key] = input
	return input
}

func (c *conductor) nextReady(watermark uint64, delivery *delivery) pendingTask {
	c.controlTs = 0
	var head *ddlTask
	for len(c.pendingDDL) != 0 {
		head = c.pendingDDL[0]
		if !head.awaitingDDL {
			break
		}
		covered := head.ddl.GetCommitTs() <= watermark
		if head.boundary != nil && head.boundary.watermark != nil {
			covered = covered || head.ddl.GetCommitTs() <= *head.boundary.watermark
		}
		if !covered {
			break
		}
		// A covering canonical-stream watermark without this DDL means that
		// its canonical input was confirmed before this run. Retire the replica.
		head.awaitingDDL = false
		head.release()
		c.popDDL()
		head = nil
	}
	headReady := head != nil && ddlReady(head, watermark)
	if headReady {
		if result := c.drainDDL(head, watermark, delivery); result != nil {
			return result
		}
	}
	//nolint:revive // A nil *decodeTask converted to pendingTask would be a non-nil interface.
	if row := c.nextDML(watermark, delivery); row != nil {
		return row
	}
	return nil
}

func ddlReady(item *ddlTask, watermark uint64) bool {
	if item.awaitingDDL {
		return false
	}
	if item.ddl.GetCommitTs() <= watermark || (item.boundary != nil && item.boundary.reached) {
		return true
	}
	switch timodel.ActionType(item.ddl.Type) {
	case timodel.ActionCreateSchema:
		return true
	case timodel.ActionCreateTable:
		blocked := item.ddl.GetBlockedTables()
		return blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal &&
			(len(blocked.TableIDs) == 0 || (len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID)) &&
			len(item.ddl.GetBlockedTableNames()) == 0
	default:
		return false
	}
}

// Emit a ready preceding row from the DDL scope, or the DDL once all such
// rows have moved to writer batches. The writer waits for their flush callbacks.
func (c *conductor) drainDDL(head *ddlTask, watermark uint64, delivery *delivery) pendingTask {
	preceding := false
	for _, groups := range c.pendingDML {
		row := firstDML(groups)
		if row.commitTs > head.ddl.GetCommitTs() || !ddlBlocksTask(head.ddl, row) {
			continue
		}
		preceding = true
		if c.rowReady(row, watermark) && delivery.admit(row) {
			return c.popDML(row)
		}
		c.trackDMLWait(row, watermark)
	}
	if preceding {
		return nil
	}
	return c.popDDL()
}

func (c *conductor) popDDL() *ddlTask {
	item := c.pendingDDL[0]
	c.pendingDDL[0] = nil
	c.pendingDDL = c.pendingDDL[1:]
	key := ddlKey{
		commitTs: item.ddl.GetCommitTs(),
		schema:   item.ddl.SchemaName,
		table:    item.ddl.TableName,
		query:    item.ddl.Query,
	}
	if input, exists := c.ddlInputs[key]; exists && input.event == item {
		input.event, input.streams = nil, nil
	}
	return item
}

func (c *conductor) nextDML(watermark uint64, delivery *delivery) *decodeTask {
	for _, groups := range c.pendingDML {
		row := firstDML(groups)
		if c.rowReady(row, watermark) && delivery.admit(row) {
			return c.popDML(row)
		}
		c.trackDMLWait(row, watermark)
	}
	return nil
}

func (c *conductor) rowReady(row *decodeTask, watermark uint64) bool {
	select {
	case <-row.done:
	default:
		return false
	}
	return c.dmlReady(row, watermark) && !slices.ContainsFunc(c.pendingDDL, func(ddl *ddlTask) bool {
		return row.commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, row)
	})
}

func (c *conductor) trackDMLWait(row *decodeTask, watermark uint64) {
	if c.dmlReady(row, watermark) {
		return
	}
	if row.boundary == nil || row.boundary.watermark != nil {
		if c.controlTs == 0 || row.commitTs < c.controlTs {
			c.controlTs = row.commitTs
		}
	}
}

func (c *conductor) dmlReady(item *decodeTask, watermark uint64) bool {
	if item.commitTs <= watermark {
		return true
	}
	if boundary := item.boundary; boundary != nil {
		if boundary.reached || (boundary.watermark != nil && item.commitTs <= *boundary.watermark) {
			return true
		}
		if boundary.watermark == nil {
			return false
		}
	}
	// A later control in this row's scope proves its preceding DDLs are known.
	return slices.ContainsFunc(c.pendingDDL, func(ddl *ddlTask) bool {
		return item.commitTs <= ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, item)
	})
}

func (c *conductor) popDML(row *decodeTask) *decodeTask {
	groups := c.pendingDML[row.tableID]
	rows := groups[row.stream]
	rows[0] = nil
	if len(rows) == 1 {
		delete(groups, row.stream)
		if len(groups) == 0 {
			delete(c.pendingDML, row.tableID)
		}
	} else {
		groups[row.stream] = rows[1:]
	}
	return row
}

func ddlBlocksTask(ddl *event.DDLEvent, task *decodeTask) bool {
	return ddlBlocksIdentity(ddl, task.tableID, task.name.SchemaName, task.name.TableName)
}

func ddlBlocksTable(ddl *event.DDLEvent, dml *event.DMLEvent) bool {
	if dml.TableInfo == nil {
		return true
	}
	return ddlBlocksIdentity(ddl, dml.PhysicalTableID, dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName())
}

func ddlBlocksIdentity(ddl *event.DDLEvent, tableID int64, schema, table string) bool {
	// Incomplete multi-table metadata must retain a global barrier.
	if ddl.SchemaName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeAll) {
		return true
	}
	if (ddl.Type == byte(timodel.ActionRenameTables) || ddl.Type == byte(timodel.ActionExchangeTablePartition)) && len(ddl.BlockedTableNames) == 0 && len(ddl.MultipleTableInfos) == 0 {
		return true
	}
	if (schema == ddl.SchemaName && table == ddl.TableName) ||
		(schema == ddl.ExtraSchemaName && table == ddl.ExtraTableName) {
		return true
	}
	for _, name := range ddl.BlockedTableNames {
		if schema == name.SchemaName && table == name.TableName {
			return true
		}
	}
	for _, info := range ddl.MultipleTableInfos {
		if info != nil && schema == info.GetSchemaName() && table == info.GetTableName() {
			return true
		}
	}
	if ddl.BlockedTables != nil && slices.Contains(ddl.BlockedTables.TableIDs, tableID) {
		return true
	}
	return (ddl.TableName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeDB)) && schema == ddl.SchemaName
}

func newConductor(
	memory *memoryBudget,
	reader Reader,
	streamIDs []int32,
	codecConfig *codecCommon.Config,
	checkpoint *atomic.Uint64,
	confirmable chan struct{},
) *conductor {
	streams := make(map[int32]*streamWatermark, len(streamIDs))
	for _, id := range streamIDs {
		streams[id] = &streamWatermark{}
	}
	_, orderedDML := reader.(*kafkaReader)
	var validator dmlValidator
	if v, ok := reader.(dmlValidator); ok {
		validator = v
	}
	c := &conductor{
		orderedDML:  orderedDML,
		reader:      reader,
		confirmable: confirmable,
		checkpoint:  checkpoint,
		memory:      memory,
		streams:     streams,
		decoder:     newDecoderGroup(memory, make(chan struct{}, 1), validator),
		merger:      newRowMerger(codecConfig, memory),
	}
	return c
}

func (c *conductor) next(delivery *delivery) pendingTask {
	return c.nextReady(c.watermark, delivery)
}

// Scope names are logical event metadata. Transport routing remains reader-owned.
func ddlScope(ddl *event.DDLEvent) []event.SchemaTableName {
	if ddl.SchemaName == "" || ddl.TableName == "" ||
		(ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType != event.InfluenceTypeNormal) {
		return nil
	}
	names := []event.SchemaTableName{{
		SchemaName: ddl.SchemaName,
		TableName:  ddl.TableName,
	}}
	if ddl.ExtraTableName != "" {
		names = append(names, event.SchemaTableName{
			SchemaName: ddl.ExtraSchemaName,
			TableName:  ddl.ExtraTableName,
		})
	}
	names = append(names, ddl.BlockedTableNames...)
	for _, info := range ddl.MultipleTableInfos {
		if info != nil {
			names = append(names, event.SchemaTableName{
				SchemaName: info.GetSchemaName(),
				TableName:  info.GetTableName(),
			})
		}
	}
	return names
}

func (c *conductor) advanceReading(w *writer) {
	var (
		pending *ddlTask
		through uint64
		tables  = []event.SchemaTableName{}
	)
	if len(c.pendingDDL) != 0 {
		head := c.pendingDDL[0]
		if head.awaitingDDL {
			through = head.ddl.GetCommitTs()
		}
		if !head.awaitingDDL && !ddlReady(head, c.watermark) {
			pending, through, tables = head, head.ddl.GetCommitTs(), ddlScope(head.ddl)
		}
	}
	if through == 0 {
		through = c.controlTs
	}
	if through == 0 && c.readThrough > c.watermark {
		through, tables = c.readThrough, nil
		// Finish the current request before starting another global catch-up.
		// The DDL and control dependencies above can still replace this target.
		if c.boundary != nil && !c.boundary.reached && c.boundaryDDL == nil {
			through = c.boundaryTs
		}
	}
	if through == 0 {
		c.boundary, c.boundaryDDL = nil, nil
		c.reader.Boundary([]event.SchemaTableName{}, 0)
		c.reader.ContinueReading(c.needsMoreInput(w))
		return
	}
	if c.boundary != nil && !c.boundary.reached && c.boundaryTs == through && c.boundaryDDL == pending {
		c.reader.ContinueReading(c.needsMoreInput(w))
		return
	}
	c.boundary = c.reader.Boundary(tables, through)
	c.boundaryTs, c.boundaryDDL = through, nil
	if pending != nil && c.boundary != nil {
		pending.boundary = c.boundary
		c.boundaryDDL = pending
		key := ddlKey{
			commitTs: pending.ddl.GetCommitTs(),
			schema:   pending.ddl.SchemaName,
			table:    pending.ddl.TableName,
			query:    pending.ddl.Query,
		}
		var updates []readUpdate
		for _, stream := range c.ddlInputs[key].streams {
			updates = append(updates, readUpdate{
				stream:   stream,
				boundary: c.boundary,
			})
		}
		for _, groups := range c.pendingDML {
			for stream, rows := range groups {
				index := sort.Search(len(rows), func(i int) bool {
					return rows[i].commitTs > pending.ddl.GetCommitTs()
				})
				for _, item := range rows[index:] {
					if ddlBlocksTask(pending.ddl, item) {
						updates = append(updates, readUpdate{stream: stream, boundary: c.boundary})
						break
					}
				}
			}
		}
		c.reader.Advance(updates)
	}
	c.reader.ContinueReading(c.needsMoreInput(w))
}

func (c *conductor) needsMoreInput(w *writer) bool {
	for _, groups := range c.pendingDML {
		if !c.dmlReady(firstDML(groups), c.watermark) {
			return true
		}
	}
	for _, item := range c.pendingDDL {
		if item.ddl.GetCommitTs() > c.watermark && !ddlReady(item, c.watermark) {
			return true
		}
	}
	// Let conversions and writes release memory before reading solely to catch
	// up global progress. An empty pipeline must still drain source buffers and
	// reach markers that release retained transport positions.
	return len(c.pendingDML) == 0 && len(c.pendingDDL) == 0 &&
		len(w.pendingDML) == 0 && len(w.inFlight) == 0 &&
		len(w.pendingDDL) == 0 && w.ddlInFlight == nil
}

// Partition markers establish a global watermark when every stream is covered.
// Sources can also publish a global boundary after proving complete input.
func (c *conductor) collectWatermarks() {
	watermark := uint64(math.MaxUint64)
	for _, stream := range c.streams {
		if !stream.hasWatermark {
			watermark = 0
			break
		}
		watermark = min(watermark, stream.watermark)
	}
	if len(c.streams) != 0 {
		c.watermark = max(c.watermark, watermark)
	}
}

// Input progress can pass conversions still in flight. Register their earliest
// timestamps before letting the writer advance global or table checkpoints.
func (c *conductor) updateWatermark(w *writer) {
	w.watermark = max(w.watermark, c.watermark)
	w.resetUnsubmitted()
	for _, groups := range c.pendingDML {
		w.trackUnsubmittedHead(firstDML(groups))
	}
	for _, ddl := range c.pendingDDL {
		w.trackUnsubmitted(ddl.ddl.GetCommitTs())
	}
	w.progressChanged = true
}

// Reclaim history only through the inclusive downstream checkpoint.
func (c *conductor) cleanHistory() {
	checkpoint := c.checkpoint.Load()
	if checkpoint <= c.cleanedCheckpoint {
		return
	}
	c.cleanedCheckpoint = checkpoint
	for key := range c.ddlInputs {
		if key.commitTs <= checkpoint {
			delete(c.ddlInputs, key)
		}
	}
}

type streamWatermark struct {
	watermark    uint64
	hasWatermark bool
}

func (c *conductor) accept(ctx context.Context, work task) error {
	switch result := work.(type) {
	case *decodeTask:
		c.readThrough = max(c.readThrough, result.commitTs)
		if result.table != nil && result.commitTs > 0 && result.commitTs <= c.checkpoint.Load() {
			result.message = nil
			c.memory.release(result.bytes)
			return nil
		}
		c.queueDML(result)
		if err := c.decoder.submit(ctx, result); err != nil {
			return err
		}
		c.crossBoundary(result.stream, result.commitTs, nil, result)
	case *watermark:
		c.watermark = max(c.watermark, result.watermark)
	case *inputWatermark:
		c.readThrough = max(c.readThrough, result.watermark)
		if result.sourceCheckpoint {
			c.watermark = max(c.watermark, result.watermark)
		} else {
			stream := c.streams[result.stream]
			stream.watermark = max(stream.watermark, result.watermark)
			stream.hasWatermark = true
			c.reader.Advance([]readUpdate{{stream: result.stream, watermark: result.watermark}})
		}
	case *inputDDL:
		c.readThrough = max(c.readThrough, result.ddl.GetCommitTs())
		if err := setDDLTableNames(result.ddl); err != nil {
			return err
		}
		if result.registration != nil {
			defer result.registration.done()
		}
		defer c.crossBoundary(result.scope.stream, result.ddl.GetCommitTs(), result.ddl, nil)
		ddl := result.ddl
		key := ddlKey{
			commitTs: ddl.GetCommitTs(),
			schema:   ddl.SchemaName,
			table:    ddl.TableName,
			query:    ddl.Query,
		}
		input, exists := c.ddlInputs[key]
		if !exists {
			input = c.queueDDL(ddl)
			item := input.event
			item.awaitingDDL = result.scope.ddlCopy
			item.boundary = result.scope.ddlBoundary
			if result.scope.ddlCopy {
				item.boundary = result.scope.dmlBoundary
			}
		}
		if input.event != nil && !slices.Contains(input.streams, result.scope.stream) {
			input.streams = append(input.streams, result.scope.stream)
		}
		if exists && !result.scope.ddlCopy && input.event != nil && input.event.awaitingDDL {
			// Canonical input arrives before its effect can leave the conductor.
			item := input.event
			item.awaitingDDL, item.ddl, item.boundary = false, ddl, result.scope.ddlBoundary
			return nil
		}
		if exists {
			return nil
		}
		if result.scope.ddlOrder == commitOrder {
			slices.SortStableFunc(c.pendingDDL, func(first, second *ddlTask) int {
				return cmp.Compare(first.ddl.GetCommitTs(), second.ddl.GetCommitTs())
			})
		}
	}
	return nil
}

func (c *conductor) crossBoundary(stream int32, commitTs uint64, effect *event.DDLEvent, row *decodeTask) {
	ddl := c.boundaryDDL
	if ddl == nil || ddl.boundary.reached {
		return
	}
	crossed := effect != nil && effect.GetCommitTs() == ddl.ddl.GetCommitTs() && effect.SchemaName == ddl.ddl.SchemaName && effect.TableName == ddl.ddl.TableName && effect.Query == ddl.ddl.Query
	crossed = crossed || (row != nil && commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, row))
	if crossed {
		c.reader.Advance([]readUpdate{{stream: stream, boundary: ddl.boundary}})
	}
}
