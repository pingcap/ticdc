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

// Package conductor orders decoded events, resolves DDL dependencies, tracks
// source progress, and turns both into sink work.
package conductor

import (
	"cmp"
	"context"
	"maps"
	"math"
	"slices"
	"sort"
	"sync"
	"unsafe"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// dmlTask holds a decoded row and the source metadata needed for scheduling.
type dmlTask struct {
	dml    *event.DMLEvent
	stream int32
	offset int64
}

type pendingTask interface {
	pendingTask()
}

func (*dmlTask) pendingTask() {
}

func (*ddlTask) pendingTask() {
}

type ddlTask struct {
	ddl         *event.DDLEvent
	memory      *budget.Budget
	bytes       int64
	boundary    *readBoundary
	awaitingDDL bool
}

func (t *ddlTask) release() {
	t.memory.Release(t.bytes)
}

// readBoundary is scheduler-owned evidence that input dependencies are covered.
type readBoundary struct {
	tables    []event.SchemaTableName
	reached   bool
	watermark *uint64
}

type sourcePosition struct {
	through uint64
	ack     func(context.Context) error
}

const sourcePositionBytes = int64(unsafe.Sizeof(sourcePosition{}))

const (
	roundRows  = 1024
	roundBytes = 2 << 20
)

// One loop owns dependency selection, decoded work and writer queues.
// Protocol decoding and sink execution run independently of that state.
func (c *Conductor) Start(ctx context.Context, tasks <-chan []decode.Event, w *writer.Writer) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return c.run(ctx, tasks, w)
	})
	g.Go(func() error {
		return c.confirm(ctx)
	})
	err := g.Wait()
	for _, positions := range c.positions {
		c.memory.Release(int64(len(positions)) * sourcePositionBytes)
	}
	clear(c.positions)
	return err
}

func (c *Conductor) run(ctx context.Context, tasks <-chan []decode.Event, w *writer.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case batch, ok := <-tasks:
			if !ok {
				tasks = nil
				continue
			}
			if err := c.acceptBatch(batch); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer task handling failed", zap.Error(err))
				}
				return err
			}
			c.collectWatermarks()
		case <-w.Done():
		}
		if err := c.flush(ctx, w); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DML submission failed", zap.Error(err))
			}
			return err
		}
		c.observeCheckpoint(w.Checkpoint())
		c.cleanHistory()
		c.advanceReading(w)
	}
}

func (c *Conductor) acceptBatch(batch []decode.Event) error {
	for _, work := range batch {
		if err := c.accept(work); err != nil {
			return err
		}
	}
	return nil
}

// Resolve only a bounded, decoded prefix into the writer. Unselected work
// stays in its input/table group and contributes its head to progress.
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
			control *ddlTask
			rows    int
			bytes   int64
		)
		for rows < roundRows && bytes < roundBytes {
			item := c.nextReady(c.watermark)
			if item == nil {
				break
			}
			if ddl, ok := item.(*ddlTask); ok {
				control = ddl
				break
			}
			task := item.(*dmlTask)
			if c.validator != nil {
				c.validator.CheckDML(task.dml, task.stream, task.offset)
			}
			if !w.AddDML(task.dml) {
				break
			}
			c.popDML(task)
			rows += int(task.dml.Len())
			bytes += task.dml.Rows.MemoryUsage()
		}
		if rows == 0 && control == nil {
			// Selection can retire a covered DDL replica without delivering work.
			c.advanceWriter(w)
			return w.Flush(ctx)
		}
		if control != nil {
			c.popDDL()
			w.AddDDL(control.ddl, control.release)
		}
	}
}

// Conductor coordinates write ordering and completion progress.
type Conductor struct {
	control           *readBoundary
	inputRows         int
	inputMarker       uint64
	hasInputMarker    bool
	positions         map[int32][]sourcePosition
	positionsMu       sync.Mutex
	reader            source.Reader
	validator         source.Validator
	notifier          source.SchemaNotifier
	confirmable       chan struct{}
	readThrough       uint64 // Largest observed source marker still needing global coverage.
	ddlInputs         map[ddl.Key]*ddlInput
	boundary          *readBoundary
	boundaryTs        uint64
	boundaryDDL       *ddlTask
	checkpoint        *atomic.Uint64
	cleanedCheckpoint uint64
	memory            *budget.Budget
	streams           map[int32]*streamWatermark
	watermark         uint64
	pendingDML        map[int64]map[int32][]*dmlTask
	orderedDML        bool
	reorderDDL        bool
	pendingDDL        []*ddlTask
	controlTs         uint64
	proven            []int32
	onCheckpoint      func(uint64)
}

type ddlInput struct {
	event   *ddlTask // Nil once the Conductor emits or retires the canonical effect.
	streams []int32  // Inputs that have crossed this exact DDL.
}

// queueDML takes ownership of the decoded row's memory quota.
func (c *Conductor) queueDML(item *dmlTask) {
	if c.pendingDML == nil {
		c.pendingDML = make(map[int64]map[int32][]*dmlTask)
	}
	groups := c.pendingDML[item.dml.PhysicalTableID]
	if groups == nil {
		groups = make(map[int32][]*dmlTask)
		c.pendingDML[item.dml.PhysicalTableID] = groups
	}
	rows := append(groups[item.stream], item)
	// Kafka preserves commit order within a table/partition. Other sources
	// retain the ability to receive an earlier timestamp before resolution.
	if !c.orderedDML && len(rows) > 1 && item.dml.CommitTs < rows[len(rows)-2].dml.CommitTs {
		rows = rows[:len(rows)-1]
		index := sort.Search(len(rows), func(i int) bool {
			return rows[i].dml.CommitTs > item.dml.CommitTs
		})
		rows = slices.Insert(rows, index, item)
	}
	groups[item.stream] = rows
}

// The earliest head across inputs preserves table order.
// Within each group, consuming a prefix just shortens the slice.
func firstDML(groups map[int32][]*dmlTask) *dmlTask {
	var first *dmlTask
	for _, rows := range groups {
		if first == nil || rows[0].dml.CommitTs < first.dml.CommitTs {
			first = rows[0]
		}
	}
	return first
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

func (c *Conductor) queueDDL(block *event.DDLEvent) *ddlInput {
	bytes := int64(len(block.Query) + len(block.SchemaName) + len(block.TableName))
	c.memory.ForceAcquire(bytes)
	task := &ddlTask{
		ddl:    block,
		memory: c.memory,
		bytes:  bytes,
	}
	c.pendingDDL = append(c.pendingDDL, task)
	if c.reorderDDL && len(c.pendingDDL) > 1 && task.ddl.GetCommitTs() < c.pendingDDL[len(c.pendingDDL)-2].ddl.GetCommitTs() {
		c.pendingDDL = c.pendingDDL[:len(c.pendingDDL)-1]
		index := sort.Search(len(c.pendingDDL), func(i int) bool {
			return c.pendingDDL[i].ddl.GetCommitTs() > task.ddl.GetCommitTs()
		})
		c.pendingDDL = slices.Insert(c.pendingDDL, index, task)
	}
	if c.ddlInputs == nil {
		c.ddlInputs = make(map[ddl.Key]*ddlInput)
	}
	key := ddl.KeyOf(block)
	input := &ddlInput{
		event: task,
	}
	c.ddlInputs[key] = input
	return input
}

func (c *Conductor) nextReady(watermark uint64) pendingTask {
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
	if head != nil && ddlReady(head, watermark) {
		if result := c.drainDDL(head, watermark); result != nil {
			return result
		}
	}
	//nolint:revive // Assigning a nil *dmlTask to pendingTask yields a non-nil interface.
	if row := c.nextDML(watermark); row != nil {
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
	switch model.ActionType(item.ddl.Type) {
	case model.ActionCreateSchema:
		return true
	case model.ActionCreateTable:
		blocked := item.ddl.GetBlockedTables()
		return blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal &&
			(len(blocked.TableIDs) == 0 || (len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID)) &&
			len(item.ddl.GetBlockedTableNames()) == 0
	default:
		return false
	}
}

// Select a ready preceding row from the DDL scope, or the DDL itself once all
// such rows have moved to the writer. The caller pops the selected task.
func (c *Conductor) drainDDL(head *ddlTask, watermark uint64) pendingTask {
	preceding := false
	for _, groups := range c.pendingDML {
		row := firstDML(groups)
		if row.dml.CommitTs > head.ddl.GetCommitTs() || !ddl.BlocksTable(head.ddl, row.dml) {
			continue
		}
		preceding = true
		if c.rowReady(row, watermark) {
			return row
		}
		c.trackDMLWait(row, watermark)
	}
	if preceding {
		return nil
	}
	return head
}

func (c *Conductor) popDDL() *ddlTask {
	item := c.pendingDDL[0]
	c.pendingDDL[0] = nil
	c.pendingDDL = c.pendingDDL[1:]
	key := ddl.KeyOf(item.ddl)
	if input, exists := c.ddlInputs[key]; exists && input.event == item {
		input.event, input.streams = nil, nil
	}
	return item
}

func (c *Conductor) nextDML(watermark uint64) *dmlTask {
	for _, groups := range c.pendingDML {
		row := firstDML(groups)
		if c.rowReady(row, watermark) {
			return row
		}
		c.trackDMLWait(row, watermark)
	}
	return nil
}

func (c *Conductor) rowReady(row *dmlTask, watermark uint64) bool {
	if !c.dmlReady(row, watermark) {
		return false
	}
	for _, block := range c.pendingDDL {
		if row.dml.CommitTs > block.ddl.GetCommitTs() && ddl.BlocksTable(block.ddl, row.dml) {
			return false
		}
	}
	return true
}

func (c *Conductor) trackDMLWait(row *dmlTask, watermark uint64) {
	if c.dmlReady(row, watermark) {
		return
	}
	if c.controlTs == 0 || row.dml.CommitTs < c.controlTs {
		c.controlTs = row.dml.CommitTs
	}
}

func (c *Conductor) dmlReady(item *dmlTask, watermark uint64) bool {
	if item.dml.CommitTs <= watermark || c.control == nil || c.streams[item.stream].control {
		return true
	}
	if c.control.reached || item.dml.CommitTs <= *c.control.watermark {
		return true
	}
	// A later control in this row's scope proves its preceding DDLs are known.
	for _, block := range c.pendingDDL {
		if item.dml.CommitTs <= block.ddl.GetCommitTs() && ddl.BlocksTable(block.ddl, item.dml) {
			return true
		}
	}
	return false
}

func (c *Conductor) popDML(row *dmlTask) *dmlTask {
	groups := c.pendingDML[row.dml.PhysicalTableID]
	rows := groups[row.stream]
	rows[0] = nil
	if len(rows) > 1 {
		groups[row.stream] = rows[1:]
		return row
	}
	delete(groups, row.stream)
	if len(groups) == 0 {
		delete(c.pendingDML, row.dml.PhysicalTableID)
	}
	return row
}

func New(memory *budget.Budget, reader source.Reader) *Conductor {
	specs := reader.Streams()
	streams := make(map[int32]*streamWatermark, len(specs))
	for _, stream := range specs {
		streams[stream.ID] = &streamWatermark{control: stream.Control}
	}
	c := &Conductor{
		reader:      reader,
		confirmable: make(chan struct{}, 1),
		checkpoint:  new(atomic.Uint64),
		positions:   make(map[int32][]sourcePosition),
		memory:      memory,
		streams:     streams,
	}
	semantics := reader.Semantics()
	c.orderedDML = semantics.CommitOrdered
	c.reorderDDL = semantics.DDLsCanReorder
	if validator, ok := reader.(source.Validator); ok {
		c.validator = validator
	}
	if notifier, ok := reader.(source.SchemaNotifier); ok {
		c.notifier = notifier
	}
	for _, spec := range specs {
		if spec.Control {
			c.control = &readBoundary{
				reached:   spec.DDLBroadcast,
				watermark: &streams[spec.ID].watermark,
			}
			break
		}
	}
	return c
}

// Scope names identify the inputs that must cover a DDL.
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

func (c *Conductor) advanceReading(w *writer.Writer) {
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
		c.proven = nil
		c.requestBoundary([]event.SchemaTableName{}, 0)
		c.reader.ContinueReading(c.needsMoreInput(w))
		return
	}
	if c.boundary != nil && !c.boundary.reached && c.boundaryTs == through && c.boundaryDDL == pending {
		c.advanceBoundary(false, 0)
		c.reader.ContinueReading(c.needsMoreInput(w))
		return
	}
	// Streams that already crossed the DDL must not be retargeted for it.
	c.proven = c.proven[:0]
	if pending != nil {
		c.markProven(pending)
	}
	c.boundary = c.requestBoundary(tables, through)
	c.boundaryTs, c.boundaryDDL = through, nil
	if pending != nil && c.boundary != nil {
		pending.boundary = c.boundary
		c.boundaryDDL = pending
	}
	c.reader.ContinueReading(c.needsMoreInput(w))
}

// markProven records streams whose read target must not move past block: those
// that already delivered it and those with later rows still blocked by it.
func (c *Conductor) markProven(block *ddlTask) {
	if input := c.ddlInputs[ddl.KeyOf(block.ddl)]; input != nil {
		c.proven = append(c.proven, input.streams...)
	}
	for _, groups := range c.pendingDML {
		c.proven = append(c.proven, blockedStreams(groups, block.ddl)...)
	}
}

func blockedStreams(groups map[int32][]*dmlTask, block *event.DDLEvent) []int32 {
	var streams []int32
	for stream, rows := range groups {
		if rowsBlockedAfter(rows, block) {
			streams = append(streams, stream)
		}
	}
	return streams
}

func rowsBlockedAfter(rows []*dmlTask, block *event.DDLEvent) bool {
	index, _ := slices.BinarySearchFunc(rows, block.GetCommitTs(), compareDMLToDDL)
	for _, item := range rows[index:] {
		if ddl.BlocksTable(block, item.dml) {
			return true
		}
	}
	return false
}

func (c *Conductor) needsMoreInput(w *writer.Writer) bool {
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
	// Let decoding and writes release memory before reading solely to catch
	// up global progress. An empty conductor must still drain source buffers and
	// reach markers that release retained transport positions.
	return len(c.pendingDML) == 0 && len(c.pendingDDL) == 0 && w.Idle()
}

// Stream markers establish a global watermark when every input is covered.
// Sources can also publish a global boundary after proving complete input.
func (c *Conductor) collectWatermarks() {
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

// Input progress can pass decoded work still awaiting delivery. Register its
// earliest timestamps before letting the writer advance checkpoints.
func (c *Conductor) advanceWriter(w *writer.Writer) {
	pendingTables := make(map[int64]uint64, len(c.pendingDML))
	for table, groups := range c.pendingDML {
		pendingTables[table] = firstDML(groups).dml.CommitTs
	}
	var pendingDDL uint64
	for _, item := range c.pendingDDL {
		ts := item.ddl.GetCommitTs()
		if pendingDDL == 0 || ts < pendingDDL {
			pendingDDL = ts
		}
	}
	w.Advance(writer.Progress{
		Watermark:     c.watermark,
		PendingTables: pendingTables,
		PendingDDL:    pendingDDL,
	})
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

// Reclaim history only through the inclusive downstream checkpoint.
func (c *Conductor) Checkpoint() uint64 {
	return c.checkpoint.Load()
}

func (c *Conductor) cleanHistory() {
	checkpoint := c.checkpoint.Load()
	if checkpoint <= c.cleanedCheckpoint {
		return
	}
	c.cleanedCheckpoint = checkpoint
	for key := range c.ddlInputs {
		if key.CommitTs <= checkpoint {
			delete(c.ddlInputs, key)
		}
	}
}

type streamWatermark struct {
	control      bool
	watermark    uint64
	hasWatermark bool
}

func (c *Conductor) accept(work decode.Event) error {
	origin := work.Origin
	switch value := work.Event.(type) {
	case *event.DMLEvent:
		result := &dmlTask{dml: value, stream: origin.Stream, offset: origin.Offset}
		c.readThrough = max(c.readThrough, result.dml.CommitTs)
		// Source positions cover every event, not only markers. A Kafka or
		// Pulsar row record has no trailing resolved event, so its commit
		// timestamp is the only progress the source can confirm.
		c.inputMarker = max(c.inputMarker, result.dml.CommitTs)
		c.hasInputMarker = true
		c.inputRows++
		if c.validator != nil {
			c.validator.CheckInput(origin, c.inputRows)
		}
		c.queueDML(result)
		c.crossBoundary(result.stream, result.dml.CommitTs, nil, result)
	case *event.ResolvedEvent:
		ts := value.ResolvedTs
		c.inputMarker, c.hasInputMarker = max(c.inputMarker, ts), true
		c.readThrough = max(c.readThrough, ts)
		// A source without streams (storage) publishes a global checkpoint.
		// Partitioned sources must keep per-stream progress: a resolved event
		// on one partition says nothing about the others, so the global
		// watermark is the minimum over all streams.
		if len(c.streams) == 0 {
			c.watermark = max(c.watermark, ts)
			break
		}
		stream := c.streams[origin.Stream]
		stream.watermark = max(stream.watermark, ts)
		stream.hasWatermark = true
		c.advanceBoundary(false, 0)
	case *event.DDLEvent:
		block := value
		c.inputMarker, c.hasInputMarker = max(c.inputMarker, block.GetCommitTs()), true
		c.readThrough = max(c.readThrough, block.GetCommitTs())
		if err := setDDLTableNames(block); err != nil {
			return err
		}
		if c.notifier != nil {
			tables := []event.SchemaTableName{{SchemaName: block.SchemaName, TableName: block.TableName}}
			if block.Type == byte(model.ActionRenameTable) && len(block.BlockedTableNames) > 0 {
				tables = append(tables, block.BlockedTableNames[0])
			}
			defer c.notifier.MarkProcessed(tables, block.GetCommitTs())
		}
		defer c.crossBoundary(origin.Stream, block.GetCommitTs(), block, nil)
		replica := c.control != nil && !c.streams[origin.Stream].control
		var boundary *readBoundary
		if len(c.streams) == 0 || (c.control != nil && len(c.streams) == 1) {
			boundary = &readBoundary{reached: true}
		}
		key := ddl.KeyOf(block)
		input, exists := c.ddlInputs[key]
		if !exists {
			input = c.queueDDL(block)
			input.event.awaitingDDL = replica
			if replica {
				boundary = c.control
			}
			input.event.boundary = boundary
		}
		if input.event != nil && !slices.Contains(input.streams, origin.Stream) {
			input.streams = append(input.streams, origin.Stream)
		}
		if exists && !replica && input.event != nil && input.event.awaitingDDL {
			item := input.event
			item.awaitingDDL, item.ddl, item.boundary = false, block, boundary
			return nil
		}
	case nil:
		// A source position becomes confirmable only after all its events
		// have been decoded and registered.
		if origin.Ack != nil && c.hasInputMarker {
			c.positionsMu.Lock()
			c.memory.ForceAcquire(sourcePositionBytes)
			c.positions[origin.Stream] = append(c.positions[origin.Stream], sourcePosition{through: c.inputMarker, ack: origin.Ack})
			c.positionsMu.Unlock()
			notifyConfirmation(c.confirmable)
		}
		c.inputRows, c.inputMarker, c.hasInputMarker = 0, 0, false
	}
	return nil
}

func (c *Conductor) crossBoundary(stream int32, commitTs uint64, effect *event.DDLEvent, row *dmlTask) {
	block := c.boundaryDDL
	if block == nil || block.boundary.reached {
		return
	}
	crossed := effect != nil && effect.GetCommitTs() == block.ddl.GetCommitTs() && effect.SchemaName == block.ddl.SchemaName && effect.TableName == block.ddl.TableName && effect.Query == block.ddl.Query
	crossed = crossed || (row != nil && commitTs > block.ddl.GetCommitTs() && ddl.BlocksTable(block.ddl, row.dml))
	if crossed {
		c.proven = append(c.proven, stream)
		c.advanceBoundary(false, 0)
	}
}

// Request coverage without knowing which source inputs carry the table scope.
func (c *Conductor) requestBoundary(tables []event.SchemaTableName, commitTs uint64) *readBoundary {
	if c.control == nil {
		return nil
	}
	if commitTs == 0 {
		c.boundary = nil
		c.advanceBoundary(true, 0)
		return nil
	}
	c.boundary = &readBoundary{tables: tables}
	c.advanceBoundary(true, commitTs)
	return c.boundary
}

// Publish a snapshot of read priorities; reader never sees the shared boundary.
func (c *Conductor) advanceBoundary(retarget bool, target uint64) {
	watermarks := make([]source.StreamWatermark, 0, len(c.streams))
	for id, stream := range c.streams {
		watermarks = append(watermarks, source.StreamWatermark{Stream: id, Timestamp: stream.watermark})
	}
	var tables []event.SchemaTableName
	if c.boundary != nil {
		tables = c.boundary.tables
	}
	progress := source.Progress{
		Watermarks: watermarks,
		Scope:      tables,
		Target:     target,
		Retarget:   retarget,
		Proven:     c.proven,
	}
	c.proven = nil
	reached := c.reader.Advance(progress)
	if c.boundary != nil {
		c.boundary.reached = reached
	}
}

func notifyConfirmation(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
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
			if err := c.confirmPositions(ctx, checkpoint); err != nil {
				return err
			}
		}
	}
}

func (c *Conductor) confirmPositions(ctx context.Context, checkpoint uint64) error {
	c.positionsMu.Lock()
	positions := maps.Clone(c.positions)
	c.positionsMu.Unlock()
	for stream, pending := range positions {
		count := confirmablePrefix(pending, checkpoint)
		if count == 0 {
			continue
		}
		if err := pending[count-1].ack(ctx); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer input confirmation failed", zap.Error(err))
			}
			return err
		}
		c.dropPositions(stream, count)
	}
	return nil
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

func (c *Conductor) dropPositions(stream int32, count int) {
	c.positionsMu.Lock()
	current := c.positions[stream]
	clear(current[:count])
	c.positions[stream] = current[count:]
	c.positionsMu.Unlock()
	c.memory.Release(int64(count) * sourcePositionBytes)
}

// Equal timestamps precede the DDL; search for the first strictly later row.
func compareDMLToDDL(row *dmlTask, ts uint64) int {
	if row.dml.CommitTs <= ts {
		return -1
	}
	return 1
}
