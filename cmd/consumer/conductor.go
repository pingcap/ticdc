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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// pendingTask is private scheduling state. Only decodeTask has a conversion
// result; DDL and checkpoint tasks never carry row-conversion fields.
type pendingTask interface{ pendingTask() }

func (*decodeTask) pendingTask() {}
func (*ddlTask) pendingTask()    {}
func (*watermark) pendingTask()  {}

type ddlTask struct {
	ddl         *event.DDLEvent
	onFlush     func()
	boundary    *readBoundary
	awaitingDDL bool
}

type watermark struct {
	// All input through watermark has been read and registered. onFlush runs
	// only after the downstream checkpoint covers the same inclusive range.
	watermark uint64
	onFlush   func()
}

// One loop owns dependency selection, conversion collection and writer queues.
// Protocol parsing and sink execution run independently of that state.
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
		for task := c.next(); task != nil; task = c.next() {
			c.decoding = append(c.decoding, task)
		}
		c.deliver(c.collectDecoded(), w)
		c.updateWatermark(w)
		if err := w.flushDML(ctx); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DML submission failed", zap.Error(err))
			}
			return err
		}
		c.checkpoint.Store(w.checkpoint)
		c.cleanHistory()
		c.advanceReading()
	}
}

func (c *conductor) close() {
	if c.g != nil {
		_ = c.g.Wait()
	}
	c.decoder.close()
}

// Join conversions in table order without making independent tables wait.
func (c *conductor) collectDecoded() []pendingTask {
	var items []pendingTask
	var blockedRows []*decodeTask
	var blockedControls []pendingTask
	blockedTables := make(map[int64]bool)
	remaining := c.decoding[:0]
	for _, item := range c.decoding {
		ready := true
		row, isRow := item.(*decodeTask)
		if isRow {
			select {
			case <-row.done:
			default:
				ready = false
			}
			ready = ready && !blockedTables[row.tableID]
		}
		for _, prior := range blockedControls {
			switch prior := prior.(type) {
			case *ddlTask:
				ready = ready && isRow && !ddlBlocksTask(prior.ddl, row)
			case *watermark:
				if _, progress := item.(*watermark); progress {
					ready = false
				}
			}
		}
		switch item := item.(type) {
		case *ddlTask:
			for _, prior := range blockedRows {
				if ddlBlocksTask(item.ddl, prior) {
					ready = false
					break
				}
			}
		case *watermark:
			for _, prior := range blockedRows {
				if prior.commitTs <= item.watermark {
					ready = false
					break
				}
			}
		}
		if !ready {
			remaining = append(remaining, item)
			if isRow {
				blockedTables[row.tableID] = true
				blockedRows = append(blockedRows, row)
			} else {
				blockedControls = append(blockedControls, item)
			}
			continue
		}
		items = append(items, item)
	}
	clear(c.decoding[len(remaining):])
	c.decoding = remaining
	return items
}

func (c *conductor) deliver(items []pendingTask, w *writer) {
	var rows []*dmlWrite
	var bytes int64
	flush := func() {
		for _, row := range c.prepare(rows) {
			w.addDML(row)
		}
		rows = rows[:0]
		bytes = 0
	}
	for _, item := range items {
		switch item := item.(type) {
		case *decodeTask:
			item.dml.AddPostFlushFunc(item.record.release)
			rows = append(rows, &dmlWrite{dml: item.dml, bytes: item.bytes, sequential: c.mergeRows})
			bytes += item.bytes
			if len(rows) >= batchRows || bytes >= batchBytes {
				flush()
			}
		case *ddlTask:
			flush()
			w.addDDL(&ddlWrite{ddl: item.ddl, onFlush: item.onFlush})
		case *watermark:
			flush()
			w.addWatermark(item)
		}
	}
	flush()
}

func (c *conductor) prepare(items []*dmlWrite) []*dmlWrite {
	if !c.mergeRows {
		return items
	}
	if c.sortCSVRows {
		slices.SortStableFunc(items, func(x, y *dmlWrite) int {
			return cmp.Or(cmp.Compare(x.dml.CommitTs, y.dml.CommitTs), cmp.Compare(x.dml.RowTypes[0], y.dml.RowTypes[0]))
		})
	}
	for _, item := range items {
		item.sequential = true
	}
	prepared := items[:0]
	remaining := items
	for len(remaining) != 0 {
		item := remaining[0]
		// Merge only the bounded batch already available. Waiting for an entire
		// transaction could retain arbitrarily large input in memory.
		first := item.dml
		count, retained := int(first.Len()), item.bytes
		end := 1
		for end < len(remaining) {
			if count >= batchRows || retained >= batchBytes {
				break
			}
			next := remaining[end].dml
			if next.PhysicalTableID != first.PhysicalTableID || next.CommitTs != first.CommitTs || !sameTableSchema(first.TableInfo, next.TableInfo) {
				break
			}
			count += int(next.Len())
			retained += remaining[end].bytes
			end++
		}
		if end == 1 {
			prepared = append(prepared, item)
			remaining = remaining[1:]
			continue
		}
		var rows int
		for _, part := range remaining[:end] {
			rows += len(part.dml.RowTypes)
		}
		dml := event.NewDMLEvent(first.DispatcherID, first.PhysicalTableID, first.StartTs, first.CommitTs, first.TableInfo)
		dml.Version, dml.Seq, dml.Epoch = first.Version, first.Seq, first.Epoch
		dml.ReplicatingTs, dml.TableInfoVersion = first.ReplicatingTs, first.TableInfoVersion
		dml.Rows = chunk.NewChunkWithCapacity(first.TableInfo.GetFieldSlice(), rows)
		for _, source := range remaining[:end] {
			part := source.dml
			dml.Rows.Append(part.Rows, part.PreviousTotalOffset, part.PreviousTotalOffset+len(part.RowTypes))
			dml.RowTypes = append(dml.RowTypes, part.RowTypes...)
			if len(dml.RowKeys) != 0 || len(part.RowKeys) != 0 {
				if len(dml.RowKeys) == 0 {
					dml.RowKeys = make([][]byte, dml.Rows.NumRows()-len(part.RowTypes))
				}
				keys := part.RowKeys
				if len(keys) == 0 {
					keys = make([][]byte, len(part.RowTypes))
				}
				dml.RowKeys = append(dml.RowKeys, keys...)
			}
			if len(dml.Checksum) != 0 || len(part.Checksum) != 0 {
				if len(dml.Checksum) == 0 {
					dml.Checksum = make([]*integrity.Checksum, dml.Length)
				}
				checksums := part.Checksum
				if len(checksums) == 0 {
					checksums = make([]*integrity.Checksum, part.Length)
				}
				dml.Checksum = append(dml.Checksum, checksums...)
			}
			dml.Length += part.Length
			dml.ApproximateSize += part.ApproximateSize
			dml.AddPostEnqueueFunc(part.PostEnqueue)
			dml.AddPostFlushFunc(part.PostFlush)
		}
		bytes := dml.Rows.MemoryUsage()
		c.memory.reserve(bytes)
		prepared = append(prepared, &dmlWrite{
			dml:        dml,
			bytes:      retained + bytes,
			sequential: true,
		})
		remaining = remaining[end:]
	}
	clear(items[len(prepared):])
	return prepared
}

// conductor coordinates row conversion, write ordering and completion progress.
type conductor struct {
	reader            Reader
	controls          []*readControl
	ddlCopies         map[uint64][]*ack
	ddlInputs         map[ddlKey]*ddlInput
	boundary          *readBoundary
	boundaryTs        uint64
	boundaryDDL       *ddlTask
	checkpoint        atomic.Uint64
	cleanedCheckpoint uint64
	g                 *errgroup.Group
	memory            *memoryUsage
	streams           map[int32]*streamWatermark
	waitingMetadata   int
	decoder           *decoderGroup
	pendingWatermarks []*watermark
	watermark         uint64
	mergeRows         bool
	sortCSVRows       bool
	decoding          []pendingTask
	pendingDML        []*decodeTask
	pendingDDL        []*ddlTask
	controlTs         uint64
	scanIndex         int
	priorityIndex     int
	scanWatermark     uint64
	scanDDL           *ddlTask
	scanDDLReady      bool
	scanInvalid       bool
	scanBlocked       map[int64]uint64
	scanBoundaries    map[*readBoundary]uint64
	holes             int
	waitingWatermark  uint64
	waitingCount      int
	waitingKnown      bool
}

type ddlInput struct {
	event   *ddlTask // Nil once the conductor emits or retires the canonical effect.
	streams []int32  // Inputs that have crossed this exact DDL.
}

// queueDML takes the input lease transferred by the source parser.
func (c *conductor) queueDML(item *decodeTask) {
	if len(c.pendingDML) == 0 {
		c.scanInvalid = true
	}
	c.pendingDML = append(c.pendingDML, item)
	if c.waitingKnown && item.commitTs <= c.waitingWatermark {
		c.waitingCount++
	}
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

func (c *conductor) queueDDL(ddl *event.DDLEvent, record *ack) *ddlInput {
	c.scanInvalid = true
	if c.waitingKnown && ddl.GetCommitTs() <= c.waitingWatermark {
		c.waitingCount++
	}
	record.retain()
	c.pendingDDL = append(c.pendingDDL, &ddlTask{
		ddl:     ddl,
		onFlush: record.release,
	})
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
		event: c.pendingDDL[len(c.pendingDDL)-1],
	}
	c.ddlInputs[key] = input
	return input
}

func (c *conductor) nextReady(watermark uint64) pendingTask {
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
		head.onFlush()
		c.popDDL()
		c.scanInvalid = true
		head = nil
	}
	headReady := head != nil && ddlReady(head, watermark)
	c.prepareScan(watermark, head, headReady)
	if headReady {
		if result := c.drainDDL(head, watermark); result != nil {
			return result
		}
	}
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

func (c *conductor) prepareScan(watermark uint64, head *ddlTask, headReady bool) {
	reset := c.scanInvalid || c.scanWatermark != watermark || c.scanDDL != head || c.scanDDLReady != headReady || c.scanIndex > len(c.pendingDML)
	for boundary, before := range c.scanBoundaries {
		reset = reset || boundary.reached || (boundary.watermark != nil && *boundary.watermark != before)
	}
	c.scanWatermark, c.scanDDL, c.scanDDLReady = watermark, head, headReady
	if !reset {
		return
	}
	c.scanIndex, c.priorityIndex, c.scanBlocked = 0, 0, nil
	c.controlTs = 0
	clear(c.scanBoundaries)
	c.scanInvalid = false
}

// Drain the first preceding row in the DDL scope, or emit the DDL when no such
// row remains. Independent tables can still progress through nextDML.
func (c *conductor) drainDDL(head *ddlTask, watermark uint64) pendingTask {
	for index := c.priorityIndex; index < len(c.pendingDML); index++ {
		pending := c.pendingDML[index]
		if pending == nil || pending.commitTs > head.ddl.GetCommitTs() || !ddlBlocksTask(head.ddl, pending) {
			continue
		}
		c.priorityIndex = index
		if pending.boundary != nil && !pending.boundary.reached {
			if c.scanBoundaries == nil {
				c.scanBoundaries = make(map[*readBoundary]uint64)
			}
			var through uint64
			if pending.boundary.watermark != nil {
				through = *pending.boundary.watermark
			}
			c.scanBoundaries[pending.boundary] = through
		}
		if c.dmlReady(pending, watermark) && !slices.ContainsFunc(c.pendingDDL, func(ddl *ddlTask) bool {
			return pending.commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, pending)
		}) {
			c.priorityIndex = max(c.priorityIndex, index+1)
			return c.popDML(index)
		}
		return nil
	}
	return c.popDDL()
}

func (c *conductor) popDDL() *ddlTask {
	item := c.pendingDDL[0]
	if c.waitingKnown && item.ddl.GetCommitTs() <= c.waitingWatermark {
		c.waitingCount--
	}
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

func (c *conductor) nextDML(watermark uint64) *decodeTask {
	for c.scanIndex < len(c.pendingDML) {
		index := c.scanIndex
		c.scanIndex++
		result := c.pendingDML[index]
		if result == nil {
			continue
		}
		tableID := result.tableID
		if ts, ok := c.scanBlocked[tableID]; ok && ts <= result.commitTs {
			continue
		}
		// Every queued DDL fences its own post-DDL rows.
		known := c.dmlReady(result, watermark)
		ready := known && !slices.ContainsFunc(c.pendingDDL, func(ddl *ddlTask) bool {
			return result.commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, result)
		})
		if ready {
			return c.popDML(index)
		}
		// A later row must not overtake an earlier row waiting for its proof.
		if result.boundary != nil && !result.boundary.reached {
			if c.scanBoundaries == nil {
				c.scanBoundaries = make(map[*readBoundary]uint64)
			}
			var through uint64
			if result.boundary.watermark != nil {
				through = *result.boundary.watermark
			}
			c.scanBoundaries[result.boundary] = through
		}
		if !known && (result.boundary == nil || result.boundary.watermark != nil) {
			if c.controlTs == 0 || result.commitTs < c.controlTs {
				c.controlTs = result.commitTs
			}
		}
		if c.scanBlocked == nil {
			c.scanBlocked = make(map[int64]uint64)
		}
		if ts, ok := c.scanBlocked[tableID]; !ok || result.commitTs < ts {
			c.scanBlocked[tableID] = result.commitTs
		}
	}
	return nil
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

func (c *conductor) popDML(index int) *decodeTask {
	result := c.pendingDML[index]
	if c.waitingKnown && result.commitTs <= c.waitingWatermark {
		c.waitingCount--
	}
	c.pendingDML[index] = nil
	c.holes++
	for len(c.pendingDML) != 0 && c.pendingDML[0] == nil {
		c.pendingDML = c.pendingDML[1:]
		c.scanIndex = max(0, c.scanIndex-1)
		c.priorityIndex = max(0, c.priorityIndex-1)
		c.holes--
	}
	if c.holes <= 256 || c.holes*2 <= len(c.pendingDML) {
		return result
	}
	c.pendingDML = slices.DeleteFunc(c.pendingDML, func(item *decodeTask) bool {
		return item == nil
	})
	c.holes, c.scanInvalid = 0, true
	return result
}

func (c *conductor) hasPendingThrough(watermark uint64) bool {
	if !c.waitingKnown || c.waitingWatermark != watermark {
		c.waitingWatermark, c.waitingCount, c.waitingKnown = watermark, 0, true
		for _, pending := range c.pendingDML {
			if pending != nil && pending.commitTs <= watermark {
				c.waitingCount++
			}
		}
		for _, pending := range c.pendingDDL {
			if pending.ddl.GetCommitTs() <= watermark {
				c.waitingCount++
			}
		}
	}
	return c.waitingCount != 0
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

func newConductor(memory *memoryUsage, reader Reader, parsing *inputParser) *conductor {
	streams := make(map[int32]*streamWatermark, len(parsing.streams))
	for id := range parsing.streams {
		streams[id] = &streamWatermark{}
	}
	c := &conductor{
		reader: reader,
		memory: memory, streams: streams,
		decoder:     newDecoderGroup(memory, make(chan struct{}, 1)),
		mergeRows:   parsing.codecConfig.Protocol == config.ProtocolCsv,
		sortCSVRows: parsing.codecConfig.Protocol == config.ProtocolCsv && parsing.codecConfig.OutputOldValue && parsing.codecConfig.IncludeCommitTs,
	}
	parsing.checkpoint = &c.checkpoint
	return c
}

func (c *conductor) next() pendingTask {
	if result := c.nextReady(c.watermark); result != nil {
		return result
	}
	if len(c.pendingWatermarks) == 0 || c.hasPendingThrough(c.pendingWatermarks[0].watermark) {
		return nil
	}
	result := c.pendingWatermarks[0]
	c.pendingWatermarks[0] = nil
	c.pendingWatermarks = c.pendingWatermarks[1:]
	return result
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

func (c *conductor) advanceReading() {
	if c.waitingMetadata != 0 {
		c.boundary, c.boundaryDDL = nil, nil
		c.reader.Boundary([]event.SchemaTableName{}, 0)
		c.reader.ContinueReading(true)
		return
	}
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
	if through == 0 {
		// Whole-input progress only drives reclamation and input confirmation.
		// Its lag must not fence already safe DML from independent streams.
		tables = nil
		for _, control := range c.controls {
			if through == 0 || control.watermark < through {
				through = control.watermark
			}
		}
		for ts := range c.ddlCopies {
			if through == 0 || ts < through {
				through = ts
			}
		}
	}
	if through == 0 {
		c.boundary, c.boundaryDDL = nil, nil
		c.reader.Boundary([]event.SchemaTableName{}, 0)
		c.reader.ContinueReading(c.needsMoreInput())
		return
	}
	if c.boundary != nil && !c.boundary.reached && c.boundaryTs == through && c.boundaryDDL == pending {
		c.reader.ContinueReading(true)
		return
	}
	c.boundary = c.reader.Boundary(tables, through)
	c.boundaryTs, c.boundaryDDL = through, nil
	if pending != nil && c.boundary != nil {
		pending.boundary = c.boundary
		c.boundaryDDL = pending
		c.scanInvalid = true
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
		for _, item := range c.pendingDML {
			if item == nil || item.commitTs <= pending.ddl.GetCommitTs() || !ddlBlocksTask(pending.ddl, item) {
				continue
			}
			updates = append(updates, readUpdate{
				stream:   item.stream,
				boundary: c.boundary,
			})
		}
		c.reader.Advance(updates)
	}
	c.reader.ContinueReading(c.needsMoreInput())
}

func (c *conductor) needsMoreInput() bool {
	if c.boundary != nil && !c.boundary.reached {
		return true
	}
	if len(c.controls) != 0 || len(c.ddlCopies) != 0 || c.controlTs != 0 {
		return true
	}
	for boundary := range c.scanBoundaries {
		if !boundary.reached {
			return true
		}
	}
	for _, item := range c.pendingDDL {
		if item.ddl.GetCommitTs() > c.watermark && !ddlReady(item, c.watermark) {
			return true
		}
	}
	if c.waitingMetadata != 0 {
		return true
	}
	return false
}

// Kafka and Pulsar provide one read watermark per physical partition. A global
// watermark needs every partition; missing progress holds the previous boundary.
func (c *conductor) collectWatermarks() {
	watermark := uint64(math.MaxUint64)
	for _, stream := range c.streams {
		if !stream.hasWatermark {
			return
		}
		watermark = min(watermark, stream.watermark)
	}
	if len(c.streams) != 0 {
		c.watermark = max(c.watermark, watermark)
	}
	var records []*ack
	c.controls = slices.DeleteFunc(c.controls, func(control *readControl) bool {
		if control.watermark > c.watermark {
			return false
		}
		records = append(records, control.records...)
		return true
	})
	for ts, copies := range c.ddlCopies {
		if ts <= c.watermark {
			records = append(records, copies...)
			delete(c.ddlCopies, ts)
		}
	}
	if len(records) == 0 {
		return
	}
	c.addWatermark(c.watermark, records)
}

func (c *conductor) addWatermark(through uint64, records []*ack) {
	c.pendingWatermarks = append(c.pendingWatermarks, &watermark{
		watermark: through,
		onFlush: func() {
			for _, record := range records {
				record.release()
			}
		},
	})
}

// Input progress can pass conversions still in flight. Register their earliest
// timestamps before letting the writer advance global or table checkpoints.
func (c *conductor) updateWatermark(w *writer) {
	w.watermark = max(w.watermark, c.watermark)
	w.unsubmitted, w.hasUnsubmitted = math.MaxUint64, false
	clear(w.unsubmittedTables)
	track := func(ts uint64) {
		w.unsubmitted, w.hasUnsubmitted = min(w.unsubmitted, ts), true
	}
	trackDML := func(row *decodeTask) {
		track(row.commitTs)
		if w.unsubmittedTables == nil {
			w.unsubmittedTables = make(map[int64]uint64)
		}
		if ts, ok := w.unsubmittedTables[row.tableID]; !ok || row.commitTs < ts {
			w.unsubmittedTables[row.tableID] = row.commitTs
		}
	}
	for _, row := range c.pendingDML {
		if row != nil {
			trackDML(row)
		}
	}
	for _, ddl := range c.pendingDDL {
		track(ddl.ddl.GetCommitTs())
	}
	for _, work := range c.decoding {
		switch work := work.(type) {
		case *decodeTask:
			trackDML(work)
		case *ddlTask:
			track(work.ddl.GetCommitTs())
		}
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
		if result.table != nil && result.commitTs > 0 && result.commitTs <= c.checkpoint.Load() {
			result.record.release()
			return nil
		}
		c.queueDML(result)
		if result.waiting {
			c.waitingMetadata++
		} else if err := c.decoder.submit(ctx, result); err != nil {
			return err
		}
		c.crossBoundary(result.stream, result.commitTs, nil, result)
	case *resumeTask:
		c.waitingMetadata--
		if err := c.decoder.submit(ctx, result.task); err != nil {
			return err
		}
		result.task.record.release()
	case *watermark:
		c.watermark = max(c.watermark, result.watermark)
		c.addWatermark(result.watermark, nil)
	case *inputWatermark:
		defer result.record.release()
		stream := c.streams[result.stream]
		stream.watermark = max(stream.watermark, result.watermark)
		stream.hasWatermark = true
		result.record.retain()
		c.controls = append(c.controls, &readControl{watermark: result.watermark, records: []*ack{result.record}})
		c.reader.Advance([]readUpdate{{stream: result.stream, watermark: result.watermark}})
	case *inputDDL:
		defer result.scope.record.release()
		if err := setDDLTableNames(result.ddl); err != nil {
			return err
		}
		defer func() {
			if result.registered != nil {
				result.registered()
			}
		}()
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
			input = c.queueDDL(ddl, result.scope.record)
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
			onFlush := item.onFlush
			result.scope.record.retain()
			item.onFlush = func() {
				onFlush()
				result.scope.record.release()
			}
			c.scanInvalid = true
			return nil
		}
		if exists {
			result.scope.record.retain()
			if c.ddlCopies == nil {
				c.ddlCopies = make(map[uint64][]*ack)
			}
			c.ddlCopies[ddl.GetCommitTs()] = append(c.ddlCopies[ddl.GetCommitTs()], result.scope.record)
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
