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

// assemblyTask is private scheduling state. Only decodeTask has a conversion
// result; DDL and checkpoint tasks never carry row-conversion fields.
type assemblyTask interface{ assemblyTask() }

func (*decodeTask) assemblyTask() {}
func (*ddlTask) assemblyTask()    {}
func (*checkpoint) assemblyTask() {}

type ddlTask struct {
	ddl         *event.DDLEvent
	onFlush     func()
	boundary    *readBoundary
	awaitingDDL bool
}

type checkpoint struct {
	watermark uint64
	onFlush   func()
}

// One loop owns dependency selection, conversion collection and writer queues.
// Protocol parsing and sink execution run independently of that state.
func (a *assembler) start(ctx context.Context, inputs <-chan []assemblyInput, w *writer) error {
	a.g, ctx = errgroup.WithContext(ctx)
	a.g.Go(func() error { return a.decoder.start(ctx) })
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case batch, ok := <-inputs:
			if !ok {
				inputs = nil
				continue
			}
			for _, input := range batch {
				if err := a.accept(ctx, input); err != nil {
					if ctx.Err() == nil {
						log.Error("consumer event assembly failed", zap.Error(err))
					}
					return err
				}
			}
			a.collectWatermarks()
		case <-a.decoder.completed:
		case <-w.completed:
		case <-a.historyChanged:
		}
		for task := a.next(); task != nil; task = a.next() {
			a.decoding = append(a.decoding, task)
		}
		a.deliver(a.collectDecoded(), w)
		if err := w.flushDML(ctx); err != nil {
			if ctx.Err() == nil {
				log.Error("consumer DML submission failed", zap.Error(err))
			}
			return err
		}
		a.cleanHistory()
		a.advanceReading()
	}
}

func (a *assembler) close() {
	if a.g != nil {
		_ = a.g.Wait()
	}
	a.decoder.close()
}

// Join conversions in table order without making independent tables wait.
func (a *assembler) collectDecoded() []assemblyTask {
	var items []assemblyTask
	var blockedRows []*decodeTask
	var blockedControls []assemblyTask
	blockedTables := make(map[int64]bool)
	remaining := a.decoding[:0]
	for _, item := range a.decoding {
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
			case *checkpoint:
				if _, progress := item.(*checkpoint); progress {
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
		case *checkpoint:
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
	clear(a.decoding[len(remaining):])
	a.decoding = remaining
	return items
}

func (a *assembler) deliver(items []assemblyTask, w *writer) {
	var rows []*dmlWrite
	var bytes int64
	flush := func() {
		for _, row := range a.prepare(rows) {
			w.enqueueDML(row)
		}
		rows = rows[:0]
		bytes = 0
	}
	for _, item := range items {
		switch item := item.(type) {
		case *decodeTask:
			item.dml.AddPostFlushFunc(item.record.release)
			rows = append(rows, &dmlWrite{dml: item.dml, bytes: item.bytes, sequential: a.mergeRows})
			bytes += item.bytes
			if len(rows) >= batchRows || bytes >= batchBytes {
				flush()
			}
		case *ddlTask:
			flush()
			w.enqueueDDL(&ddlWrite{ddl: item.ddl, onFlush: item.onFlush})
		case *checkpoint:
			flush()
			w.enqueueWatermark(item)
		}
	}
	flush()
}

func (a *assembler) prepare(items []*dmlWrite) []*dmlWrite {
	if !a.mergeRows {
		return items
	}
	if a.sortCSVRows {
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
		a.memory.reserve(bytes)
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

// assembler decodes logical inputs and arranges events for downstream writes.
type assembler struct {
	reader             Reader
	controls           []*readControl
	ddlCopies          map[uint64][]*ack
	ddlInputs          map[ddlKey]*ddlInput
	boundary           *readBoundary
	boundaryTs         uint64
	boundaryDDL        *ddlTask
	completedWatermark atomic.Uint64
	cleanedWatermark   uint64
	historyChanged     chan struct{}
	g                  *errgroup.Group
	memory             *memoryUsage
	streams            map[int32]*streamProgress
	waitingMetadata    int
	decoder            *decoderGroup
	pendingWatermarks  []*checkpoint
	watermark          uint64
	mergeRows          bool
	sortCSVRows        bool
	decoding           []assemblyTask
	pendingDML         []*decodeTask
	pendingDDL         []*ddlTask
	controlTs          uint64
	scanIndex          int
	priorityIndex      int
	scanWatermark      uint64
	scanDDL            *ddlTask
	scanDDLReady       bool
	scanInvalid        bool
	scanBlocked        map[int64]uint64
	scanBoundaries     map[*readBoundary]uint64
	holes              int
	waitingWatermark   uint64
	waitingCount       int
	waitingKnown       bool
}

type ddlInput struct {
	event   *ddlTask // Nil once assembly emits or retires the canonical effect.
	streams []int32  // Inputs that have crossed this exact DDL.
}

// queueDML takes the input lease transferred by the source parser.
func (a *assembler) queueDML(item *decodeTask) {
	if len(a.pendingDML) == 0 {
		a.scanInvalid = true
	}
	a.pendingDML = append(a.pendingDML, item)
	if a.waitingKnown && item.commitTs <= a.waitingWatermark {
		a.waitingCount++
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

func (a *assembler) queueDDL(ddl *event.DDLEvent, record *ack) *ddlInput {
	a.scanInvalid = true
	if a.waitingKnown && ddl.GetCommitTs() <= a.waitingWatermark {
		a.waitingCount++
	}
	record.retain()
	a.pendingDDL = append(a.pendingDDL, &ddlTask{
		ddl:     ddl,
		onFlush: record.release,
	})
	if a.ddlInputs == nil {
		a.ddlInputs = make(map[ddlKey]*ddlInput)
	}
	key := ddlKey{
		commitTs: ddl.GetCommitTs(),
		schema:   ddl.SchemaName,
		table:    ddl.TableName,
		query:    ddl.Query,
	}
	input := &ddlInput{
		event: a.pendingDDL[len(a.pendingDDL)-1],
	}
	a.ddlInputs[key] = input
	return input
}

func (a *assembler) nextReady(watermark uint64) assemblyTask {
	var head *ddlTask
	for len(a.pendingDDL) != 0 {
		head = a.pendingDDL[0]
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
		a.popDDL()
		a.scanInvalid = true
		head = nil
	}
	headReady := head != nil && ddlReady(head, watermark)
	a.prepareScan(watermark, head, headReady)
	if headReady {
		if result := a.drainDDL(head, watermark); result != nil {
			return result
		}
	}
	if row := a.nextDML(watermark); row != nil {
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

func (a *assembler) prepareScan(watermark uint64, head *ddlTask, headReady bool) {
	reset := a.scanInvalid || a.scanWatermark != watermark || a.scanDDL != head || a.scanDDLReady != headReady || a.scanIndex > len(a.pendingDML)
	for boundary, before := range a.scanBoundaries {
		reset = reset || boundary.reached || (boundary.watermark != nil && *boundary.watermark != before)
	}
	a.scanWatermark, a.scanDDL, a.scanDDLReady = watermark, head, headReady
	if !reset {
		return
	}
	a.scanIndex, a.priorityIndex, a.scanBlocked = 0, 0, nil
	a.controlTs = 0
	clear(a.scanBoundaries)
	a.scanInvalid = false
}

// Drain the first preceding row in the DDL scope, or emit the DDL when no such
// row remains. Independent tables can still progress through nextDML.
func (a *assembler) drainDDL(head *ddlTask, watermark uint64) assemblyTask {
	for index := a.priorityIndex; index < len(a.pendingDML); index++ {
		pending := a.pendingDML[index]
		if pending == nil || pending.commitTs > head.ddl.GetCommitTs() || !ddlBlocksTask(head.ddl, pending) {
			continue
		}
		a.priorityIndex = index
		if pending.boundary != nil && !pending.boundary.reached {
			if a.scanBoundaries == nil {
				a.scanBoundaries = make(map[*readBoundary]uint64)
			}
			var through uint64
			if pending.boundary.watermark != nil {
				through = *pending.boundary.watermark
			}
			a.scanBoundaries[pending.boundary] = through
		}
		if a.dmlReady(pending, watermark) && !slices.ContainsFunc(a.pendingDDL, func(ddl *ddlTask) bool {
			return pending.commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, pending)
		}) {
			a.priorityIndex = max(a.priorityIndex, index+1)
			return a.popDML(index)
		}
		return nil
	}
	return a.popDDL()
}

func (a *assembler) popDDL() *ddlTask {
	item := a.pendingDDL[0]
	if a.waitingKnown && item.ddl.GetCommitTs() <= a.waitingWatermark {
		a.waitingCount--
	}
	a.pendingDDL[0] = nil
	a.pendingDDL = a.pendingDDL[1:]
	key := ddlKey{
		commitTs: item.ddl.GetCommitTs(),
		schema:   item.ddl.SchemaName,
		table:    item.ddl.TableName,
		query:    item.ddl.Query,
	}
	if input, exists := a.ddlInputs[key]; exists && input.event == item {
		input.event, input.streams = nil, nil
	}
	return item
}

func (a *assembler) nextDML(watermark uint64) *decodeTask {
	for a.scanIndex < len(a.pendingDML) {
		index := a.scanIndex
		a.scanIndex++
		result := a.pendingDML[index]
		if result == nil {
			continue
		}
		tableID := result.tableID
		if ts, ok := a.scanBlocked[tableID]; ok && ts <= result.commitTs {
			continue
		}
		// Every queued DDL fences its own post-DDL rows.
		known := a.dmlReady(result, watermark)
		ready := known && !slices.ContainsFunc(a.pendingDDL, func(ddl *ddlTask) bool {
			return result.commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, result)
		})
		if ready {
			return a.popDML(index)
		}
		// A later row must not overtake an earlier row waiting for its proof.
		if result.boundary != nil && !result.boundary.reached {
			if a.scanBoundaries == nil {
				a.scanBoundaries = make(map[*readBoundary]uint64)
			}
			var through uint64
			if result.boundary.watermark != nil {
				through = *result.boundary.watermark
			}
			a.scanBoundaries[result.boundary] = through
		}
		if !known && (result.boundary == nil || result.boundary.watermark != nil) {
			if a.controlTs == 0 || result.commitTs < a.controlTs {
				a.controlTs = result.commitTs
			}
		}
		if a.scanBlocked == nil {
			a.scanBlocked = make(map[int64]uint64)
		}
		if ts, ok := a.scanBlocked[tableID]; !ok || result.commitTs < ts {
			a.scanBlocked[tableID] = result.commitTs
		}
	}
	return nil
}

func (a *assembler) dmlReady(item *decodeTask, watermark uint64) bool {
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
	return slices.ContainsFunc(a.pendingDDL, func(ddl *ddlTask) bool {
		return item.commitTs <= ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, item)
	})
}

func (a *assembler) popDML(index int) *decodeTask {
	result := a.pendingDML[index]
	if a.waitingKnown && result.commitTs <= a.waitingWatermark {
		a.waitingCount--
	}
	a.pendingDML[index] = nil
	a.holes++
	for len(a.pendingDML) != 0 && a.pendingDML[0] == nil {
		a.pendingDML = a.pendingDML[1:]
		a.scanIndex = max(0, a.scanIndex-1)
		a.priorityIndex = max(0, a.priorityIndex-1)
		a.holes--
	}
	if a.holes <= 256 || a.holes*2 <= len(a.pendingDML) {
		return result
	}
	a.pendingDML = slices.DeleteFunc(a.pendingDML, func(item *decodeTask) bool {
		return item == nil
	})
	a.holes, a.scanInvalid = 0, true
	return result
}

func (a *assembler) hasPendingThrough(watermark uint64) bool {
	if !a.waitingKnown || a.waitingWatermark != watermark {
		a.waitingWatermark, a.waitingCount, a.waitingKnown = watermark, 0, true
		for _, pending := range a.pendingDML {
			if pending != nil && pending.commitTs <= watermark {
				a.waitingCount++
			}
		}
		for _, pending := range a.pendingDDL {
			if pending.ddl.GetCommitTs() <= watermark {
				a.waitingCount++
			}
		}
	}
	return a.waitingCount != 0
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

func newAssembler(memory *memoryUsage, reader Reader, parsing *inputParser) *assembler {
	streams := make(map[int32]*streamProgress, len(parsing.streams))
	for id := range parsing.streams {
		streams[id] = &streamProgress{}
	}
	a := &assembler{
		reader: reader, historyChanged: make(chan struct{}, 1),
		memory: memory, streams: streams,
		decoder:     newDecoderGroup(memory, make(chan struct{}, 1)),
		mergeRows:   parsing.codecConfig.Protocol == config.ProtocolCsv,
		sortCSVRows: parsing.codecConfig.Protocol == config.ProtocolCsv && parsing.codecConfig.OutputOldValue && parsing.codecConfig.IncludeCommitTs,
	}
	parsing.completedWatermark = &a.completedWatermark
	return a
}

func (a *assembler) next() assemblyTask {
	if result := a.nextReady(a.watermark); result != nil {
		return result
	}
	if len(a.pendingWatermarks) == 0 || a.hasPendingThrough(a.pendingWatermarks[0].watermark) {
		return nil
	}
	result := a.pendingWatermarks[0]
	a.pendingWatermarks[0] = nil
	a.pendingWatermarks = a.pendingWatermarks[1:]
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

func (a *assembler) advanceReading() {
	if a.waitingMetadata != 0 {
		a.boundary, a.boundaryDDL = nil, nil
		a.reader.Boundary([]event.SchemaTableName{}, 0)
		a.reader.ContinueReading(true)
		return
	}
	var (
		pending *ddlTask
		through uint64
		tables  = []event.SchemaTableName{}
	)
	if len(a.pendingDDL) != 0 {
		head := a.pendingDDL[0]
		if head.awaitingDDL {
			through = head.ddl.GetCommitTs()
		}
		if !head.awaitingDDL && !ddlReady(head, a.watermark) {
			pending, through, tables = head, head.ddl.GetCommitTs(), ddlScope(head.ddl)
		}
	}
	if through == 0 {
		through = a.controlTs
	}
	if through == 0 {
		// Whole-input progress only drives reclamation and input confirmation.
		// Its lag must not fence already safe DML from independent streams.
		tables = nil
		for _, control := range a.controls {
			if through == 0 || control.watermark < through {
				through = control.watermark
			}
		}
		for ts := range a.ddlCopies {
			if through == 0 || ts < through {
				through = ts
			}
		}
	}
	if through == 0 {
		a.boundary, a.boundaryDDL = nil, nil
		a.reader.Boundary([]event.SchemaTableName{}, 0)
		a.reader.ContinueReading(a.needsMoreInput())
		return
	}
	if a.boundary != nil && !a.boundary.reached && a.boundaryTs == through && a.boundaryDDL == pending {
		a.reader.ContinueReading(true)
		return
	}
	a.boundary = a.reader.Boundary(tables, through)
	a.boundaryTs, a.boundaryDDL = through, nil
	if pending != nil && a.boundary != nil {
		pending.boundary = a.boundary
		a.boundaryDDL = pending
		a.scanInvalid = true
		key := ddlKey{
			commitTs: pending.ddl.GetCommitTs(),
			schema:   pending.ddl.SchemaName,
			table:    pending.ddl.TableName,
			query:    pending.ddl.Query,
		}
		var updates []readUpdate
		for _, stream := range a.ddlInputs[key].streams {
			updates = append(updates, readUpdate{
				stream:   stream,
				boundary: a.boundary,
			})
		}
		for _, item := range a.pendingDML {
			if item == nil || item.commitTs <= pending.ddl.GetCommitTs() || !ddlBlocksTask(pending.ddl, item) {
				continue
			}
			updates = append(updates, readUpdate{
				stream:   item.stream,
				boundary: a.boundary,
			})
		}
		a.reader.Advance(updates)
	}
	a.reader.ContinueReading(a.needsMoreInput())
}

func (a *assembler) needsMoreInput() bool {
	if a.boundary != nil && !a.boundary.reached {
		return true
	}
	if len(a.controls) != 0 || len(a.ddlCopies) != 0 || a.controlTs != 0 {
		return true
	}
	for boundary := range a.scanBoundaries {
		if !boundary.reached {
			return true
		}
	}
	for _, item := range a.pendingDDL {
		if item.ddl.GetCommitTs() > a.watermark && !ddlReady(item, a.watermark) {
			return true
		}
	}
	if a.waitingMetadata != 0 {
		return true
	}
	return false
}

// The input contract groups control progress by decoding stream. Kafka provides
// one stream per partition; topic-wide controls use a single merged stream.
func (a *assembler) collectWatermarks() {
	watermark := uint64(math.MaxUint64)
	for _, stream := range a.streams {
		if !stream.hasWatermark {
			return
		}
		watermark = min(watermark, stream.watermark)
	}
	if len(a.streams) != 0 {
		a.watermark = max(a.watermark, watermark)
	}
	var records []*ack
	a.controls = slices.DeleteFunc(a.controls, func(control *readControl) bool {
		if control.watermark > a.watermark {
			return false
		}
		records = append(records, control.records...)
		return true
	})
	for ts, copies := range a.ddlCopies {
		if ts <= a.watermark {
			records = append(records, copies...)
			delete(a.ddlCopies, ts)
		}
	}
	if len(records) == 0 {
		return
	}
	a.queueWatermark(a.watermark, records)
}

func (a *assembler) queueWatermark(watermark uint64, records []*ack) {
	a.pendingWatermarks = append(a.pendingWatermarks, &checkpoint{
		watermark: watermark,
		onFlush: func() {
			for _, record := range records {
				record.release()
			}
			for completed := a.completedWatermark.Load(); completed < watermark; completed = a.completedWatermark.Load() {
				if a.completedWatermark.CAS(completed, watermark) {
					break
				}
			}
			select {
			case a.historyChanged <- struct{}{}:
			default:
			}
		},
	})
}

// Input watermarks establish history boundaries. Downstream completion delays
// reclamation until earlier effects finish; current schemas remain available.
func (a *assembler) cleanHistory() {
	watermark := a.completedWatermark.Load()
	if watermark <= a.cleanedWatermark {
		return
	}
	a.cleanedWatermark = watermark
	for key := range a.ddlInputs {
		if key.commitTs < watermark {
			delete(a.ddlInputs, key)
		}
	}
}

type streamProgress struct {
	watermark    uint64
	hasWatermark bool
}

func (a *assembler) accept(ctx context.Context, input assemblyInput) error {
	switch result := input.(type) {
	case *decodeTask:
		if result.table != nil && result.commitTs < a.watermark {
			result.record.release()
			return nil
		}
		a.queueDML(result)
		if result.waiting {
			a.waitingMetadata++
		} else if err := a.decoder.submit(ctx, result); err != nil {
			return err
		}
		a.crossBoundary(result.stream, result.commitTs, nil, result)
	case *resumeTask:
		a.waitingMetadata--
		if err := a.decoder.submit(ctx, result.task); err != nil {
			return err
		}
		result.task.record.release()
	case *checkpoint:
		a.watermark = max(a.watermark, result.watermark)
		a.queueWatermark(result.watermark, nil)
	case *inputProgress:
		defer result.record.release()
		stream := a.streams[result.stream]
		stream.watermark = max(stream.watermark, result.watermark)
		stream.hasWatermark = true
		result.record.retain()
		a.controls = append(a.controls, &readControl{watermark: result.watermark, records: []*ack{result.record}})
		a.reader.Advance([]readUpdate{{stream: result.stream, progress: result.watermark}})
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
		defer a.crossBoundary(result.scope.stream, result.ddl.GetCommitTs(), result.ddl, nil)
		ddl := result.ddl
		key := ddlKey{
			commitTs: ddl.GetCommitTs(),
			schema:   ddl.SchemaName,
			table:    ddl.TableName,
			query:    ddl.Query,
		}
		input, exists := a.ddlInputs[key]
		if !exists {
			input = a.queueDDL(ddl, result.scope.record)
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
			// Canonical input arrives before its effect can leave assembly.
			item := input.event
			item.awaitingDDL, item.ddl, item.boundary = false, ddl, result.scope.ddlBoundary
			onFlush := item.onFlush
			result.scope.record.retain()
			item.onFlush = func() {
				onFlush()
				result.scope.record.release()
			}
			a.scanInvalid = true
			return nil
		}
		if exists {
			result.scope.record.retain()
			if a.ddlCopies == nil {
				a.ddlCopies = make(map[uint64][]*ack)
			}
			a.ddlCopies[ddl.GetCommitTs()] = append(a.ddlCopies[ddl.GetCommitTs()], result.scope.record)
			return nil
		}
		if result.scope.ddlOrder == commitOrder {
			slices.SortStableFunc(a.pendingDDL, func(first, second *ddlTask) int {
				return cmp.Compare(first.ddl.GetCommitTs(), second.ddl.GetCommitTs())
			})
		}
	}
	return nil
}

func (a *assembler) crossBoundary(stream int32, commitTs uint64, effect *event.DDLEvent, row *decodeTask) {
	ddl := a.boundaryDDL
	if ddl == nil || ddl.boundary.reached {
		return
	}
	crossed := effect != nil && effect.GetCommitTs() == ddl.ddl.GetCommitTs() && effect.SchemaName == ddl.ddl.SchemaName && effect.TableName == ddl.ddl.TableName && effect.Query == ddl.ddl.Query
	crossed = crossed || (row != nil && commitTs > ddl.ddl.GetCommitTs() && ddlBlocksTask(ddl.ddl, row))
	if crossed {
		a.reader.Advance([]readUpdate{{stream: stream, boundary: ddl.boundary}})
	}
}
