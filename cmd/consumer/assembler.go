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
	"database/sql"
	"iter"
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	"github.com/pingcap/ticdc/pkg/sink/codec/simple"
	putil "github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func (a *assembler) start(ctx context.Context, requests chan<- readRequest, inputs <-chan readResult, output chan<- []*writeEvent) error {
	a.g, ctx = errgroup.WithContext(ctx)
	ready := make(chan *writeEvent, 64)
	a.g.Go(func() error {
		return a.decoder.start(ctx)
	})
	a.g.Go(func() error {
		defer close(ready)
		err := a.arrange(ctx, requests, inputs, ready)
		if err != nil && ctx.Err() == nil {
			log.Error("consumer event assembly failed", zap.Error(err))
		}
		return err
	})
	defer close(output)
	for {
		var (
			items []*writeEvent
			err   error
		)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result, ok := <-ready:
			if !ok {
				return a.g.Wait()
			}
			items, err = a.assemble(ctx, ready, result)
		case <-a.decoder.completed:
			items = a.collectDecoded()
		}
		if err != nil {
			return err
		}
		if len(items) == 0 {
			continue
		}
		select {
		case output <- items:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
}

func (a *assembler) close() {
	if a.g != nil {
		_ = a.g.Wait()
	}
	a.decoder.close()
}

// arrange owns protocol cursors and event dependency state. Only channel
// messages cross into the reader; transport methods remain reader-owned.
func (a *assembler) arrange(ctx context.Context, requests chan<- readRequest, inputs <-chan readResult, output chan<- *writeEvent) error {
	awaitingInput := true
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if result := a.next(); result != nil {
			select {
			case output <- result:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
			continue
		}
		decoded := a.decodePending()
		if decoded == nil {
			if !awaitingInput {
				feedback := a.feedback()
				result, err := exchangeRead(ctx, requests, inputs, readRequest{
					feedback: feedback,
				})
				if err != nil {
					return err
				}
				progress := result.progress
				a.advance(feedback, progress)
				if progress.control != nil || (progress.boundary != nil && progress.boundary.reached) {
					continue
				}
				if result := a.next(); result != nil {
					select {
					case output <- result:
					case <-ctx.Done():
						return context.Cause(ctx)
					}
					continue
				}
				select {
				case requests <- readRequest{
					resume:         true,
					needsMoreInput: progress.needsMoreInput || a.needsMoreInput(),
				}:
				case <-ctx.Done():
					return context.Cause(ctx)
				}
				awaitingInput = true
			}
			select {
			case input := <-inputs:
				decoded = a.decode(ctx, input.data)
				awaitingInput = false
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		for result, err := range decoded {
			if err != nil {
				return err
			}
			input, err := exchangeRead(ctx, requests, inputs, readRequest{
				feedback: result.feedback,
			})
			if err != nil {
				return err
			}
			if err := a.accept(ctx, result, input.progress); err != nil {
				return err
			}
		}
	}
}

// exchangeRead serializes decoded facts with transport progress. A completed
// exchange publishes every reader-owned boundary change before event selection.
func exchangeRead(ctx context.Context, requests chan<- readRequest, inputs <-chan readResult, request readRequest) (readResult, error) {
	select {
	case requests <- request:
	case <-ctx.Done():
		return readResult{}, context.Cause(ctx)
	}
	select {
	case result := <-inputs:
		return result, nil
	case <-ctx.Done():
		return readResult{}, context.Cause(ctx)
	}
}

func (a *assembler) assemble(ctx context.Context, results <-chan *writeEvent, result *writeEvent) ([]*writeEvent, error) {
	var (
		rows  int64
		bytes int64
	)
collect:
	for count := range batchRows {
		a.decoding = append(a.decoding, result)
		bytes += result.bytes
		if result.dml != nil {
			rows += int64(result.dml.Len())
		}
		if count+1 == batchRows || rows >= batchRows || bytes >= batchBytes {
			break
		}
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		case next, ok := <-results:
			if !ok {
				break collect
			}
			result = next
		default:
			break collect
		}
	}
	return a.collectDecoded(), nil
}

// Join conversion results in table order. A slow table does not hold up other
// tables, but controls cannot pass unfinished conversions in their scope.
func (a *assembler) collectDecoded() []*writeEvent {
	var (
		items           []*writeEvent
		blockedRows     []*writeEvent
		blockedControls []*writeEvent
		blockedTables   map[int64]bool
	)
	remaining := a.decoding[:0]
	for _, item := range a.decoding {
		ready := true
		if item.decoding != nil {
			select {
			case <-item.decoding.done:
			default:
				ready = false
			}
		}
		if ready && len(remaining) != 0 {
			ready = item.dml == nil || !blockedTables[item.dml.PhysicalTableID]
			for _, prior := range blockedControls {
				if item.hasWatermark || (prior.ddl != nil && (item.dml == nil || ddlBlocksTable(prior.ddl, item.dml))) {
					ready = false
					break
				}
			}
			// Ordinary DML only checks its table and the few pending controls.
			// Scan row scopes when a control itself needs a completion barrier.
			if item.dml == nil {
				for _, prior := range blockedRows {
					if (item.ddl != nil && ddlBlocksTable(item.ddl, prior.dml)) ||
						(item.hasWatermark && prior.dml.CommitTs <= item.watermark) {
						ready = false
						break
					}
				}
			}
		}
		if !ready {
			remaining = append(remaining, item)
			if item.dml == nil {
				blockedControls = append(blockedControls, item)
				continue
			}
			if blockedTables == nil {
				blockedTables = make(map[int64]bool)
			}
			blockedTables[item.dml.PhysicalTableID] = true
			blockedRows = append(blockedRows, item)
			continue
		}
		if task := item.decoding; task != nil {
			task.dml.AddPostFlushFunc(item.dml.PostFlush)
			item.dml, item.bytes, item.decoding = task.dml, item.bytes+task.bytes, nil
		}
		items = append(items, item)
	}
	clear(a.decoding[len(remaining):])
	a.decoding = remaining
	return a.prepare(items)
}

type writeEvent struct {
	dml          *event.DMLEvent
	decoding     *decodeTask
	ddl          *event.DDLEvent
	onFlush      func()
	bytes        int64
	watermark    uint64
	hasWatermark bool
	sequential   bool // Preserve table batch order when metadata cannot describe every conflict key.
	boundary     *readBoundary
	proofPending bool
}

func (a *assembler) prepare(items []*writeEvent) []*writeEvent {
	if !a.mergeRows {
		return items
	}
	// A control event bounds sorting; no row crosses a DDL or watermark.
	for start := 0; start < len(items); {
		if items[start].dml == nil {
			start++
			continue
		}
		end := start
		for end < len(items) && items[end].dml != nil {
			items[end].sequential = true
			end++
		}
		if a.sortCSVRows {
			slices.SortStableFunc(items[start:end], func(x, y *writeEvent) int {
				return cmp.Or(cmp.Compare(x.dml.CommitTs, y.dml.CommitTs), cmp.Compare(x.dml.RowTypes[0], y.dml.RowTypes[0]))
			})
		}
		start = end
	}
	prepared := items[:0]
	remaining := items
	for len(remaining) != 0 {
		item := remaining[0]
		if item.dml == nil {
			prepared = append(prepared, item)
			remaining = remaining[1:]
			continue
		}
		// Merge only the bounded batch already available. Waiting for an entire
		// transaction could retain arbitrarily large input in memory.
		first := item.dml
		count, retained := int(first.Len()), item.bytes
		end := 1
		for end < len(remaining) {
			if remaining[end].dml == nil || count >= batchRows || retained >= batchBytes {
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
		var (
			rows  int
			bytes int64 = 256
		)
		for _, part := range remaining[:end] {
			rows += len(part.dml.RowTypes)
			bytes += part.dml.Rows.MemoryUsage() + int64(len(part.dml.RowTypes))*64
		}
		// Original chunks remain live through their flush callbacks. Account for
		// the merged copy separately before allocating it.
		a.memory.reserve(bytes)
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
		prepared = append(prepared, &writeEvent{
			dml:        dml,
			bytes:      retained + bytes,
			sequential: true,
		})
		remaining = remaining[end:]
	}
	clear(items[len(prepared):])
	return prepared
}

type decodeStream struct {
	decoder       codecCommon.Decoder
	cachedRecords map[*codecCommon.DMLMessage]*ack
	schemas       map[string]map[uint64]*common.TableInfo
}

// Simple retains versioned metadata independently of events awaiting writes.
func (p *decodeStream) cacheSchemas(ddl *event.DDLEvent, schemas *schemaUsage) {
	for _, table := range append([]*common.TableInfo{ddl.TableInfo}, ddl.MultipleTableInfos...) {
		if table == nil {
			continue
		}
		name := common.QuoteSchema(table.GetSchemaName(), table.GetTableName())
		versions := p.schemas[name]
		if versions == nil {
			if p.schemas == nil {
				p.schemas = make(map[string]map[uint64]*common.TableInfo)
			}
			versions = make(map[uint64]*common.TableInfo)
			p.schemas[name] = versions
		}
		previous := versions[table.UpdateTS]
		if previous == table {
			continue
		}
		schemas.retain(table)
		schemas.release(previous)
		versions[table.UpdateTS] = table
	}
}

// assembler decodes logical inputs and arranges events for downstream writes.
type assembler struct {
	g                 *errgroup.Group
	memory            *memoryUsage
	schemas           *schemaUsage
	codecConfig       *codecCommon.Config
	selectors         *columnselector.ColumnSelectors
	streams           map[int32]*decodeStream
	decoder           *decoderGroup
	tables            map[[2]string]*common.TableInfo
	current           *readData
	currentDecoder    codecCommon.Decoder
	pendingWatermarks []*writeEvent
	watermark         uint64
	mergeRows         bool
	sortCSVRows       bool
	decoding          []*writeEvent // Owned by the goroutine that collects conversion results.
	pendingDML        []*writeEvent
	pendingDDL        []*writeEvent
	unproven          []*writeEvent
	unprovenCount     int
	scanIndex         int
	priorityIndex     int
	scanWatermark     uint64
	scanDDL           *writeEvent
	scanDDLReady      bool
	scanInvalid       bool
	scanBlocked       map[int64]uint64
	scanBoundaries    map[*readBoundary]bool
	holes             int
	waitingWatermark  uint64
	waitingCount      int
	waitingKnown      bool
}

func (a *assembler) queueDML(item *writeEvent, record *ack, boundary *readBoundary) {
	dml := item.dml
	bytes := int64(256)
	a.memory.reserve(bytes)
	record.refs.Inc()
	dml.AddPostFlushFunc(record.release)
	item.bytes, item.sequential, item.boundary = bytes, a.mergeRows, boundary
	if item.boundary == nil {
		for _, ddl := range a.pendingDDL {
			if dml.CommitTs <= ddl.ddl.GetCommitTs() && ddlBlocksTable(ddl.ddl, dml) {
				// The already observed control proves that its earlier rows can
				// drain while the reader completes the control's input scope.
				item.boundary = &readBoundary{reached: true}
				break
			}
		}
	}
	if item.boundary == nil && dml.CommitTs > a.watermark {
		item.proofPending = true
		a.unproven = append(a.unproven, item)
		a.unprovenCount++
	}
	if len(a.pendingDML) == 0 {
		a.scanInvalid = true
	}
	a.pendingDML = append(a.pendingDML, item)
	if a.waitingKnown && dml.CommitTs <= a.waitingWatermark {
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

func (a *assembler) queueDDL(ddl *event.DDLEvent, record *ack) {
	a.scanInvalid = true
	if a.waitingKnown && ddl.GetCommitTs() <= a.waitingWatermark {
		a.waitingCount++
	}
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
	a.memory.reserve(bytes)
	tables := append([]*common.TableInfo{ddl.TableInfo}, ddl.MultipleTableInfos...)
	for _, table := range tables {
		a.schemas.retain(table)
	}
	record.refs.Add(1)
	a.pendingDDL = append(a.pendingDDL, &writeEvent{
		ddl:   ddl,
		bytes: bytes,
		onFlush: func() {
			for _, table := range tables {
				a.schemas.release(table)
			}
			record.release()
		},
	})
}

func (a *assembler) nextReady(watermark uint64) *writeEvent {
	var head *writeEvent
	if len(a.pendingDDL) != 0 {
		head = a.pendingDDL[0]
	}
	headReady := head != nil && ddlReady(head, watermark)
	a.prepareScan(watermark, head, headReady)
	if headReady {
		if result := a.drainDDL(head, watermark); result != nil {
			return result
		}
	}
	return a.nextDML(watermark)
}

func ddlReady(item *writeEvent, watermark uint64) bool {
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

func (a *assembler) prepareScan(watermark uint64, head *writeEvent, headReady bool) {
	reset := a.scanInvalid || a.scanWatermark != watermark || a.scanDDL != head || a.scanDDLReady != headReady || a.scanIndex > len(a.pendingDML)
	for boundary := range a.scanBoundaries {
		reset = reset || boundary.reached
	}
	a.scanWatermark, a.scanDDL, a.scanDDLReady = watermark, head, headReady
	if !reset {
		return
	}
	a.scanIndex, a.priorityIndex, a.scanBlocked = 0, 0, nil
	clear(a.scanBoundaries)
	a.scanInvalid = false
}

// Drain the first preceding row in the DDL scope, or emit the DDL when no such
// row remains. Independent tables can still progress through nextDML.
func (a *assembler) drainDDL(head *writeEvent, watermark uint64) *writeEvent {
	for index := a.priorityIndex; index < len(a.pendingDML); index++ {
		pending := a.pendingDML[index]
		if pending == nil || pending.dml.CommitTs > head.ddl.GetCommitTs() || !ddlBlocksTable(head.ddl, pending.dml) {
			continue
		}
		a.priorityIndex = index
		if pending.boundary != nil && !pending.boundary.reached {
			if a.scanBoundaries == nil {
				a.scanBoundaries = make(map[*readBoundary]bool)
			}
			a.scanBoundaries[pending.boundary] = true
		}
		if (pending.dml.CommitTs <= watermark || (pending.boundary != nil && pending.boundary.reached)) && !slices.ContainsFunc(a.pendingDDL, func(ddl *writeEvent) bool {
			return pending.dml.CommitTs > ddl.ddl.GetCommitTs() && ddlBlocksTable(ddl.ddl, pending.dml)
		}) {
			a.priorityIndex = max(a.priorityIndex, index+1)
			return a.popDML(index)
		}
		return nil
	}
	if a.waitingKnown && head.ddl.GetCommitTs() <= a.waitingWatermark {
		a.waitingCount--
	}
	a.pendingDDL[0] = nil
	a.pendingDDL = a.pendingDDL[1:]
	return head
}

func (a *assembler) nextDML(watermark uint64) *writeEvent {
	for a.scanIndex < len(a.pendingDML) {
		index := a.scanIndex
		a.scanIndex++
		result := a.pendingDML[index]
		if result == nil {
			continue
		}
		tableID := result.dml.PhysicalTableID
		if ts, ok := a.scanBlocked[tableID]; ok && ts <= result.dml.CommitTs {
			continue
		}
		// Every queued DDL fences its own post-DDL rows.
		ready := (result.dml.CommitTs <= watermark || (result.boundary != nil && result.boundary.reached)) && !slices.ContainsFunc(a.pendingDDL, func(ddl *writeEvent) bool {
			return result.dml.CommitTs > ddl.ddl.GetCommitTs() && ddlBlocksTable(ddl.ddl, result.dml)
		})
		if ready {
			return a.popDML(index)
		}
		// A later row must not overtake an earlier row waiting for its proof.
		if result.boundary != nil && !result.boundary.reached {
			if a.scanBoundaries == nil {
				a.scanBoundaries = make(map[*readBoundary]bool)
			}
			a.scanBoundaries[result.boundary] = true
		}
		if result.boundary == nil && !result.proofPending {
			result.proofPending = true
			a.unproven = append(a.unproven, result)
			a.unprovenCount++
		}
		if a.scanBlocked == nil {
			a.scanBlocked = make(map[int64]uint64)
		}
		if ts, ok := a.scanBlocked[tableID]; !ok || result.dml.CommitTs < ts {
			a.scanBlocked[tableID] = result.dml.CommitTs
		}
	}
	return nil
}

func (a *assembler) popDML(index int) *writeEvent {
	result := a.pendingDML[index]
	if a.waitingKnown && result.dml.CommitTs <= a.waitingWatermark {
		a.waitingCount--
	}
	if result.proofPending {
		result.proofPending = false
		a.unprovenCount--
		if a.unprovenCount == 0 {
			clear(a.unproven)
			a.unproven = a.unproven[:0]
		}
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
	a.pendingDML = slices.DeleteFunc(a.pendingDML, func(item *writeEvent) bool {
		return item == nil
	})
	a.holes, a.scanInvalid = 0, true
	return result
}

func (a *assembler) hasPendingThrough(watermark uint64) bool {
	if !a.waitingKnown || a.waitingWatermark != watermark {
		a.waitingWatermark, a.waitingCount, a.waitingKnown = watermark, 0, true
		for _, pending := range a.pendingDML {
			if pending != nil && pending.dml.CommitTs <= watermark {
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

func ddlBlocksTable(ddl *event.DDLEvent, dml *event.DMLEvent) bool {
	if dml.TableInfo == nil {
		return true
	}
	schema, table := dml.TableInfo.GetSchemaName(), dml.TableInfo.GetTableName()
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
	if ddl.BlockedTables != nil && slices.Contains(ddl.BlockedTables.TableIDs, dml.PhysicalTableID) {
		return true
	}
	return (ddl.TableName == "" || (ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType == event.InfluenceTypeDB)) && schema == ddl.SchemaName
}

func newAssembler(ctx context.Context, decoding *decodeConfig, upstreamDB *sql.DB, replicaConfig *config.ReplicaConfig, memory *memoryUsage, schemas *schemaUsage) *assembler {
	var selectors *columnselector.ColumnSelectors
	if decoding.codec.Protocol == config.ProtocolCsv {
		var err error
		selectors, err = columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
		if err != nil {
			log.Panic("consumer column selection configuration is invalid", zap.Error(err))
		}
	}
	streams := make(map[int32]*decodeStream)
	if decoding.format == messageFormat {
		ids := decoding.streams
		if len(ids) == 0 {
			ids = []int32{0}
		}
		// Constructors reset protocol table-ID allocation. Create all cursors
		// before parsing inputs or starting concurrent row conversion.
		for _, id := range ids {
			decoder, err := codec.NewEventDecoder(ctx, int(id), decoding.codec, decoding.topic, upstreamDB)
			if err != nil {
				log.Panic("consumer event decoder initialization failed", zap.String("protocol", decoding.codec.Protocol.String()), zap.Error(err))
			}
			streams[id] = &decodeStream{decoder: decoder}
		}
	}
	return &assembler{
		memory:      memory,
		schemas:     schemas,
		codecConfig: decoding.codec,
		selectors:   selectors,
		streams:     streams,
		decoder:     newDecoderGroup(schemas, make(chan struct{}, 1)),
		tables:      make(map[[2]string]*common.TableInfo),
		mergeRows:   decoding.codec.Protocol == config.ProtocolCsv,
		sortCSVRows: decoding.codec.Protocol == config.ProtocolCsv && decoding.codec.OutputOldValue && decoding.codec.IncludeCommitTs,
	}
}

func (a *assembler) next() *writeEvent {
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

func (a *assembler) feedback() readFeedback {
	feedback := readFeedback{pendingDML: a.unprovenCount}
	for _, pending := range a.pendingDDL {
		if pending.boundary == nil && pending.ddl.GetCommitTs() > a.watermark {
			feedback.boundaryDDL = pending.ddl
			break
		}
	}
	return feedback
}

func (a *assembler) advance(feedback readFeedback, progress readProgress) {
	a.applyProgress(progress)
	if progress.control != nil || progress.boundary == nil {
		return
	}
	for _, pending := range a.pendingDDL {
		if pending.ddl == feedback.boundaryDDL {
			pending.boundary = progress.boundary
			break
		}
	}
	// Only candidates decoded before this proof was captured may use it.
	for _, pending := range a.unproven {
		if pending.proofPending {
			pending.boundary = progress.boundary
			pending.proofPending = false
		}
	}
	clear(a.unproven)
	a.unproven = a.unproven[:0]
	a.unprovenCount = 0
	a.scanInvalid = true
}

func (a *assembler) needsMoreInput() bool {
	for boundary := range a.scanBoundaries {
		if !boundary.reached {
			return true
		}
	}
	for _, stream := range a.streams {
		if len(stream.cachedRecords) != 0 {
			return true
		}
	}
	return false
}

func (a *assembler) applyProgress(progress readProgress) {
	a.watermark = 0
	if progress.hasWatermark {
		a.watermark = progress.watermark
	}
	if control := progress.control; control != nil {
		item := &writeEvent{
			watermark:    control.watermark,
			hasWatermark: true,
		}
		if len(control.records) != 0 {
			item.onFlush = func() {
				for _, record := range control.records {
					record.release()
				}
				a.memory.release(int64(len(control.records)) * 128)
			}
		}
		a.pendingWatermarks = append(a.pendingWatermarks, item)
	}
}

type decodeConfig struct {
	codec   *codecCommon.Config
	format  readFormat
	topic   string
	streams []int32
}

// decodedData carries decoded facts and the row conversion input. The consumer
// exchanges the facts for reading progress before the assembler queues effects.
type decodedData struct {
	feedback readFeedback
	message  *codecCommon.DMLMessage
	record   *ack
}

func (a *assembler) decode(ctx context.Context, data *readData) iter.Seq2[decodedData, error] {
	if data.control == nil && !data.groupEnd && data.ddl == nil && data.format == messageFormat {
		return a.decodeMessages(data)
	}
	return func(yield func(decodedData, error) bool) {
		if data.control != nil || data.groupEnd {
			a.scanInvalid = true
			if data.control == nil && data.group.compare != nil {
				start := slices.IndexFunc(a.pendingDML, func(item *writeEvent) bool {
					return item != nil && item.boundary == data.group.boundary
				})
				if start >= 0 {
					end := start
					for end < len(a.pendingDML) && a.pendingDML[end].boundary == data.group.boundary {
						end++
					}
					slices.SortStableFunc(a.pendingDML[start:end], func(first, second *writeEvent) int {
						return data.group.compare(first.dml, second.dml)
					})
				}
			}
			yield(decodedData{
				feedback: readFeedback{
					data:    data,
					decoded: true,
				},
			}, nil)
			return
		}
		if data.ddl != nil {
			result, err := a.decodeDDL(data, data.ddl)
			if err != nil {
				yield(decodedData{}, err)
				return
			}
			if !yield(result, nil) {
				return
			}
			if !yield(decodedData{
				feedback: readFeedback{
					data:    data,
					decoded: true,
				},
			}, nil) {
				return
			}
			data.record.decoded(data.retainedBytes)
			return
		}
		// Row decoders retain parsed records alongside the encoded file until
		// the input is exhausted. Include their scratch space in its lifetime.
		bytes := int64(len(data.value))*3 + 256
		data.record.retain(bytes)
		switch a.codecConfig.Protocol {
		case config.ProtocolCsv:
			decoder, err := csv.NewDecoderWithColumnSelector(ctx, a.codecConfig, data.table, data.value, a.selectors.GetForTableInfo(data.table))
			if err != nil {
				yield(decodedData{}, errors.WrapError(errors.ErrCodecDecode, err, "create CSV decoder"))
				return
			}
			a.currentDecoder = decoder
		case config.ProtocolCanalJSON:
			a.currentDecoder = canal.NewTxnDecoder(a.codecConfig)
			a.currentDecoder.AddKeyValue(data.key, data.value)
		}
		a.current = data
	}
}

// Decode at most one row before yielding to event selection. A nil iterator
// means the assembler needs a new input; an exhausted input yields no facts.
func (a *assembler) decodePending() iter.Seq2[decodedData, error] {
	if a.current == nil {
		return nil
	}
	return func(yield func(decodedData, error) bool) {
		data := a.current
		for {
			messageType, hasNext := a.currentDecoder.HasNext()
			if !hasNext {
				data.record.decoded(data.retainedBytes)
				a.current, a.currentDecoder = nil, nil
				return
			}
			if messageType != codecCommon.MessageTypeRow {
				continue
			}
			message := a.currentDecoder.NextDMLMessage()
			yield(a.decodeMessage(data, message, data.record), nil)
			return
		}
	}
}

func (a *assembler) decodeMessage(data *readData, message *codecCommon.DMLMessage, record *ack) decodedData {
	tableID := message.TableID
	if data.format == rowFormat {
		tableID = data.group.tableID
	}
	name := [2]string{message.Schema, message.Table}
	table := a.tables[name]
	if table == nil {
		table = &common.TableInfo{
			TableName: common.TableName{
				Schema:  message.Schema,
				Table:   message.Table,
				TableID: tableID,
			},
		}
		a.memory.reserve(int64(512 + len(message.Schema) + len(message.Table)))
		a.tables[name] = table
	}
	return decodedData{
		feedback: readFeedback{
			data: data,
			dml: &event.DMLEvent{
				PhysicalTableID: tableID,
				CommitTs:        message.GetCommitTs(),
				TableInfo:       table,
				Length:          1,
				RowTypes:        []common.RowType{message.RowType},
			},
		},
		message: message,
		record:  record,
	}
}

func (a *assembler) decodeDDL(data *readData, ddl *event.DDLEvent) (decodedData, error) {
	if err := setDDLTableNames(ddl); err != nil {
		return decodedData{}, err
	}
	var needsMoreInput bool
	if stream := a.streams[data.stream]; stream != nil {
		needsMoreInput = len(stream.cachedRecords) != 0
	}
	return decodedData{
		feedback: readFeedback{
			data:           data,
			ddl:            ddl,
			needsMoreInput: needsMoreInput,
		},
	}, nil
}

func (a *assembler) accept(ctx context.Context, result decodedData, progress readProgress) error {
	a.applyProgress(progress)
	if progress.skip {
		return nil
	}
	data := result.feedback.data
	if dml := result.feedback.dml; dml != nil {
		task := &decodeTask{
			message:       result.message,
			record:        result.record,
			done:          make(chan struct{}),
			table:         data.table,
			tableID:       dml.PhysicalTableID,
			retainedBytes: data.retainedBytes,
		}
		item := &writeEvent{
			dml:      dml,
			decoding: task,
		}
		a.queueDML(item, result.record, data.dmlBoundary)
		return a.decoder.submit(ctx, task)
	}
	if ddl := result.feedback.ddl; ddl != nil {
		a.queueDDL(ddl, data.record)
		a.pendingDDL[len(a.pendingDDL)-1].boundary = data.ddlBoundary
		if data.ddlOrder == commitOrder {
			slices.SortStableFunc(a.pendingDDL, func(first, second *writeEvent) int {
				return cmp.Compare(first.ddl.GetCommitTs(), second.ddl.GetCommitTs())
			})
		}
	}
	return nil
}

func (a *assembler) decodeCachedMessages(data *readData, stream *decodeStream, decoder *simple.Decoder, yield func(decodedData, error) bool) bool {
	for _, message := range decoder.GetCachedMessages() {
		cached := stream.cachedRecords[message]
		delete(stream.cachedRecords, message)
		if !yield(a.decodeMessage(data, message, cached), nil) {
			return false
		}
		cached.decoded(data.retainedBytes)
	}
	return true
}

func (a *assembler) decodeMessages(data *readData) iter.Seq2[decodedData, error] {
	return func(yield func(decodedData, error) bool) {
		p := a.streams[data.stream]
		record := data.record
		// Captured protocol data remains live while conversion jobs run. Account
		// once per input, not once per derived row in a batched message.
		bytes := int64(len(data.key)+len(data.value))*3 + 256
		record.retain(bytes)
		p.decoder.AddKeyValue(data.key, data.value)
		for {
			messageType, hasNext := p.decoder.HasNext()
			if !hasNext {
				break
			}
			switch messageType {
			case codecCommon.MessageTypeRow:
				message := p.decoder.NextDMLMessage()
				if message == nil {
					decoder := p.decoder.(*simple.Decoder)
					record.retainDecode()
					if p.cachedRecords == nil {
						p.cachedRecords = make(map[*codecCommon.DMLMessage]*ack)
					}
					p.cachedRecords[decoder.PendingDMLMessage] = record
					continue
				}
				if !yield(a.decodeMessage(data, message, record), nil) {
					return
				}
			case codecCommon.MessageTypeDDL:
				ddl := p.decoder.NextDDLEvent()
				if decoder, ok := p.decoder.(*simple.Decoder); ok {
					p.cacheSchemas(ddl, a.schemas)
					if !a.decodeCachedMessages(data, p, decoder, yield) {
						return
					}
				}
				if a.codecConfig.Protocol == config.ProtocolSimple && ddl.Query == "" {
					continue
				}
				result, err := a.decodeDDL(data, ddl)
				if err != nil {
					yield(decodedData{}, err)
					return
				}
				if !yield(result, nil) {
					return
				}
			case codecCommon.MessageTypeResolved:
				if !yield(decodedData{feedback: readFeedback{
					data:           data,
					watermark:      p.decoder.NextResolvedEvent(),
					hasWatermark:   true,
					needsMoreInput: len(p.cachedRecords) != 0,
				}}, nil) {
					return
				}
			}
		}
		if !yield(decodedData{feedback: readFeedback{
			data:           data,
			decoded:        true,
			needsMoreInput: len(p.cachedRecords) != 0,
		}}, nil) {
			return
		}
		record.decoded(data.retainedBytes)
	}
}
