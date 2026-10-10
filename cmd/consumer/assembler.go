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
	"slices"

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
)

type writeEvent struct {
	dml          *event.DMLEvent
	ddl          *event.DDLEvent
	onFlush      func()
	bytes        int64
	watermark    uint64
	hasWatermark bool
	tableID      int64
	sequential   bool // Preserve table batch order when metadata cannot describe every conflict key.
	boundary     *readBoundary
	proofPending bool
}

func (a *assembler) prepare(ctx context.Context, items []*writeEvent) ([]*writeEvent, error) {
	if !a.mergeRows {
		return items, nil
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
		rows, bytes := 0, int64(256)
		for _, part := range remaining[:end] {
			rows += len(part.dml.RowTypes)
			bytes += part.dml.Rows.MemoryUsage() + int64(len(part.dml.RowTypes))*64
		}
		// Original chunks remain live through their flush callbacks. Account for
		// the merged copy separately before allocating it.
		if err := a.memory.reserve(ctx, bytes); err != nil {
			return nil, err
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
				if len(part.RowKeys) == 0 {
					dml.RowKeys = append(dml.RowKeys, make([][]byte, len(part.RowTypes))...)
				} else {
					dml.RowKeys = append(dml.RowKeys, part.RowKeys...)
				}
			}
			if len(dml.Checksum) != 0 || len(part.Checksum) != 0 {
				if len(dml.Checksum) == 0 {
					dml.Checksum = make([]*integrity.Checksum, dml.Length)
				}
				if len(part.Checksum) == 0 {
					dml.Checksum = append(dml.Checksum, make([]*integrity.Checksum, part.Length)...)
				} else {
					dml.Checksum = append(dml.Checksum, part.Checksum...)
				}
			}
			dml.Length += part.Length
			dml.ApproximateSize += part.ApproximateSize
			dml.AddPostEnqueueFunc(part.PostEnqueue)
			dml.AddPostFlushFunc(part.PostFlush)
		}
		prepared = append(prepared, &writeEvent{dml: dml, bytes: retained + bytes, sequential: true})
		remaining = remaining[end:]
	}
	clear(items[len(prepared):])
	return prepared, nil
}

type decodeStream struct {
	decoder       codecCommon.Decoder
	cachedRecords map[*codecCommon.DMLMessage]*ack
	schemas       map[string]map[uint64]*common.TableInfo
}

// assembler decodes logical inputs and arranges events for downstream writes.
type assembler struct {
	memory            *memoryUsage
	codecConfig       *codecCommon.Config
	upstreamDB        *sql.DB
	topic             string
	selectors         *columnselector.ColumnSelectors
	streams           map[int32]*decodeStream
	current           *readData
	currentDecoder    codecCommon.Decoder
	pendingWatermarks []*writeEvent
	watermark         uint64
	mergeRows         bool
	sortCSVRows       bool
	protocol          config.Protocol
	pendingDML        []*writeEvent
	pendingDDL        []*writeEvent
	unproven          []*writeEvent
	unprovenCount     int
	scanIndex         int
	priorityIndex     int
	scanWatermark     uint64
	scanDDL           *writeEvent
	scanDDLReady      bool
	scanFirst         *writeEvent
	scanInvalid       bool
	scanBlocked       map[int64]uint64
	scanBoundaries    map[*readBoundary]bool
	holes             int
	waitingWatermark  uint64
	waitingTable      int64
	waitingCount      int
	waitingKnown      bool
}

func (a *assembler) queueDML(ctx context.Context, dml *event.DMLEvent, records []*ack, boundary *readBoundary) error {
	if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
		return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
	}
	if err := a.memory.retainSchema(ctx, dml.TableInfo); err != nil {
		return err
	}
	bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64 + 256
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	for _, record := range records {
		record.refs.Add(1)
	}
	dml.AddPostFlushFunc(func() {
		a.memory.releaseSchema(dml.TableInfo)
		for _, record := range records {
			record.release()
		}
	})
	item := &writeEvent{dml: dml, bytes: bytes, sequential: a.mergeRows, boundary: boundary}
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
	a.pendingDML = append(a.pendingDML, item)
	if a.waitingKnown && dml.CommitTs <= a.waitingWatermark && (a.waitingTable == 0 || dml.PhysicalTableID == a.waitingTable) {
		a.waitingCount++
	}
	return nil
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
			if stmt.ReferTable != nil {
				for _, table := range []*ast.TableName{stmt.Table, stmt.ReferTable} {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{SchemaName: cmp.Or(table.Schema.O, ddl.SchemaName), TableName: table.Name.O})
				}
			}
		case *ast.RenameTableStmt:
			for _, pair := range stmt.TableToTables {
				for _, table := range []*ast.TableName{pair.OldTable, pair.NewTable} {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{SchemaName: cmp.Or(table.Schema.O, ddl.SchemaName), TableName: table.Name.O})
				}
			}
		case *ast.AlterTableStmt:
			ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{SchemaName: cmp.Or(stmt.Table.Schema.O, ddl.SchemaName), TableName: stmt.Table.Name.O})
			for _, spec := range stmt.Specs {
				if spec.Tp == ast.AlterTableExchangePartition && spec.NewTable != nil {
					ddl.BlockedTableNames = append(ddl.BlockedTableNames, event.SchemaTableName{SchemaName: cmp.Or(spec.NewTable.Schema.O, ddl.SchemaName), TableName: spec.NewTable.Name.O})
				}
			}
		}
	}
	return nil
}

func (a *assembler) queueDDL(ctx context.Context, ddl *event.DDLEvent, record *ack) error {
	a.scanInvalid = true
	if a.waitingKnown && ddl.GetCommitTs() <= a.waitingWatermark {
		a.waitingCount++
	}
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	tables := append([]*common.TableInfo{ddl.TableInfo}, ddl.MultipleTableInfos...)
	for _, table := range tables {
		if err := a.memory.retainSchema(ctx, table); err != nil {
			return err
		}
	}
	record.refs.Add(1)
	a.pendingDDL = append(a.pendingDDL, &writeEvent{ddl: ddl, bytes: bytes, onFlush: func() {
		for _, table := range tables {
			a.memory.releaseSchema(table)
		}
		record.release()
	}})
	return nil
}

func (a *assembler) nextReady(watermark uint64) *writeEvent {
	var head *writeEvent
	if len(a.pendingDDL) != 0 {
		head = a.pendingDDL[0]
	}
	headReady := false
	if head != nil {
		early := false
		switch timodel.ActionType(head.ddl.Type) {
		case timodel.ActionCreateSchema:
			early = true
		case timodel.ActionCreateTable:
			blocked := head.ddl.GetBlockedTables()
			early = blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal && (len(blocked.TableIDs) == 0 || (len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID)) && len(head.ddl.GetBlockedTableNames()) == 0
		}
		headReady = head.ddl.GetCommitTs() <= watermark || early || (head.boundary != nil && head.boundary.reached)
	}
	reset := a.scanInvalid || a.scanWatermark != watermark || a.scanDDL != head || a.scanDDLReady != headReady || a.scanIndex > len(a.pendingDML)
	if len(a.pendingDML) != 0 && a.scanFirst != a.pendingDML[0] {
		reset = true
	}
	for boundary := range a.scanBoundaries {
		reset = reset || boundary.reached
	}
	if reset {
		a.scanIndex, a.priorityIndex, a.scanBlocked = 0, 0, nil
		clear(a.scanBoundaries)
		a.scanInvalid = false
	}
	a.scanWatermark, a.scanDDL, a.scanDDLReady = watermark, head, headReady
	readyIndex := -1
	priority := false
	if headReady {
		ready := true
		for index := a.priorityIndex; index < len(a.pendingDML); index++ {
			pending := a.pendingDML[index]
			if pending != nil {
				if pending.dml.CommitTs <= head.ddl.GetCommitTs() && ddlBlocksTable(head.ddl, pending.dml) {
					ready = false
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
						readyIndex = index
						priority = true
					}
					break
				}
			}
		}
		if ready {
			if a.waitingKnown && head.ddl.GetCommitTs() <= a.waitingWatermark {
				a.waitingCount--
			}
			a.pendingDDL[0] = nil
			a.pendingDDL = a.pendingDDL[1:]
			return head
		}
	}
	if readyIndex < 0 {
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
				readyIndex = index
				break
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
	}
	if readyIndex >= 0 {
		result := a.pendingDML[readyIndex]
		if a.waitingKnown && result.dml.CommitTs <= a.waitingWatermark && (a.waitingTable == 0 || result.dml.PhysicalTableID == a.waitingTable) {
			a.waitingCount--
		}
		if priority {
			a.priorityIndex = max(a.priorityIndex, readyIndex+1)
		}
		if result.proofPending {
			result.proofPending = false
			a.unprovenCount--
			if a.unprovenCount == 0 {
				clear(a.unproven)
				a.unproven = a.unproven[:0]
			}
		}
		a.pendingDML[readyIndex] = nil
		a.holes++
		for len(a.pendingDML) != 0 && a.pendingDML[0] == nil {
			a.pendingDML = a.pendingDML[1:]
			a.scanIndex = max(0, a.scanIndex-1)
			a.priorityIndex = max(0, a.priorityIndex-1)
			a.holes--
		}
		if a.holes > 256 && a.holes*2 > len(a.pendingDML) {
			remaining := a.pendingDML[:0]
			for _, item := range a.pendingDML {
				if item != nil {
					remaining = append(remaining, item)
				}
			}
			clear(a.pendingDML[len(remaining):])
			a.pendingDML = remaining
			a.holes, a.scanInvalid = 0, true
		}
		if len(a.pendingDML) != 0 {
			a.scanFirst = a.pendingDML[0]
		} else {
			a.scanFirst = nil
		}
		return result
	}
	if len(a.pendingDML) != 0 {
		a.scanFirst = a.pendingDML[0]
	}
	return nil
}

func (a *assembler) hasPendingThrough(watermark uint64, tableID int64) bool {
	if !a.waitingKnown || a.waitingWatermark != watermark || a.waitingTable != tableID {
		a.waitingWatermark, a.waitingTable, a.waitingCount, a.waitingKnown = watermark, tableID, 0, true
		for _, pending := range a.pendingDML {
			if pending != nil && pending.dml.CommitTs <= watermark && (tableID == 0 || pending.dml.PhysicalTableID == tableID) {
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

func newAssembler(decoding *decodeConfig, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (*assembler, error) {
	var selectors *columnselector.ColumnSelectors
	if decoding.codec.Protocol == config.ProtocolCsv {
		var err error
		selectors, err = columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
		if err != nil {
			return nil, err
		}
	}
	return &assembler{
		memory: memory, codecConfig: decoding.codec, upstreamDB: decoding.upstreamDB, topic: decoding.topic,
		protocol: decoding.codec.Protocol, selectors: selectors, streams: make(map[int32]*decodeStream),
		mergeRows:   decoding.codec.Protocol == config.ProtocolCsv,
		sortCSVRows: decoding.codec.Protocol == config.ProtocolCsv && decoding.codec.OutputOldValue && decoding.codec.IncludeCommitTs,
	}, nil
}

func (a *assembler) next(ctx context.Context, reader reader) (*writeEvent, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		if result := a.nextReady(a.watermark); result != nil {
			return result, nil
		}
		if len(a.pendingWatermarks) != 0 && !a.hasPendingThrough(a.pendingWatermarks[0].watermark, a.pendingWatermarks[0].tableID) {
			result := a.pendingWatermarks[0]
			a.pendingWatermarks[0] = nil
			a.pendingWatermarks = a.pendingWatermarks[1:]
			return result, nil
		}
		if a.current != nil {
			if err := a.decodeRows(ctx, reader); err != nil {
				return nil, err
			}
			continue
		}

		feedback := readFeedback{}
		var control *writeEvent
		for _, pending := range a.pendingDDL {
			if pending.boundary == nil && pending.ddl.GetCommitTs() > a.watermark {
				feedback.boundaryDDL, control = pending.ddl, pending
				break
			}
		}
		feedback.pendingDML = a.unprovenCount
		progress, err := reader.Advance(ctx, feedback)
		if err != nil {
			return nil, err
		}
		a.applyProgress(progress)
		if progress.control != nil {
			continue
		}
		if progress.boundary != nil {
			// Only candidates decoded before this proof was captured may use it.
			if control != nil {
				control.boundary = progress.boundary
			}
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
			if progress.boundary.reached {
				continue
			}
		}
		if result := a.nextReady(a.watermark); result != nil {
			return result, nil
		}
		if len(a.pendingWatermarks) != 0 && !a.hasPendingThrough(a.pendingWatermarks[0].watermark, a.pendingWatermarks[0].tableID) {
			continue
		}
		needsMoreInput := progress.needsMoreInput
		for boundary := range a.scanBoundaries {
			needsMoreInput = needsMoreInput || !boundary.reached
		}
		for _, stream := range a.streams {
			needsMoreInput = needsMoreInput || len(stream.cachedRecords) != 0
		}
		if !needsMoreInput {
			if err := a.memory.wait(ctx); err != nil {
				return nil, err
			}
		}
		data, err := reader.Read(ctx)
		if err != nil {
			return nil, err
		}
		if data == nil {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an empty result")
		}
		if err := a.decode(ctx, data, reader); err != nil {
			return nil, err
		}
	}
}

func (a *assembler) applyProgress(progress readProgress) {
	a.watermark = 0
	if progress.hasWatermark {
		a.watermark = progress.watermark
	}
	if control := progress.control; control != nil {
		item := &writeEvent{watermark: control.watermark, tableID: control.tableID, hasWatermark: true}
		if len(control.records) != 0 {
			item.onFlush = func() {
				for _, record := range control.records {
					record.release()
				}
				a.memory.release(control.bytes)
			}
		}
		a.pendingWatermarks = append(a.pendingWatermarks, item)
	}
}

func (a *assembler) decode(ctx context.Context, data *readData, reader reader) error {
	if data.control != nil || data.groupEnd {
		a.scanInvalid = true
		if data.control == nil && data.group.order == commitOrder {
			start := slices.IndexFunc(a.pendingDML, func(item *writeEvent) bool { return item != nil && item.boundary == data.group.boundary })
			if start >= 0 {
				end := start
				for end < len(a.pendingDML) && a.pendingDML[end].boundary == data.group.boundary {
					end++
				}
				slices.SortStableFunc(a.pendingDML[start:end], func(first, second *writeEvent) int {
					order := cmp.Compare(first.dml.CommitTs, second.dml.CommitTs)
					if order != 0 || !a.sortCSVRows {
						return order
					}
					return cmp.Compare(first.dml.RowTypes[0], second.dml.RowTypes[0])
				})
			}
		}
		progress, err := reader.Advance(ctx, readFeedback{data: data, decoded: true})
		if err != nil {
			return err
		}
		a.applyProgress(progress)
		return nil
	}
	if data.ddl != nil {
		if err := a.assembleDDL(ctx, data, data.ddl, reader); err != nil {
			return err
		}
		progress, err := reader.Advance(ctx, readFeedback{data: data, decoded: true})
		if err != nil {
			return err
		}
		a.applyProgress(progress)
		a.memory.decoded(data.record, data.retainedBytes)
		return nil
	}
	switch data.format {
	case rowFormat:
		if data.table == nil || data.group == nil {
			return errors.ErrInternalCheckFailed.FastGenByArgs("row input has no table metadata or group")
		}
		// Row decoders retain parsed records alongside the encoded file until
		// the input is exhausted. Include their scratch space in its lifetime.
		bytes := int64(len(data.value))*3 + 256
		if err := a.memory.reserve(ctx, bytes); err != nil {
			return err
		}
		data.record.memory.Add(bytes)
		switch a.protocol {
		case config.ProtocolCsv:
			decoder, err := csv.NewDecoderWithColumnSelector(ctx, a.codecConfig, data.table, data.value, a.selectors.GetForTableInfo(data.table))
			if err != nil {
				return errors.WrapError(errors.ErrCodecDecode, err, "create CSV decoder")
			}
			a.currentDecoder = decoder
		case config.ProtocolCanalJSON:
			a.currentDecoder = canal.NewTxnDecoder(a.codecConfig)
			a.currentDecoder.AddKeyValue(data.key, data.value)
		default:
			return errors.ErrCodecDecode.FastGenByArgs("protocol cannot decode rows with external table metadata")
		}
		a.current = data
		return nil
	case messageFormat:
		return a.decodeMessages(ctx, data, reader)
	default:
		return errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an unknown input format")
	}
}

func (a *assembler) decodeRows(ctx context.Context, reader reader) error {
	data := a.current
	for {
		messageType, hasNext := a.currentDecoder.HasNext()
		if !hasNext {
			a.memory.decoded(data.record, data.retainedBytes)
			a.current, a.currentDecoder = nil, nil
			return nil
		}
		if messageType != codecCommon.MessageTypeRow {
			continue
		}
		message := a.currentDecoder.NextDMLMessage()
		if message == nil {
			return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DML message")
		}
		return a.assembleDML(ctx, data, message.ToDMLEvent(), []*ack{data.record}, reader)
	}
}

func (a *assembler) assembleDML(ctx context.Context, data *readData, dml *event.DMLEvent, records []*ack, reader reader) error {
	if dml == nil || dml.TableInfo == nil {
		return errors.ErrCodecDecode.FastGenByArgs("DML message has no table metadata")
	}
	if data.format == rowFormat {
		dml.PhysicalTableID = data.group.tableID
		dml.TableInfo.UpdateTS = data.table.UpdateTS
	}
	progress, err := reader.Advance(ctx, readFeedback{data: data, dml: dml})
	if err != nil {
		return err
	}
	a.applyProgress(progress)
	if progress.skip {
		return nil
	}
	return a.queueDML(ctx, dml, records, data.dmlBoundary)
}

func (a *assembler) assembleDDL(ctx context.Context, data *readData, ddl *event.DDLEvent, reader reader) error {
	if err := setDDLTableNames(ddl); err != nil {
		return err
	}
	needsMoreInput := false
	if stream := a.streams[data.stream]; stream != nil {
		needsMoreInput = len(stream.cachedRecords) != 0
	}
	progress, err := reader.Advance(ctx, readFeedback{data: data, ddl: ddl, needsMoreInput: needsMoreInput})
	if err != nil {
		return err
	}
	a.applyProgress(progress)
	if progress.skip {
		return nil
	}
	if err := a.queueDDL(ctx, ddl, data.record); err != nil {
		return err
	}
	a.pendingDDL[len(a.pendingDDL)-1].boundary = data.ddlBoundary
	if data.ddlOrder == commitOrder {
		slices.SortStableFunc(a.pendingDDL, func(first, second *writeEvent) int {
			return cmp.Compare(first.ddl.GetCommitTs(), second.ddl.GetCommitTs())
		})
	}
	return nil
}

func (a *assembler) decodeMessages(ctx context.Context, data *readData, reader reader) error {
	p := a.streams[data.stream]
	if p == nil {
		decoder, err := codec.NewEventDecoder(ctx, int(data.stream), a.codecConfig, a.topic, a.upstreamDB)
		if err != nil {
			return err
		}
		p = &decodeStream{decoder: decoder}
		a.streams[data.stream] = p
	}
	record := data.record
	cachedInput := false
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
				decoder, ok := p.decoder.(*simple.Decoder)
				if !ok {
					return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DML message")
				}
				record.refs.Add(1)
				if p.cachedRecords == nil {
					p.cachedRecords = make(map[*codecCommon.DMLMessage]*ack)
				}
				p.cachedRecords[decoder.PendingDMLMessage] = record
				cachedInput = true
				bytes := int64(len(data.key)+len(data.value))*3 + 256
				if err := a.memory.reserve(ctx, bytes); err != nil {
					return err
				}
				record.memory.Add(bytes)
				continue
			}
			if err := a.assembleDML(ctx, data, message.ToDMLEvent(), []*ack{record}, reader); err != nil {
				return err
			}
		case codecCommon.MessageTypeDDL:
			ddl := p.decoder.NextDDLEvent()
			if ddl == nil {
				return errors.ErrCodecDecode.FastGenByArgs("decoder returned an empty DDL event")
			}
			if decoder, ok := p.decoder.(*simple.Decoder); ok {
				// Simple retains versioned metadata in its decoder. Count that
				// ownership independently of the events currently being written.
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
					if previous := versions[table.UpdateTS]; previous != table {
						if err := a.memory.retainSchema(ctx, table); err != nil {
							return err
						}
						a.memory.releaseSchema(previous)
						versions[table.UpdateTS] = table
					}
				}
				for _, message := range decoder.GetCachedMessages() {
					cached := p.cachedRecords[message]
					if cached == nil {
						return errors.ErrInternalCheckFailed.FastGenByArgs("Simple Protocol released a DML message without its input")
					}
					delete(p.cachedRecords, message)
					if err := a.assembleDML(ctx, data, message.ToDMLEvent(), []*ack{cached}, reader); err != nil {
						return err
					}
					a.memory.decoded(cached, data.retainedBytes)
				}
			}
			if ddl.Query == "" {
				if a.protocol != config.ProtocolSimple {
					return errors.ErrCodecDecode.FastGenByArgs("DDL query is empty")
				}
				continue
			}
			if err := a.assembleDDL(ctx, data, ddl, reader); err != nil {
				return err
			}
		case codecCommon.MessageTypeResolved:
			progress, err := reader.Advance(ctx, readFeedback{
				data: data, watermark: p.decoder.NextResolvedEvent(), hasWatermark: true, needsMoreInput: len(p.cachedRecords) != 0,
			})
			if err != nil {
				return err
			}
			a.applyProgress(progress)
		default:
			return errors.ErrCodecDecode.FastGenByArgs("decoder returned an unknown message type")
		}
	}
	progress, err := reader.Advance(ctx, readFeedback{data: data, decoded: true, needsMoreInput: len(p.cachedRecords) != 0})
	if err != nil {
		return err
	}
	a.applyProgress(progress)
	if !cachedInput {
		a.memory.decoded(record, data.retainedBytes)
	} else {
		record.release()
	}
	return nil
}
