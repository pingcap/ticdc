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
	"math"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	routing "github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/pkg/cloudstorage"
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
}

func (a *assembler) prepare(ctx context.Context, items []*writeEvent) (*writeEvent, []*writeEvent, error) {
	if !a.mergeRows || items[0].dml == nil {
		return items[0], items[1:], nil
	}
	// A control event bounds sorting; no row crosses a DDL or watermark.
	end := 0
	for end < len(items) && items[end].dml != nil {
		items[end].sequential = true
		end++
	}
	if a.sortCSVRows {
		slices.SortStableFunc(items[:end], func(x, y *writeEvent) int {
			return cmp.Or(cmp.Compare(x.dml.CommitTs, y.dml.CommitTs), cmp.Compare(x.dml.RowTypes[0], y.dml.RowTypes[0]))
		})
	}
	// Merge only the bounded batch already available. Waiting for an entire
	// transaction could retain arbitrarily large input in memory.
	item := items[0]
	first := item.dml
	count, retained := int(first.Len()), item.bytes
	end = 1
	for end < len(items) {
		if items[end].dml == nil || count >= batchRows || retained >= batchBytes {
			break
		}
		next := items[end].dml
		if next.PhysicalTableID != first.PhysicalTableID || next.CommitTs != first.CommitTs || !sameTableSchema(first.TableInfo, next.TableInfo) {
			break
		}
		count += int(next.Len())
		retained += items[end].bytes
		end++
	}
	if end == 1 {
		return item, items[1:], nil
	}
	rows, bytes := 0, int64(256)
	for _, part := range items[:end] {
		rows += len(part.dml.RowTypes)
		bytes += part.dml.Rows.MemoryUsage() + int64(len(part.dml.RowTypes))*64
	}
	// Original chunks remain live through their flush callbacks. Account for
	// the merged copy separately before allocating it.
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return nil, nil, err
	}
	dml := event.NewDMLEvent(first.DispatcherID, first.PhysicalTableID, first.StartTs, first.CommitTs, first.TableInfo)
	dml.Version, dml.Seq, dml.Epoch = first.Version, first.Seq, first.Epoch
	dml.ReplicatingTs, dml.TableInfoVersion = first.ReplicatingTs, first.TableInfoVersion
	dml.Rows = chunk.NewChunkWithCapacity(first.TableInfo.GetFieldSlice(), rows)
	for _, source := range items[:end] {
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
	return &writeEvent{dml: dml, bytes: retained + bytes, sequential: true}, items[end:], nil
}

type partition struct {
	decoder          codecCommon.Decoder
	progress         *readProgress
	cachedRecords    []*ack
	cachedUnreleased int
	schemas          map[schemaKey]bool
	schemaPointers   map[*common.TableInfo]bool
}

type schemaKey struct {
	schema  string
	table   string
	version uint64
}

// assembler decodes inputs and arranges events for downstream writes.
type assembler struct {
	memory                *memoryUsage
	codecConfig           *codecCommon.Config
	upstreamDB            *sql.DB
	storage               *storageAssembly
	ddlCopies             map[uint64]map[int32][]*ack
	pendingWatermarks     []*writeEvent
	deliveredWatermark    uint64
	hasDeliveredWatermark bool
	mergeRows             bool
	sortCSVRows           bool
	protocol              config.Protocol
	source                sourceType
	partitions            map[int32]*partition
	pendingDML            []*writeEvent
	pendingDDL            []*writeEvent
	orderedDML            bool
	boundary              *readBoundary
	router                *eventrouter.EventRouter
}

func (a *assembler) queueDML(ctx context.Context, dml *event.DMLEvent, records []*ack, p *partition) error {
	if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
		return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
	}
	if p != nil {
		if err := a.trackSchema(ctx, p, dml.TableInfo); err != nil {
			return err
		}
	}
	bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64 + 256
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	for _, record := range records {
		record.refs.Add(1)
	}
	dml.AddPostFlushFunc(func() {
		for _, record := range records {
			record.refs.Add(-1)
		}
	})
	item := &writeEvent{dml: dml, bytes: bytes, sequential: a.mergeRows}
	if p != nil && (a.source == sourcePulsar || (p.progress != nil && p == a.partitions[0])) {
		// Pulsar keeps its existing input-order contract. Kafka partition zero
		// already exposes every preceding executable DDL.
		item.boundary = &readBoundary{reached: true}
	}
	if item.boundary == nil {
		for _, ddl := range a.pendingDDL {
			if dml.CommitTs <= ddl.ddl.GetCommitTs() && ddlBlocksTable(ddl.ddl, dml) {
				if p != nil && p.progress != nil {
					// Kafka's canonical DDL stream already contains the preceding
					// controls for these rows. Flush them while reading its scope.
					item.boundary = &readBoundary{reached: true}
				} else {
					item.boundary = ddl.boundary
				}
				break
			}
		}
	}
	a.pendingDML = append(a.pendingDML, item)
	return nil
}

func (a *assembler) trackSchema(ctx context.Context, p *partition, table *common.TableInfo) error {
	if table == nil {
		return nil
	}
	key := schemaKey{schema: table.GetSchemaName(), table: table.GetTableName(), version: table.GetUpdateTS()}
	unversioned := table.GetUpdateTS() == 0 && (a.protocol == config.ProtocolCanalJSON || a.protocol == config.ProtocolOpen)
	if unversioned {
		if p.schemaPointers[table] {
			return nil
		}
	} else if p.schemas[key] {
		return nil
	}
	data, err := table.Marshal()
	if err != nil {
		return errors.WrapError(errors.ErrCodecDecode, err, "measure consumer table metadata")
	}
	bytes := int64(len(data))*4 + 1024
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	if unversioned {
		p.schemaPointers[table] = true
	} else {
		p.schemas[key] = true
	}
	return nil
}

func (a *assembler) queueDDL(ctx context.Context, ddl *event.DDLEvent, record *ack) error {
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
	bytes := int64(len(ddl.Query) + len(ddl.SchemaName) + len(ddl.TableName) + 1024)
	if err := a.memory.reserve(ctx, bytes); err != nil {
		return err
	}
	record.refs.Add(1)
	a.pendingDDL = append(a.pendingDDL, &writeEvent{ddl: ddl, bytes: bytes, onFlush: func() { record.refs.Add(-1) }})
	return nil
}

func (a *assembler) nextReady(watermark uint64) *writeEvent {
	var head *writeEvent
	if len(a.pendingDDL) != 0 {
		head = a.pendingDDL[0]
	}
	readyIndex := -1
	for index, result := range a.pendingDML {
		ready := result.dml.CommitTs <= watermark || (a.orderedDML && (len(a.partitions) <= 1 || (result.boundary != nil && result.boundary.reached)))
		// Every queued DDL fences its own post-DDL rows. Independent tables can
		// keep writing while the head waits for its input boundary.
		for _, pending := range a.pendingDDL {
			if result.dml.CommitTs > pending.ddl.GetCommitTs() && ddlBlocksTable(pending.ddl, result.dml) {
				ready = false
				break
			}
		}
		if ready {
			if readyIndex < 0 {
				readyIndex = index
			}
			if head == nil {
				break
			}
			if result.dml.CommitTs <= head.ddl.GetCommitTs() && ddlBlocksTable(head.ddl, result.dml) {
				// Drain the head's scope first so an independent backlog cannot
				// delay an executable DDL.
				readyIndex = index
				break
			}
		}
		if !a.orderedDML && result.dml.CommitTs > watermark {
			break
		}
	}
	if head != nil {
		early := false
		switch timodel.ActionType(head.ddl.Type) {
		case timodel.ActionCreateSchema:
			early = true
		case timodel.ActionCreateTable:
			blocked := head.ddl.GetBlockedTables()
			early = blocked != nil && blocked.InfluenceType == event.InfluenceTypeNormal && (len(blocked.TableIDs) == 0 || (len(blocked.TableIDs) == 1 && blocked.TableIDs[0] == common.DDLSpanTableID)) && len(head.ddl.GetBlockedTableNames()) == 0
		}
		ready := head.ddl.GetCommitTs() <= watermark || early || (a.source != sourcePulsar && a.orderedDML && (len(a.partitions) <= 1 || (head.boundary != nil && head.boundary.reached)))
		if ready {
			for _, pending := range a.pendingDML {
				if pending.dml.CommitTs <= head.ddl.GetCommitTs() && ddlBlocksTable(head.ddl, pending.dml) {
					ready = false
					break
				}
			}
		}
		if ready {
			a.pendingDDL[0] = nil
			a.pendingDDL = a.pendingDDL[1:]
			return head
		}
	}
	if readyIndex >= 0 {
		result := a.pendingDML[readyIndex]
		if readyIndex == 0 {
			a.pendingDML[0] = nil
			a.pendingDML = a.pendingDML[1:]
		} else {
			a.pendingDML = slices.Delete(a.pendingDML, readyIndex, readyIndex+1)
		}
		return result
	}
	return nil
}

func (a *assembler) hasPendingThrough(watermark uint64) bool {
	for _, pending := range a.pendingDML {
		if pending.dml.CommitTs <= watermark {
			return true
		}
	}
	for _, pending := range a.pendingDDL {
		if pending.ddl.GetCommitTs() <= watermark {
			return true
		}
	}
	return false
}

// Only events already decoded before the offset snapshot share its control proof.
func (a *assembler) captureBoundary(ctx context.Context, reader reader, watermark uint64) (bool, error) {
	r, ok := reader.(*kafkaReader)
	if !ok || !a.orderedDML || len(a.partitions) <= 1 {
		return false, nil
	}
	if a.boundary != nil && !a.boundary.reached {
		return true, nil
	}
	a.boundary = nil
	var (
		needed  bool
		control *writeEvent
		err     error
	)
	for _, pending := range a.pendingDDL {
		if pending.boundary == nil && pending.ddl.GetCommitTs() > watermark {
			needed, control = true, pending
			break
		}
	}
	for _, pending := range a.pendingDML {
		needed = needed || (pending.boundary == nil && pending.dml.CommitTs > watermark)
	}
	if !needed {
		return false, nil
	}
	// Drain already available input into a bounded candidate batch without
	// waiting for another message or starting a new over-budget read window.
	if control == nil && len(a.pendingDML) < 128 && a.memory.used() < maxMemoryBytes {
		r.limitReads()
		if r.nextPartition() >= 0 {
			return true, nil
		}
	}
	partitions := []int32{0}
	var commitTs uint64
	if control != nil {
		partitions, err = a.ddlPartitions(control.ddl)
		if err != nil {
			return false, err
		}
		if partitions != nil && !slices.Contains(partitions, int32(0)) {
			partitions = append(partitions, 0)
		}
		commitTs = control.ddl.GetCommitTs()
	}
	a.boundary, err = r.capture(ctx, partitions, commitTs)
	if err != nil {
		return false, err
	}
	if commitTs != 0 {
		r.receivedDDL(0, commitTs)
		for partitionID := range a.ddlCopies[commitTs] {
			r.receivedDDL(partitionID, commitTs)
		}
		r.advanceBoundary()
	}
	if control != nil {
		control.boundary = a.boundary
	}
	for _, pending := range a.pendingDML {
		if pending.boundary == nil {
			pending.boundary = a.boundary
		}
	}
	return true, nil
}

// A nil selection conservatively covers every partition. Table dispatch uses
// logical names, including both sides of rename/exchange and physical partitions.
func (a *assembler) ddlPartitions(ddl *event.DDLEvent) ([]int32, error) {
	if a.router == nil || ddl.SchemaName == "" || ddl.TableName == "" ||
		(ddl.BlockedTables != nil && ddl.BlockedTables.InfluenceType != event.InfluenceTypeNormal) {
		return nil, nil
	}
	names := []event.SchemaTableName{{SchemaName: ddl.SchemaName, TableName: ddl.TableName}}
	if ddl.ExtraTableName != "" {
		names = append(names, event.SchemaTableName{SchemaName: ddl.ExtraSchemaName, TableName: ddl.ExtraTableName})
	}
	names = append(names, ddl.BlockedTableNames...)
	for _, info := range ddl.MultipleTableInfos {
		if info != nil {
			names = append(names, event.SchemaTableName{SchemaName: info.GetSchemaName(), TableName: info.GetTableName()})
		}
	}
	var partitions []int32
	for _, name := range names {
		if name.SchemaName == "" || name.TableName == "" {
			return nil, nil
		}
		generator := a.router.GetPartitionGenerator(name.SchemaName, name.TableName)
		switch generator.(type) {
		case *routing.TablePartitionGenerator:
		default:
			// Index/column values can place a table's rows in any partition.
			return nil, nil
		}
		table := common.NewTableInfo4Decoder(name.SchemaName, &timodel.TableInfo{Name: ast.NewCIStr(name.TableName)})
		partitionID, _, err := generator.GeneratePartitionIndexAndKey(nil, int32(len(a.partitions)), table, ddl.GetCommitTs())
		if err != nil {
			return nil, err
		}
		if !slices.Contains(partitions, partitionID) {
			partitions = append(partitions, partitionID)
		}
	}
	return partitions, nil
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

type storageTableKey struct {
	schema    string
	table     string
	partition int64
}

type storageAssembly struct {
	selectors       *columnselector.ColumnSelectors
	schemas         map[cloudstorage.SchemaPathKey]*common.TableInfo
	tableIDs        map[storageTableKey]int64
	tableWatermarks map[int64]uint64
	nextTableID     int64
	current         *readData
	tableID         int64
	decoder         codecCommon.Decoder
	sortBeforeWrite bool
	groupReady      bool
}

func newAssembler(ctx context.Context, upstreamURI *url.URL, timezone string, replicaConfig *config.ReplicaConfig, reader reader, memory *memoryUsage) (a *assembler, err error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	protocolName := upstreamURI.Query().Get(config.ProtocolKey)
	switch source {
	case sourcePulsar:
		protocolName = cmp.Or(protocolName, putil.GetOrZero(replicaConfig.Sink.Protocol), "canal-json")
	case sourceStorage:
		protocolName = putil.GetOrZero(replicaConfig.Sink.Protocol)
	}
	protocol, err := config.ParseSinkProtocolFromString(protocolName)
	if err != nil {
		return nil, err
	}
	switch source {
	case sourceKafka:
		switch protocol {
		case config.ProtocolOpen, config.ProtocolCanalJSON, config.ProtocolAvro, config.ProtocolSimple, config.ProtocolDebezium, config.ProtocolDebeziumAvro:
		default:
			return nil, errors.ErrKafkaInvalidConfig.FastGenByArgs("unsupported Kafka protocol " + protocol.String())
		}
	case sourcePulsar:
		if protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrPulsarInvalidConfig.FastGenByArgs("Pulsar consumer requires canal-json")
		}
	case sourceStorage:
		if protocol != config.ProtocolCsv && protocol != config.ProtocolCanalJSON {
			return nil, errors.ErrStorageSinkInvalidConfig.FastGenByArgs("Storage consumer requires csv or canal-json")
		}
	}
	codecConfig := codecCommon.NewConfig(protocol)
	if err := codecConfig.Apply(upstreamURI, replicaConfig.Sink); err != nil {
		return nil, err
	}
	if source != sourceStorage {
		switch protocol {
		case config.ProtocolCanalJSON, config.ProtocolDebezium:
			if !codecConfig.EnableTiDBExtension {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension must be true")
			}
		case config.ProtocolAvro, config.ProtocolDebeziumAvro:
			if !codecConfig.EnableTiDBExtension || !codecConfig.AvroEnableWatermark {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("enable-tidb-extension and avro-enable-watermark must be true")
			}
			if codecConfig.AvroConfluentSchemaRegistry == "" {
				return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("schema-registry is required")
			}
		}
	} else if protocol == config.ProtocolCanalJSON {
		codecConfig.EnableTiDBExtension = true
	}
	codecConfig.TimeZone, err = putil.GetTimezone(timezone)
	if err != nil {
		return nil, err
	}
	if (protocol == config.ProtocolDebezium || protocol == config.ProtocolDebeziumAvro) && codecConfig.DebeziumDisableSchema {
		return nil, errors.ErrCodecInvalidConfig.FastGenByArgs("debezium-disable-schema must be false")
	}
	var db *sql.DB
	if source == sourceKafka && upstreamURI.Query().Get("upstream-tidb-dsn") != "" {
		db, err = sql.Open("mysql", upstreamURI.Query().Get("upstream-tidb-dsn"))
		if err != nil {
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "open consumer upstream TiDB")
		}
		defer func() {
			if err != nil {
				_ = db.Close()
			}
		}()
		db.SetMaxOpenConns(10)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(10 * time.Minute)
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err != nil {
			return nil, errors.WrapError(errors.ErrMySQLConnectionError, err, "ping consumer upstream TiDB")
		}
	}
	partitions := make(map[int32]*partition)
	var storage *storageAssembly
	topic := strings.Trim(upstreamURI.Path, "/")
	var router *eventrouter.EventRouter
	if source == sourceKafka {
		router, err = eventrouter.NewEventRouter(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive), topic, false, protocol == config.ProtocolAvro)
		if err != nil {
			return nil, err
		}
	}
	switch reader := reader.(type) {
	case *kafkaReader:
		for partitionID, progress := range reader.progress {
			decoder, err := codec.NewEventDecoder(ctx, int(partitionID), codecConfig, topic, db)
			if err != nil {
				return nil, err
			}
			partitions[partitionID] = &partition{decoder: decoder, progress: progress, schemas: make(map[schemaKey]bool), schemaPointers: make(map[*common.TableInfo]bool)}
		}
	case *pulsarReader:
		// Canal metadata belongs to the logical topic, shared by its partitions.
		decoder, err := codec.NewEventDecoder(ctx, 0, codecConfig, topic, nil)
		if err != nil {
			return nil, err
		}
		partitions[0] = &partition{decoder: decoder, progress: &readProgress{}, schemas: make(map[schemaKey]bool), schemaPointers: make(map[*common.TableInfo]bool)}
	case *storageReader:
		selectors, err := columnselector.New(replicaConfig.Sink, putil.GetOrZero(replicaConfig.CaseSensitive))
		if err != nil {
			return nil, err
		}
		storage = &storageAssembly{
			selectors: selectors, schemas: make(map[cloudstorage.SchemaPathKey]*common.TableInfo),
			tableIDs: make(map[storageTableKey]int64), tableWatermarks: make(map[int64]uint64),
		}
	}
	return &assembler{
		memory: memory, codecConfig: codecConfig, protocol: protocol, source: source, router: router, upstreamDB: db, storage: storage,
		partitions: partitions, orderedDML: source != sourceStorage,
		ddlCopies: make(map[uint64]map[int32][]*ack), mergeRows: protocol == config.ProtocolCsv,
		sortCSVRows: protocol == config.ProtocolCsv && codecConfig.OutputOldValue && codecConfig.IncludeCommitTs,
	}, nil
}

func (a *assembler) next(ctx context.Context, reader reader) (*writeEvent, error) {
	for {
		if err := context.Cause(ctx); err != nil {
			return nil, err
		}
		needsMoreInput := false
		var inputWatermark uint64
		switch reader := reader.(type) {
		case *kafkaReader:
			reader.advanceBoundary()
			watermark, ready := a.globalWatermark()
			inputWatermark = watermark
			if result := a.nextReady(watermark); result != nil {
				return result, nil
			}
			hasControls := slices.ContainsFunc(a.pendingWatermarks, func(control *writeEvent) bool { return control.watermark <= watermark })
			if ready && !a.hasPendingThrough(watermark) && (!a.hasDeliveredWatermark || watermark > a.deliveredWatermark || hasControls) {
				var completed []*ack
				for commitTs, records := range a.ddlCopies {
					if commitTs <= watermark {
						for _, copies := range records {
							completed = append(completed, copies...)
						}
						delete(a.ddlCopies, commitTs)
					}
				}
				a.deliveredWatermark = watermark
				a.hasDeliveredWatermark = true
				result := &writeEvent{watermark: watermark, hasWatermark: true}
				var controls []*writeEvent
				a.pendingWatermarks = slices.DeleteFunc(a.pendingWatermarks, func(control *writeEvent) bool {
					if control.watermark <= watermark {
						controls = append(controls, control)
						return true
					}
					return false
				})
				if len(completed) != 0 || len(controls) != 0 {
					result.onFlush = func() {
						for _, control := range controls {
							control.onFlush()
						}
						for _, record := range completed {
							record.refs.Add(-1)
						}
						a.memory.release(int64(len(completed)) * 128)
					}
				}
				return result, nil
			}
		case *pulsarReader:
			watermark := a.partitions[0].progress.watermark
			if result := a.nextReady(watermark); result != nil {
				return result, nil
			}
			inputWatermark = watermark
			if len(a.pendingWatermarks) != 0 && !a.hasPendingThrough(a.pendingWatermarks[0].watermark) {
				result := a.pendingWatermarks[0]
				a.pendingWatermarks[0] = nil
				a.pendingWatermarks = a.pendingWatermarks[1:]
				return result, nil
			}
		case *storageReader:
			state := a.storage
			if !state.sortBeforeWrite || state.groupReady {
				if result := a.nextReady(math.MaxUint64); result != nil {
					return result, nil
				}
			}
			if state.groupReady {
				state.groupReady = false
				return &writeEvent{tableID: state.tableID, watermark: state.tableWatermarks[state.tableID], hasWatermark: true}, nil
			}
			if state.decoder != nil {
				messageType, hasNext := state.decoder.HasNext()
				if hasNext {
					if messageType != codecCommon.MessageTypeRow {
						continue
					}
					message := state.decoder.NextDMLMessage()
					if message == nil {
						return nil, errors.ErrCodecDecode.FastGenByArgs("Storage decoder returned an empty DML message")
					}
					if !state.current.storage.index.EnableTableAcrossNodes && message.GetCommitTs() < state.tableWatermarks[state.tableID] {
						continue
					}
					state.tableWatermarks[state.tableID] = max(state.tableWatermarks[state.tableID], message.GetCommitTs())
					dml := message.ToDMLEvent()
					if dml == nil || dml.TableInfo == nil {
						return nil, errors.ErrCodecDecode.FastGenByArgs("Storage DML message has no table metadata")
					}
					dml.PhysicalTableID = state.tableID
					if a.protocol == config.ProtocolCanalJSON {
						dml.TableInfo.UpdateTS = state.current.storage.key.TableVersion
					}
					if err := a.queueDML(ctx, dml, []*ack{state.current.record}, nil); err != nil {
						return nil, err
					}
					continue
				}
				// Every derived row now owns a downstream completion reference.
				a.memory.decoded(state.current.record, 256)
				state.decoder = nil
				state.current = nil
				continue
			}
		}
		boundaryPending, err := a.captureBoundary(ctx, reader, inputWatermark)
		if err != nil {
			return nil, err
		}
		if boundaryPending && a.boundary != nil && a.boundary.reached {
			continue
		}
		needsMoreInput = needsMoreInput || boundaryPending || len(a.pendingDDL) != 0 || len(a.ddlCopies) != 0 || len(a.pendingWatermarks) != 0 || (a.storage != nil && len(a.pendingDML) != 0)
		for _, partition := range a.partitions {
			needsMoreInput = needsMoreInput || partition.cachedUnreleased != 0
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
		switch reader := reader.(type) {
		case *kafkaReader, *pulsarReader:
			if err := a.decodeMQ(ctx, data); err != nil {
				return nil, err
			}
		case *storageReader:
			result, err := a.decodeStorage(ctx, data, reader)
			if err != nil || result != nil {
				return result, err
			}
		}
	}
}

func (a *assembler) globalWatermark() (uint64, bool) {
	watermark := uint64(math.MaxUint64)
	for _, partition := range a.partitions {
		if !partition.progress.hasWatermark || partition.cachedUnreleased != 0 {
			return 0, false
		}
		watermark = min(watermark, partition.progress.watermark)
	}
	return watermark, true
}

func (a *assembler) decodeStorage(ctx context.Context, data *readData, reader *storageReader) (*writeEvent, error) {
	state := a.storage
	input := data.storage
	key := input.key
	table := state.schemas[key.SchemaPathKey]
	if table == nil {
		table = data.schema.TableInfo()
		table.UpdateTS = data.schema.TableVersion
		state.schemas[key.SchemaPathKey] = table
	}
	if key.IsSchemaFileDMLPathKey() {
		oldKey := ""
		if data.schema.Type == byte(timodel.ActionRenameTable) {
			statement, err := parser.New().ParseOneStmt(data.schema.Query, "", "")
			if err != nil {
				return nil, errors.WrapError(errors.ErrCodecDecode, err, "parse Storage rename DDL")
			}
			rename, ok := statement.(*ast.RenameTableStmt)
			if !ok || len(rename.TableToTables) == 0 {
				return nil, errors.ErrCodecDecode.FastGenByArgs("Storage rename DDL has no old table")
			}
			old := rename.TableToTables[0].OldTable
			oldKey = common.QuoteSchema(cmp.Or(old.Schema.O, data.schema.Schema), old.Name.O)
		}
		ddl := data.schema.DDLEvent()
		ddl.TableInfo = table
		tableKey := key.GetKey()
		reader.ddlWatermarks[tableKey] = max(reader.ddlWatermarks[tableKey], key.TableVersion)
		if oldKey != "" {
			reader.ddlWatermarks[oldKey] = max(reader.ddlWatermarks[oldKey], key.TableVersion)
		}
		return &writeEvent{ddl: ddl, onFlush: func() { data.record.refs.Add(-1) }}, nil
	}
	idKey := storageTableKey{schema: key.Schema, table: key.Table, partition: key.PartitionNum}
	tableID := state.tableIDs[idKey]
	if tableID == 0 {
		if len(state.tableIDs) >= maxRecords {
			return nil, errors.ErrInternalCheckFailed.FastGenByArgs("Storage table identity cache exceeds its count limit")
		}
		size := int64(len(key.Schema) + len(key.Table) + 128)
		if err := a.memory.reserve(ctx, size); err != nil {
			return nil, err
		}
		state.nextTableID++
		tableID = state.nextTableID
		state.tableIDs[idKey] = tableID
	}
	state.tableID = tableID
	if input.groupEnd {
		if state.sortBeforeWrite {
			slices.SortStableFunc(a.pendingDML, func(a, b *writeEvent) int { return cmp.Compare(a.dml.CommitTs, b.dml.CommitTs) })
		}
		state.groupReady = true
		return nil, nil
	}
	state.current = data
	state.sortBeforeWrite = input.sort
	if a.protocol == config.ProtocolCsv {
		decoder, err := csv.NewDecoderWithColumnSelector(ctx, a.codecConfig, table, data.value, state.selectors.GetForTableInfo(table))
		if err != nil {
			return nil, errors.WrapError(errors.ErrCodecDecode, err, "create Storage CSV decoder")
		}
		state.decoder = decoder
	} else {
		state.decoder = canal.NewTxnDecoder(a.codecConfig)
		state.decoder.AddKeyValue(nil, data.value)
	}
	return nil, nil
}

func (a *assembler) decodeMQ(ctx context.Context, data *readData) error {
	partitionID, retainedBytes, decoderName := data.partition, int64(128), "decoder"
	if a.source == sourcePulsar {
		partitionID, retainedBytes, decoderName = 0, 256, "Pulsar decoder"
	}
	p, ok := a.partitions[partitionID]
	if !ok {
		return errors.ErrInternalCheckFailed.FastGenByArgs("Kafka record belongs to an unknown partition")
	}
	record := data.record
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
				if _, ok := p.decoder.(*simple.Decoder); !ok {
					return errors.ErrCodecDecode.FastGenByArgs(decoderName + " returned an empty DML message")
				}
				record.refs.Add(1)
				p.cachedUnreleased++
				p.progress.needsMoreInput = true
				p.cachedRecords = append(p.cachedRecords, record)
				continue
			}
			if err := a.queueDML(ctx, message.ToDMLEvent(), []*ack{record}, p); err != nil {
				return err
			}
		case codecCommon.MessageTypeDDL:
			if a.protocol == config.ProtocolCanalJSON && a.source != sourcePulsar && data.partition != 0 {
				return errors.ErrCodecDecode.FastGenByArgs("Canal JSON DDL must come from partition 0")
			}
			ddl := p.decoder.NextDDLEvent()
			if ddl == nil || (a.source == sourcePulsar && ddl.Query == "") {
				return errors.ErrCodecDecode.FastGenByArgs(decoderName + " returned an empty DDL event")
			}
			if a.source != sourcePulsar {
				if err := a.trackSchema(ctx, p, ddl.TableInfo); err != nil {
					return err
				}
				for _, info := range ddl.MultipleTableInfos {
					if err := a.trackSchema(ctx, p, info); err != nil {
						return err
					}
				}
			}
			if a.protocol == config.ProtocolCanalJSON {
				key := schemaKey{schema: ddl.SchemaName, table: ddl.TableName, version: ddl.FinishedTs}
				if !p.schemas[key] {
					if err := a.memory.reserve(ctx, 128); err != nil {
						return err
					}
					p.schemas[key] = true
				}
			}
			if decoder, ok := p.decoder.(*simple.Decoder); ok {
				records := slices.Clone(p.cachedRecords)
				for _, message := range decoder.GetCachedMessages() {
					if p.cachedUnreleased == 0 {
						return errors.ErrInternalCheckFailed.FastGenByArgs("Simple Protocol released a DML message without its Kafka record")
					}
					p.cachedUnreleased--
					if err := a.queueDML(ctx, message.ToDMLEvent(), records, p); err != nil {
						return err
					}
				}
				if p.cachedUnreleased == 0 {
					p.progress.needsMoreInput = false
					for _, cached := range p.cachedRecords {
						a.memory.decoded(cached, 128)
					}
					clear(p.cachedRecords)
					p.cachedRecords = nil
				}
			}
			if ddl.Query == "" {
				if a.protocol != config.ProtocolSimple {
					return errors.ErrCodecDecode.FastGenByArgs("DDL query is empty")
				}
				continue
			}
			p.progress.ddlTs = ddl.GetCommitTs()
			if a.source != sourcePulsar && data.partition != 0 {
				// Only partition zero supplies executable DDLs. Other copies wait
				// for downstream progress without matching individual statements.
				if err := a.memory.reserve(ctx, 128); err != nil {
					return err
				}
				record.refs.Add(1)
				copies := a.ddlCopies[ddl.GetCommitTs()]
				if copies == nil {
					copies = make(map[int32][]*ack)
					a.ddlCopies[ddl.GetCommitTs()] = copies
				}
				copies[data.partition] = append(copies[data.partition], record)
				continue
			}
			if err := a.queueDDL(ctx, ddl, record); err != nil {
				return err
			}
			if a.source == sourcePulsar {
				// Equal timestamps retain input order, including split rename jobs.
				slices.SortStableFunc(a.pendingDDL, func(first, second *writeEvent) int {
					return cmp.Compare(first.ddl.GetCommitTs(), second.ddl.GetCommitTs())
				})
			}
		case codecCommon.MessageTypeResolved:
			watermark := p.decoder.NextResolvedEvent()
			if !p.progress.hasWatermark || watermark > p.progress.watermark {
				p.progress.watermark = watermark
				p.progress.hasWatermark = true
			}
			if err := a.memory.reserve(ctx, 128); err != nil {
				return err
			}
			record.refs.Add(1)
			a.pendingWatermarks = append(a.pendingWatermarks, &writeEvent{watermark: watermark, hasWatermark: true, onFlush: func() {
				record.refs.Add(-1)
				a.memory.release(128)
			}})
		default:
			return errors.ErrCodecDecode.FastGenByArgs(decoderName + " returned an unknown message type")
		}
	}
	if !slices.Contains(p.cachedRecords, record) {
		a.memory.decoded(record, retainedBytes)
	} else {
		record.refs.Add(-1)
	}
	return nil
}
