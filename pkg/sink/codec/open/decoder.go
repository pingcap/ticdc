// Copyright 2025 PingCAP, Inc.
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

package open

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

var tableIDAllocator = common.NewTableIDAllocator()

type tableCacheKey struct {
	schema      string
	table       string
	ddlCommitTs uint64
}

type cachedTable struct {
	columns    map[string]column
	info       *commonType.TableInfo
	projection string
}

type decoder struct {
	// Keep old intervals available for delayed decoding of buffered messages.
	tables      map[tableCacheKey]*cachedTable
	ddlCommitTs map[[2]string][]uint64
	keyBytes    []byte
	valueBytes  []byte

	nextKey *messageKey

	storage storeapi.Storage

	config *common.Config

	upstreamTiDB *sql.DB

	idx int
}

// NewDecoder creates a new decoder.
func NewDecoder(
	ctx context.Context, idx int, config *common.Config, db *sql.DB,
) (common.Decoder, error) {
	var (
		externalStorage storeapi.Storage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorageWithDefaultTimeout(ctx, storageURI)
		if err != nil {
			return nil, err
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() {
		if db == nil {
			log.Warn("handle-key-only is enabled, but upstream TiDB is not provided")
		}
	}

	tableIDAllocator.Clean()
	return &decoder{
		idx:          idx,
		config:       config,
		storage:      externalStorage,
		upstreamTiDB: db,
	}, nil
}

// AddKeyValue implements the Decoder interface
func (b *decoder) AddKeyValue(key, value []byte) {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		log.Panic("add key / value to the decoder failed, since it's already set")
	}
	version := binary.BigEndian.Uint64(key[:8])
	if version != batchVersion1 {
		log.Panic("the batch version is not supported", zap.Uint64("version", version))
	}

	b.keyBytes = key[8:]
	b.valueBytes = value
}

func (b *decoder) hasNext() bool {
	keyLen := len(b.keyBytes)
	valueLen := len(b.valueBytes)

	if keyLen > 0 && valueLen > 0 {
		return true
	}

	if keyLen == 0 && valueLen != 0 || keyLen != 0 && valueLen == 0 {
		log.Panic("open-protocol meet invalid data",
			zap.Int("keyLen", keyLen), zap.Int("valueLen", valueLen))
	}

	return false
}

// HasNext implements the Decoder interface
func (b *decoder) HasNext() (common.MessageType, bool) {
	if !b.hasNext() {
		return common.MessageTypeUnknown, false
	}

	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(messageKey)
	msgKey.Decode(key)
	b.nextKey = msgKey
	b.keyBytes = b.keyBytes[keyLen+8:]

	return b.nextKey.Type, true
}

// NextResolvedEvent implements the Decoder interface
func (b *decoder) NextResolvedEvent() uint64 {
	if b.nextKey.Type != common.MessageTypeResolved {
		log.Panic("message type is not watermark", zap.Any("messageType", b.nextKey.Type))
	}
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	// resolved ts event's value part is empty, can be ignored.
	b.valueBytes = nil
	return resolvedTs
}

type messageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

// NextDDLEvent implements the Decoder interface
func (b *decoder) NextDDLEvent() *commonEvent.DDLEvent {
	if b.nextKey.Type != common.MessageTypeDDL {
		log.Panic("message type is not DDL", zap.Any("messageType", b.nextKey.Type))
	}

	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", util.RedactAny(value)), zap.Error(err))
	}

	var m messageDDL
	err = json.Unmarshal(value, &m)
	if err != nil {
		log.Panic("decode message DDL failed", zap.String("data", util.RedactAny(value)), zap.Error(err))
	}

	result := new(commonEvent.DDLEvent)
	result.Query = m.Query
	result.Type = byte(m.Type)
	result.FinishedTs = b.nextKey.Ts
	result.SchemaName = b.nextKey.Schema
	result.TableName = b.nextKey.Table

	// Every partition decoder must record the boundary, even though only
	// partition zero executes the DDL downstream.
	schema, table := result.SchemaName, result.TableName
	switch m.Type {
	case timodel.ActionRenameTable, timodel.ActionRenameTables, timodel.ActionExchangeTablePartition:
		// These DDLs affect multiple names. Start a new cache interval for all
		// tables rather than relying on the single name carried by the message.
		schema, table = "", ""
	}
	b.addDDLCommitTs(schema, table, result.FinishedTs)

	// only the DDL comes from the first partition will be processed.
	if b.idx == 0 {
		tableIDAllocator.AddBlockTableID(result.SchemaName, result.TableName, tableIDAllocator.Allocate(result.SchemaName, result.TableName))
		result.BlockedTables = common.GetBlockedTables(tableIDAllocator, result)
	}

	b.nextKey = nil
	b.valueBytes = nil
	return result
}

// NextDMLMessage implements the Decoder interface
func (b *decoder) NextDMLMessage() *common.DMLMessage {
	if b.nextKey.Type != common.MessageTypeRow {
		log.Panic("message type is not row", zap.Any("messageType", b.nextKey.Type))
	}

	key := *b.nextKey
	value := b.nextDMLValue()
	b.nextKey = nil

	rowType := commonType.RowTypeInsert
	if key.ClaimCheckLocation == "" {
		rowType = b.rowTypeFromDMLValue(value)
	}
	tableID := tableIDAllocator.Allocate(key.Schema, key.Table)
	return common.NewDMLMessage(tableID, key.Schema, key.Table, key.Ts, rowType, func() *commonEvent.DMLEvent {
		return b.decodeDMLMessage(&key, value)
	})
}

func (b *decoder) nextDMLValue() []byte {
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]
	return append([]byte(nil), value...)
}

func (b *decoder) rowTypeFromDMLValue(value []byte) commonType.RowType {
	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", util.RedactAny(value)), zap.Error(err))
	}

	nextRow := new(messageRow)
	nextRow.decode(value)
	return rowTypeFromMessageRow(nextRow)
}

func rowTypeFromMessageRow(row *messageRow) commonType.RowType {
	if len(row.Delete) != 0 {
		return commonType.RowTypeDelete
	}
	if len(row.Update) != 0 && len(row.PreColumns) != 0 {
		return commonType.RowTypeUpdate
	}
	if len(row.Update) != 0 {
		return commonType.RowTypeInsert
	}
	log.Panic("unknown event type")
	return commonType.RowTypeInsert
}

func (b *decoder) decodeDMLMessage(key *messageKey, value []byte) *commonEvent.DMLEvent {
	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", util.RedactAny(value)), zap.Error(err))
	}

	nextRow := new(messageRow)
	nextRow.decode(value)

	ctx := context.Background()
	// claim-check message found
	if key.ClaimCheckLocation != "" {
		return b.assembleEventFromClaimCheckStorage(ctx, key)
	}

	if key.OnlyHandleKey && b.upstreamTiDB != nil {
		return b.assembleHandleKeyOnlyDMLEvent(ctx, key, nextRow)
	}

	return b.assembleDMLEvent(key, nextRow)
}

func buildColumns(
	holder *common.ColumnsHolder, columns map[string]column,
) map[string]column {
	columnsCount := holder.Length()
	for i := 0; i < columnsCount; i++ {
		columnType := holder.Types[i]
		name := columnType.Name()
		if _, ok := columns[name]; ok {
			continue
		}
		var flag uint64
		// todo: we can extract more detailed type information here.
		dataType := strings.ToLower(columnType.DatabaseTypeName())
		if common.IsUnsignedMySQLType(dataType) {
			flag |= unsignedFlag
		}
		if nullable, _ := columnType.Nullable(); nullable {
			flag |= nullableFlag
		}
		columns[name] = column{
			Type:  common.ExtractBasicMySQLType(dataType),
			Flag:  flag,
			Value: holder.Values[i],
		}
	}
	return columns
}

// snapshotColumns uses protocol metadata so snapshot rows have the same chunk
// layout as ordinary messages. Cached columns never retain row values.
func (b *decoder) snapshotColumns(ctx context.Context, key *messageKey, ts uint64, conditions map[string]interface{}, columns map[string]column) map[string]column {
	schema, table := key.Schema, key.Table
	cacheKey := b.tableCacheKey(key)
	cached := b.tables[cacheKey]
	var holder *common.ColumnsHolder
	if cached == nil {
		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, ts, schema, table, conditions)
		columns = buildColumns(holder, columns)
		b.queryTableInfo(key, &messageRow{Update: columns})
		cached = b.tables[cacheKey]
	}
	// On a cold cache, ENUM/SET names need another query to obtain numeric values.
	if holder == nil || slices.ContainsFunc(cached.info.GetColumns(), func(col *timodel.ColumnInfo) bool {
		return col.GetType() == mysql.TypeEnum || col.GetType() == mysql.TypeSet
	}) {
		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, ts, schema, table, conditions, cached.projection)
	}
	result := maps.Clone(cached.columns)
	for i, typ := range holder.Types {
		name := typ.Name()
		col := result[name]
		col.Value = holder.Values[i]
		result[name] = col
	}
	return result
}

func (b *decoder) assembleHandleKeyOnlyDMLEvent(ctx context.Context, key *messageKey, row *messageRow) *commonEvent.DMLEvent {
	commitTs := key.Ts
	conditions := make(map[string]interface{}, 1)
	if len(row.Delete) != 0 {
		for name, col := range row.Delete {
			conditions[name] = col.Value
		}
		row.Delete = b.snapshotColumns(ctx, key, commitTs-1, conditions, row.Delete)
	} else if len(row.PreColumns) != 0 {
		for name, col := range row.PreColumns {
			conditions[name] = col.Value
		}
		row.PreColumns = b.snapshotColumns(ctx, key, commitTs-1, conditions, row.PreColumns)
		row.Update = b.snapshotColumns(ctx, key, commitTs, conditions, row.Update)
	} else if len(row.Update) != 0 {
		for name, col := range row.Update {
			conditions[name] = col.Value
		}
		row.Update = b.snapshotColumns(ctx, key, commitTs, conditions, row.Update)
	} else {
		log.Panic("unknown event type")
	}
	key.OnlyHandleKey = false
	return b.assembleDMLEvent(key, row)
}

func (b *decoder) assembleEventFromClaimCheckStorage(ctx context.Context, key *messageKey) *commonEvent.DMLEvent {
	_, claimCheckFileName := filepath.Split(key.ClaimCheckLocation)
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		log.Panic("read claim check file failed", zap.String("fileName", claimCheckFileName), zap.Error(err))
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		log.Panic("unmarshal claim check message failed", zap.String("data", util.RedactAny(data)), zap.Error(err))
	}

	version := binary.BigEndian.Uint64(claimCheckM.Key[:8])
	if version != batchVersion1 {
		log.Panic("the batch version is not supported", zap.Uint64("version", version))
	}

	encodedKey := claimCheckM.Key[8:]
	keyLen := binary.BigEndian.Uint64(encodedKey[:8])
	encodedKey = encodedKey[8 : keyLen+8]
	msgKey := new(messageKey)
	msgKey.Decode(encodedKey)

	valueLen := binary.BigEndian.Uint64(claimCheckM.Value[:8])
	value := claimCheckM.Value[8 : valueLen+8]
	value, err = common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress large message failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", util.RedactAny(value)), zap.Error(err))
	}

	rowMsg := new(messageRow)
	rowMsg.decode(value)

	return b.assembleDMLEvent(msgKey, rowMsg)
}

// DML at the DDL commit timestamp is flushed before that DDL.
func (b *decoder) tableCacheKey(key *messageKey) tableCacheKey {
	var version uint64
	for _, name := range [][2]string{{"", ""}, {key.Schema, ""}, {key.Schema, key.Table}} {
		timestamps := b.ddlCommitTs[name]
		i, _ := slices.BinarySearch(timestamps, key.Ts)
		if i > 0 {
			version = max(version, timestamps[i-1])
		}
	}
	return tableCacheKey{schema: key.Schema, table: key.Table, ddlCommitTs: version}
}

func (b *decoder) addDDLCommitTs(schema, table string, ts uint64) {
	if ts == 0 {
		return
	}
	if b.ddlCommitTs == nil {
		b.ddlCommitTs = make(map[[2]string][]uint64)
	}
	name := [2]string{schema, table}
	timestamps := b.ddlCommitTs[name]
	i, exists := slices.BinarySearch(timestamps, ts)
	if !exists {
		b.ddlCommitTs[name] = slices.Insert(timestamps, i, ts)
	}
}

func (b *decoder) queryTableInfo(key *messageKey, value *messageRow) *commonType.TableInfo {
	columns := value.Update
	if columns == nil {
		columns = value.Delete
	}
	tableKey := b.tableCacheKey(key)
	if cached := b.tables[tableKey]; cached != nil {
		id := cached.info.TableName.TableID
		key.Partition = &id
		return cached.info
	}
	info := b.newTableInfo(key, value)
	// A delete can contain only handle columns. Use a complete row to seed
	// the cache; a cold delete must not define the layout of later inserts.
	if value.Update == nil {
		return info
	}
	metadata := make(map[string]column, len(columns))
	for name, col := range columns {
		col.Value = nil
		col.WhereHandle = nil
		metadata[name] = col
	}
	names := make([]string, 0, len(columns))
	for _, col := range info.GetColumns() {
		name := commonType.QuoteName(col.Name.O)
		if col.GetType() == mysql.TypeEnum || col.GetType() == mysql.TypeSet {
			name = "CAST(" + name + " AS UNSIGNED) AS " + name
		}
		names = append(names, name)
	}
	if b.tables == nil {
		b.tables = make(map[tableCacheKey]*cachedTable)
	}
	b.tables[tableKey] = &cachedTable{columns: metadata, info: info, projection: strings.Join(names, ",")}
	return info
}

func (b *decoder) newTableInfo(key *messageKey, value *messageRow) *commonType.TableInfo {
	physicalTableID := tableIDAllocator.Allocate(key.Schema, key.Table)
	tableIDAllocator.AddBlockTableID(key.Schema, key.Table, physicalTableID)
	key.Partition = &physicalTableID
	tableInfo := new(timodel.TableInfo)
	tableInfo.ID = *key.Partition
	tableInfo.Name = ast.NewCIStr(key.Table)

	var rawColumns map[string]column
	if value.Update != nil {
		rawColumns = value.Update
	} else if value.Delete != nil {
		rawColumns = value.Delete
	}
	columns := newTiColumns(rawColumns)
	tableInfo.Columns = columns
	tableInfo.Indices = newTiIndices(columns)
	commonType.SetHandleKeyFlags(tableInfo)
	return commonType.NewTableInfo4Decoder(key.Schema, tableInfo)
}

func newTiColumns(rawColumns map[string]column) []*timodel.ColumnInfo {
	result := make([]*timodel.ColumnInfo, 0)
	var nextColumnID int64

	type columnPair struct {
		column column
		name   string
	}

	rawColumnList := make([]columnPair, 0, len(rawColumns))
	for name, raw := range rawColumns {
		rawColumnList = append(rawColumnList, columnPair{
			column: raw,
			name:   name,
		})
	}
	slices.SortFunc(rawColumnList, func(a, b columnPair) int {
		return strings.Compare(a.name, b.name)
	})

	for _, pair := range rawColumnList {
		name := pair.name
		raw := pair.column
		col := new(timodel.ColumnInfo)
		col.ID = nextColumnID
		col.Offset = int(nextColumnID)
		col.Name = ast.NewCIStr(name)
		col.FieldType = *types.NewFieldType(raw.Type)

		// Open protocol omits virtual generated columns and carries no index
		// membership. A non-primary composite key may therefore be incomplete
		// (for example, UNIQUE(a, virtual_b) carries only a). Do not promote
		// its remaining handle columns to a primary key.
		isComposite := raw.Flag&multipleKeyFlag != 0
		if isPrimary(raw.Flag) || (isHandle(raw.Flag) && !isComposite) {
			col.AddFlag(mysql.PriKeyFlag)
			col.AddFlag(mysql.UniqueKeyFlag)
			col.AddFlag(mysql.NotNullFlag)
		}
		if isUnsigned(raw.Flag) {
			col.AddFlag(mysql.UnsignedFlag)
		}
		if isBinary(raw.Flag) {
			col.AddFlag(mysql.BinaryFlag)
			col.SetCharset("binary")
			col.SetCollate("binary")
		}
		if !isNullable(raw.Flag) {
			col.AddFlag(mysql.NotNullFlag)
		}
		if isGenerated(raw.Flag) {
			col.AddFlag(mysql.GeneratedColumnFlag)
			col.GeneratedExprString = "holder" // just to make it not empty
			col.GeneratedStored = true
		}
		// A column belonging to a composite unique index is not individually
		// unique. Only infer single-column unique indexes from unambiguous flags.
		if isUnique(raw.Flag) && !isComposite {
			col.AddFlag(mysql.UniqueKeyFlag)
		}

		switch col.GetType() {
		case mysql.TypeVarchar, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if !mysql.HasBinaryFlag(col.GetFlag()) {
				col.SetCharset("utf8mb4")
				col.SetCollate("utf8mb4_bin")
			}
		case mysql.TypeDuration:
			col.SetDecimal(tiTypes.MaxFsp)
		case mysql.TypeEnum, mysql.TypeSet:
			col.SetCharset("utf8mb4")
			col.SetCollate("utf8mb4_bin")
			elements := common.ExtractElements("")
			col.SetElems(elements)
		}
		nextColumnID++
		result = append(result, col)
	}
	return result
}

func newTiIndices(columns []*timodel.ColumnInfo) []*timodel.IndexInfo {
	indices := make([]*timodel.IndexInfo, 0, 2)
	primaryColumns := make([]*timodel.IndexColumn, 0, 2)
	multiColumns := make([]*timodel.IndexColumn, 0, 2)

	for idx, col := range columns {
		switch {
		case mysql.HasPriKeyFlag(col.GetFlag()):
			primaryColumns = append(primaryColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
		case mysql.HasUniKeyFlag(col.GetFlag()):
			indices = append(indices, &timodel.IndexInfo{
				ID:   2 + int64(len(indices)), // Reserve ID 1 for the primary index.
				Name: ast.NewCIStr(col.Name.O + "_idx"),
				Columns: []*timodel.IndexColumn{{
					Name:   col.Name,
					Offset: idx,
				}},
				Unique: true,
				State:  timodel.StatePublic,
			})
		}
		if mysql.HasMultipleKeyFlag(col.GetFlag()) {
			multiColumns = append(multiColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
		}
	}
	// One primary index over the whole key: one index per primary key column
	// would make the row locator look like a set of single column keys.
	if len(primaryColumns) != 0 {
		indices = append(indices, &timodel.IndexInfo{
			ID:      1,
			Name:    ast.NewCIStr("primary"),
			Columns: primaryColumns,
			Primary: true,
			Unique:  true,
			State:   timodel.StatePublic,
		})
	}
	// if there are multiple multi-column indices, consider as one.
	if len(multiColumns) != 0 {
		indices = append(indices, &timodel.IndexInfo{
			ID:      2 + int64(len(indices)),
			Name:    ast.NewCIStr("multi_idx"),
			Columns: multiColumns,
			Unique:  false,
			State:   timodel.StatePublic,
		})
	}
	return indices
}

func (b *decoder) assembleDMLEvent(key *messageKey, value *messageRow) *commonEvent.DMLEvent {
	tableInfo := b.queryTableInfo(key, value)
	result := new(commonEvent.DMLEvent)
	result.TableInfo = tableInfo
	result.PhysicalTableID = tableInfo.TableName.TableID
	result.StartTs = key.Ts
	result.CommitTs = key.Ts
	result.Length++

	chk := chunk.NewChunkFromPoolWithCapacity(tableInfo.GetFieldSlice(), chunk.InitialCapacity)
	result.AddPostFlushFunc(func() {
		chk.Destroy(chunk.InitialCapacity, tableInfo.GetFieldSlice())
	})
	columns := tableInfo.GetColumns()
	if len(value.Delete) != 0 {
		data := collectAllColumnsValue(value.Delete, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonType.RowTypeDelete)
	} else if len(value.Update) != 0 && len(value.PreColumns) != 0 {
		previous := collectAllColumnsValue(value.PreColumns, columns)
		data := collectAllColumnsValue(value.Update, columns)
		for k, v := range data {
			if _, ok := previous[k]; !ok {
				previous[k] = v
			}
		}
		common.AppendRow2Chunk(previous, columns, chk)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonType.RowTypeUpdate, commonType.RowTypeUpdate)
	} else if len(value.Update) != 0 {
		// if OpenOutputOldValue is false, the PreColumns is nil, but Update is not nil,
		// we will treat it as an insert event.
		data := collectAllColumnsValue(value.Update, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonType.RowTypeInsert)
	} else {
		log.Panic("unknown event type")
	}
	result.Rows = chk
	return result
}

func collectAllColumnsValue(data map[string]column, columns []*timodel.ColumnInfo) map[string]any {
	result := make(map[string]any, len(data))
	for _, col := range columns {
		raw, ok := data[col.Name.O]
		if !ok {
			continue
		}
		result[col.Name.O] = formatColumn(raw, col.FieldType).Value
	}
	return result
}

// formatColumn formats a codec column.
func formatColumn(c column, ft types.FieldType) column {
	if c.Value == nil {
		return c
	}
	var err error
	switch c.Type {
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		var data []byte
		switch v := c.Value.(type) {
		case []uint8:
			data = v
		case string:
			if isBinary(c.Flag) {
				v, err = strconv.Unquote("\"" + v + "\"")
				if err != nil {
					log.Panic("invalid column value, please report a bug", zap.String("value", util.RedactAny(data)), zap.Error(err))
				}
			}
			data = []byte(v)
		default:
			log.Panic("invalid column value, please report a bug", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value = data
	case mysql.TypeTinyBlob, mysql.TypeMediumBlob,
		mysql.TypeLongBlob, mysql.TypeBlob:
		var data []byte
		switch v := c.Value.(type) {
		case []uint8:
			data = v
		case string:
			data, err = base64.StdEncoding.DecodeString(v)
		default:
			log.Panic("invalid column value, please report a bug", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.String("col", util.RedactAny(c)), zap.Error(err))
		}
		c.Value = data
	case mysql.TypeFloat, mysql.TypeDouble:
		var data float64
		switch v := c.Value.(type) {
		case []uint8:
			data, err = strconv.ParseFloat(string(v), 64)
		case json.Number:
			data, err = v.Float64()
		case float64:
			data = v
		case float32:
			data = float64(v)
		default:
			log.Panic("invalid column value, please report a bug", zap.String("col", util.RedactAny(c)), zap.Any("type", v))
		}
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.String("col", util.RedactAny(c)), zap.Error(err))
		}
		c.Value = data
		if c.Type == mysql.TypeFloat {
			c.Value = float32(data)
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeInt24:
		var data string
		switch v := c.Value.(type) {
		case json.Number:
			data = string(v)
		case []uint8:
			data = string(v)
		case int64, uint64:
			data = fmt.Sprintf("%v", v)
		default:
			log.Panic("invalid column value, please report a bug", zap.String("col", util.RedactAny(c)), zap.Any("type", v))
		}
		if isUnsigned(c.Flag) {
			c.Value, err = strconv.ParseUint(data, 10, 64)
		} else {
			c.Value, err = strconv.ParseInt(data, 10, 64)
		}
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.String("col", util.RedactAny(c)), zap.Error(err))
		}
	case mysql.TypeYear:
		var value int64
		switch v := c.Value.(type) {
		case json.Number:
			value, err = v.Int64()
		case int64:
			value = v
		case uint64:
			value = int64(v)
		default:
			log.Panic("invalid column value for year", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		if err != nil {
			log.Panic("invalid column value for year", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
		c.Value = value
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		var data string
		switch v := c.Value.(type) {
		case []uint8:
			data = string(v)
		case string:
			data = v
		default:
			log.Panic("invalid column value for date / datetime / timestamp", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value, err = tiTypes.ParseTime(tiTypes.DefaultStmtNoWarningContext, data, ft.GetType(), tiTypes.MaxFsp)
		if err != nil {
			log.Panic("invalid column value for date / datetime / timestamp", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
	// todo: shall we also convert timezone for the mysql.TypeTimestamp ?
	//if mysqlType == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
	//	err = t.ConvertTimeZone(time.UTC, decoder.loc)
	//	if err != nil {
	//		log.Panic("convert timestamp to local timezone failed", zap.Any("rawValue", rawValue), zap.Error(err))
	//	}
	//}
	case mysql.TypeDuration:
		var data string
		switch v := c.Value.(type) {
		case []uint8:
			data = string(v)
		case string:
			data = v
		default:
			log.Panic("invalid column value for duration", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value, _, err = tiTypes.ParseDuration(tiTypes.DefaultStmtNoWarningContext, data, tiTypes.MaxFsp)
		if err != nil {
			log.Panic("invalid column value for duration", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
	case mysql.TypeBit:
		var intVal uint64
		switch v := c.Value.(type) {
		case []uint8:
			intVal = common.MustBinaryLiteralToInt(v)
		case json.Number:
			a, err := v.Int64()
			if err != nil {
				log.Panic("invalid column value for the bit type", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
			}
			intVal = uint64(a)
		default:
			log.Panic("invalid column value for the bit type", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value = tiTypes.NewBinaryLiteralFromUint(intVal, -1)
	case mysql.TypeEnum, mysql.TypeSet:
		var value uint64
		switch v := c.Value.(type) {
		case json.Number:
			value, err = strconv.ParseUint(string(v), 10, 64)
		case []uint8:
			value, err = strconv.ParseUint(string(v), 10, 64)
		case uint64:
			value = v
		case int64:
			value = uint64(v)
		default:
			log.Panic("invalid column value for enum/set", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		if err != nil {
			log.Panic("invalid column value for enum/set", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
		// The MySQL sink accesses only the numeric value; Open carries no elements.
		if c.Type == mysql.TypeEnum {
			c.Value = tiTypes.Enum{Value: value}
		} else {
			c.Value = tiTypes.Set{Value: value}
		}
	case mysql.TypeJSON:
		var data string
		switch v := c.Value.(type) {
		case []uint8:
			data = string(v)
		case string:
			data = v
		default:
			log.Panic("invalid column value for JSON", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value, err = tiTypes.ParseBinaryJSONFromString(data)
		if err != nil {
			log.Panic("invalid column value for json", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
	case mysql.TypeNewDecimal:
		var data []byte
		switch v := c.Value.(type) {
		case []uint8:
			data = v
		case string:
			data = []byte(v)
		default:
			log.Panic("invalid column value for decimal", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		dec := new(tiTypes.MyDecimal)
		err = dec.FromString(data)
		if err != nil {
			log.Panic("invalid column value for decimal", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
		c.Value = dec
	case mysql.TypeTiDBVectorFloat32:
		var data string
		switch v := c.Value.(type) {
		case []uint8:
			data = string(v)
		case string:
			data = v
		default:
			log.Panic("invalid column value for vector float32", zap.String("value", util.RedactAny(c.Value)), zap.Any("type", v))
		}
		c.Value, err = tiTypes.ParseVectorFloat32(data)
		if err != nil {
			log.Panic("invalid column value for vector float32", zap.String("value", util.RedactAny(c.Value)), zap.Error(err))
		}
	default:
		log.Panic("unknown data type found", zap.Any("type", c.Type), zap.String("value", util.RedactAny(c.Value)))
	}
	return c
}
