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
	"encoding/binary"
	"encoding/json"
	"golang.org/x/text/encoding/charmap"
	"path/filepath"
	"strings"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey   *messageKey
	nextEvent *commonEvent.DMLEvent

	storage storage.ExternalStorage

	config *common.Config

	upstreamTiDB *sql.DB

	tableInfoCache   map[tableKey]*commonType.TableInfo
	tableIDAllocator *common.FakeTableIDAllocator
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder(ctx context.Context, config *common.Config, db *sql.DB) (common.RowEventDecoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, err
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() {
		if db == nil {
			log.Warn("handle-key-only is enabled, but upstream TiDB is not provided")
		}
	}

	return &BatchDecoder{
		config:           config,
		storage:          externalStorage,
		upstreamTiDB:     db,
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *BatchDecoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	if version != batchVersion1 {
		return errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	b.keyBytes = key[8:]
	b.valueBytes = value
	return nil
}

func (b *BatchDecoder) hasNext() bool {
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

func (b *BatchDecoder) decodeNextKey() {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(messageKey)
	msgKey.Decode(key)
	b.nextKey = msgKey

	b.keyBytes = b.keyBytes[keyLen+8:]
}

// HasNext implements the RowEventDecoder interface
func (b *BatchDecoder) HasNext() (common.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	b.decodeNextKey()

	switch b.nextKey.Type {
	case common.MessageTypeResolved, common.MessageTypeDDL:
		return b.nextKey.Type, true, nil
	default:
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	b.valueBytes = b.valueBytes[valueLen+8:]

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Panic("decompress failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", value), zap.Error(err))
	}
	rowMsg := new(messageRow)
	rowMsg.decode(value)
	b.nextEvent = b.assembleDMLEvent(b.nextKey, rowMsg)

	return common.MessageTypeRow, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey.Type != common.MessageTypeResolved {
		return 0, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
	}
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	// resolved ts event's value part is empty, can be ignored.
	b.valueBytes = nil
	return resolvedTs, nil
}

type messageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

// NextDDLEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if b.nextKey.Type != common.MessageTypeDDL {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}

	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decompress DDL event failed")
	}

	var m messageDDL
	err = json.Unmarshal(value, &m)
	if err != nil {
		log.Panic("decode message DDL failed", zap.Any("data", value), zap.Error(err))
	}

	result := new(commonEvent.DDLEvent)
	result.Query = m.Query
	result.Type = byte(m.Type)
	result.FinishedTs = b.nextKey.Ts
	result.SchemaName = b.nextKey.Schema
	result.TableName = b.nextKey.Table
	b.nextKey = nil
	b.valueBytes = nil
	return result, nil
}

// NextDMLEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if b.nextKey.Type != common.MessageTypeRow {
		return nil, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}

	ctx := context.Background()
	// claim-check message found
	if b.nextKey.ClaimCheckLocation != "" {
		return b.assembleEventFromClaimCheckStorage(ctx)
	}

	event := b.nextEvent
	if b.nextKey.OnlyHandleKey {
		event = b.assembleHandleKeyOnlyEvent(ctx, event)
	}

	b.nextKey = nil
	return event, nil
}

func (b *BatchDecoder) buildColumns(
	holder *common.ColumnsHolder, handleKeyColumns map[string]interface{},
) []*model.Column {
	columnsCount := holder.Length()
	columns := make([]*model.Column, 0, columnsCount)
	for i := 0; i < columnsCount; i++ {
		columnType := holder.Types[i]
		name := columnType.Name()
		mysqlType := types.StrToType(strings.ToLower(columnType.DatabaseTypeName()))

		var value interface{}
		value = holder.Values[i].([]uint8)

		switch mysqlType {
		case mysql.TypeJSON:
			value = string(value.([]uint8))
		case mysql.TypeBit:
			value = common.MustBinaryLiteralToInt(value.([]uint8))
		}

		column := &model.Column{
			Name:  name,
			Type:  mysqlType,
			Value: value,
		}

		if _, ok := handleKeyColumns[name]; ok {
			column.Flag = model.PrimaryKeyFlag | model.HandleKeyFlag
		}
		columns = append(columns, column)
	}
	return columns
}

func (b *BatchDecoder) assembleHandleKeyOnlyEvent(
	ctx context.Context, handleKeyOnlyEvent *commonEvent.DMLEvent,
) *commonEvent.DMLEvent {
	//var (
	//	schema   = handleKeyOnlyEvent.TableInfo.GetSchemaName()
	//	table    = handleKeyOnlyEvent.TableInfo.GetTableName()
	//	commitTs = handleKeyOnlyEvent.CommitTs
	//)
	//
	//tableInfo := handleKeyOnlyEvent.TableInfo
	//if handleKeyOnlyEvent.IsInsert() {
	//	conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
	//	for _, col := range handleKeyOnlyEvent.Columns {
	//		colName := tableInfo.ForceGetColumnName(col.ColumnID)
	//		conditions[colName] = col.Value
	//	}
	//	holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
	//	columns := b.buildColumns(holder, conditions)
	//	indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(columns)
	//	handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, columns, indexColumns)
	//	handleKeyOnlyEvent.Columns = model.Columns2ColumnDatas(columns, handleKeyOnlyEvent.TableInfo)
	//} else if handleKeyOnlyEvent.IsDelete() {
	//	conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
	//	for _, col := range handleKeyOnlyEvent.PreColumns {
	//		colName := tableInfo.ForceGetColumnName(col.ColumnID)
	//		conditions[colName] = col.Value
	//	}
	//	holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
	//	preColumns := b.buildColumns(holder, conditions)
	//	indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(preColumns)
	//	handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, preColumns, indexColumns)
	//	handleKeyOnlyEvent.PreColumns = model.Columns2ColumnDatas(preColumns, handleKeyOnlyEvent.TableInfo)
	//} else if handleKeyOnlyEvent.IsUpdate() {
	//	conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
	//	for _, col := range handleKeyOnlyEvent.Columns {
	//		colName := tableInfo.ForceGetColumnName(col.ColumnID)
	//		conditions[colName] = col.Value
	//	}
	//	holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
	//	columns := b.buildColumns(holder, conditions)
	//	indexColumns := model.GetHandleAndUniqueIndexOffsets4Test(columns)
	//	handleKeyOnlyEvent.TableInfo = model.BuildTableInfo(schema, table, columns, indexColumns)
	//	handleKeyOnlyEvent.Columns = model.Columns2ColumnDatas(columns, handleKeyOnlyEvent.TableInfo)
	//
	//	conditions = make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
	//	for _, col := range handleKeyOnlyEvent.PreColumns {
	//		colName := tableInfo.ForceGetColumnName(col.ColumnID)
	//		conditions[colName] = col.Value
	//	}
	//	holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
	//	preColumns := b.buildColumns(holder, conditions)
	//	handleKeyOnlyEvent.PreColumns = model.Columns2ColumnDatas(preColumns, handleKeyOnlyEvent.TableInfo)
	//}

	return handleKeyOnlyEvent
}

func (b *BatchDecoder) assembleEventFromClaimCheckStorage(ctx context.Context) (*commonEvent.DMLEvent, error) {
	_, claimCheckFileName := filepath.Split(b.nextKey.ClaimCheckLocation)
	b.nextKey = nil
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		return nil, errors.Trace(err)
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, errors.Trace(err)
	}

	version := binary.BigEndian.Uint64(claimCheckM.Key[:8])
	if version != codec.BatchVersion1 {
		return nil, errors.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	key := claimCheckM.Key[8:]
	keyLen := binary.BigEndian.Uint64(key[:8])
	key = key[8 : keyLen+8]
	msgKey := new(messageKey)
	msgKey.Decode(key)

	valueLen := binary.BigEndian.Uint64(claimCheckM.Value[:8])
	value := claimCheckM.Value[8 : valueLen+8]
	value, err = common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return nil, errors.WrapError(errors.ErrOpenProtocolCodecInvalidData, err)
	}

	rowMsg := new(messageRow)
	rowMsg.decode(value)
	event := b.assembleDMLEvent(msgKey, rowMsg)

	return event, nil
}

type tableKey struct {
	schema string
	table  string
}

func (b *BatchDecoder) queryTableInfo(key *messageKey, value *messageRow) *commonType.TableInfo {
	cacheKey := tableKey{
		schema: key.Schema,
		table:  key.Table,
	}
	tableInfo, ok := b.tableInfoCache[cacheKey]
	if !ok {
		tableInfo = b.newTableInfo(key, value)
		b.tableInfoCache[cacheKey] = tableInfo
	}
	return tableInfo
}

func (b *BatchDecoder) newTableInfo(key *messageKey, value *messageRow) *commonType.TableInfo {
	tableInfo := new(timodel.TableInfo)
	tableInfo.ID = b.tableIDAllocator.AllocateTableID(key.Schema, key.Table)
	tableInfo.Name = pmodel.NewCIStr(key.Table)

	var rawColumns map[string]column
	if value.Update != nil {
		rawColumns = value.Update
	} else if value.Delete != nil {
		rawColumns = value.Delete
	}
	columns := newTiColumns(rawColumns)
	tableInfo.Columns = columns
	tableInfo.Indices = newTiIndices(columns)
	return commonType.NewTableInfo4Decoder(key.Schema, tableInfo)
}

func newTiColumns(rawColumns map[string]column) []*timodel.ColumnInfo {
	result := make([]*timodel.ColumnInfo, 0)
	var nextColumnID int64
	for name, raw := range rawColumns {
		col := new(timodel.ColumnInfo)
		col.ID = nextColumnID
		col.Name = pmodel.NewCIStr(name)
		col.FieldType = *types.NewFieldType(raw.Type)
		if raw.Flag.IsUnsigned() {
			col.AddFlag(mysql.BinaryFlag)
			col.SetCharset("binary")
			col.SetCollate("binary")
		}
		if raw.Flag.IsPrimaryKey() {
			col.AddFlag(mysql.PriKeyFlag)
			col.AddFlag(mysql.UniqueKeyFlag)
			col.AddFlag(mysql.NotNullFlag)
		}
		if raw.Flag.IsUnsigned() {
			col.AddFlag(mysql.UnsignedFlag)
		}
		//if strings.HasPrefix(mysqlType, "char") ||
		//	strings.HasPrefix(mysqlType, "varchar") ||
		//	strings.Contains(mysqlType, "text") ||
		//	strings.Contains(mysqlType, "enum") ||
		//	strings.Contains(mysqlType, "set") {
		//	col.SetCharset("utf8mb4")
		//	col.SetCollate("utf8mb4_bin")
		//}
		//flen, decimal := common.ExtractFlenDecimal(mysqlType)
		//col.FieldType.SetFlen(flen)
		//col.FieldType.SetDecimal(decimal)
		//switch basicType {
		//case mysql.TypeEnum, mysql.TypeSet:
		//	elements := common.ExtractElements(mysqlType)
		//	col.SetElems(elements)
		//case mysql.TypeDuration:
		//	decimal = common.ExtractDecimal(mysqlType)
		//	col.FieldType.SetDecimal(decimal)
		//default:
		//}
		nextColumnID++
		result = append(result, col)
	}
	return result
}

func newTiIndices(columns []*timodel.ColumnInfo) []*timodel.IndexInfo {
	indexColumns := make([]*timodel.IndexColumn, 0)
	for idx, col := range columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
		}
	}
	indexInfo := &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		Primary: true,
	}
	result := []*timodel.IndexInfo{indexInfo}
	return result
}

func (b *BatchDecoder) assembleDMLEvent(key *messageKey, value *messageRow) *commonEvent.DMLEvent {
	tableInfo := b.queryTableInfo(key, value)
	result := new(commonEvent.DMLEvent)
	result.Length++
	result.StartTs = key.Ts
	result.ApproximateSize = 0
	result.TableInfo = tableInfo
	result.CommitTs = key.Ts
	if key.Partition != nil {
		result.PhysicalTableID = *key.Partition
		result.TableInfo.TableName.IsPartition = true
	}

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	columns := tableInfo.GetColumns()
	if len(value.Delete) != 0 {
		common.AppendRow2Chunk(value.Delete, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
	} else if len(value.Update) != 0 && len(value.PreColumns) != 0 {
		appendRow2Chunk(value.PreColumns, columns, chk)
		appendRow2Chunk(value.Update, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
	} else if len(value.Update) != 0 {
		appendRow2Chunk(value.Update, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
	} else {
		log.Panic("unknown event type")
	}

	result.Rows = chk
	return result
}

func formatAllColumnsValue(data map[string]any, columns []*timodel.ColumnInfo) map[string]any {
	for _, col := range columns {
		raw, ok := data[col.Name.O]
		if !ok {
			continue
		}
		data[col.Name.O] = formatValue(raw, col.FieldType)
	}
	return data
}

func formatValue(value any, ft types.FieldType) any {
	if value == nil {
		return nil
	}
	rawValue, ok := value.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}
	if mysql.HasBinaryFlag(ft.GetFlag()) {
		// when encoding the `JavaSQLTypeBLOB`, use `ISO8859_1` decoder, now reverse it back.
		result, err := charmap.ISO8859_1.NewEncoder().String(rawValue)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return []byte(result)
	}
	switch ft.GetType() {
	case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeInt24, mysql.TypeShort, mysql.TypeTiny:
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			data, err := strconv.ParseUint(rawValue, 10, 64)
			if err != nil {
				log.Panic("invalid column value for unsigned integer", zap.Any("rawValue", rawValue), zap.Error(err))
			}
			return data
		}
		data, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for integer", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return data
	case mysql.TypeYear:
		result, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for year", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeFloat:
		result, err := strconv.ParseFloat(rawValue, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return float32(result)
	case mysql.TypeDouble:
		result, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString,
		mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		return []byte(rawValue)
	case mysql.TypeNewDecimal:
		result := new(tiTypes.MyDecimal)
		err := result.FromString([]byte(rawValue))
		if err != nil {
			log.Panic("invalid column value for decimal", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		// workaround the decimal `digitInt` field incorrect problem.
		bin, err := result.ToBin(ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert decimal to binary failed", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		_, err = result.FromBin(bin, ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert binary to decimal failed", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		result, err := tiTypes.ParseTime(tiTypes.DefaultStmtNoWarningContext, rawValue, ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for time", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeDuration:
		result, _, err := tiTypes.ParseDuration(tiTypes.DefaultStmtNoWarningContext, rawValue, ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for duration", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeEnum:
		enumValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		result, err := tiTypes.ParseEnumValue(ft.GetElems(), enumValue)
		if err != nil {
			log.Panic("parse enum value failed", zap.Any("rawValue", rawValue),
				zap.Any("enumValue", enumValue), zap.Error(err))
		}
		return result
	case mysql.TypeSet:
		setValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for set", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		result, err := tiTypes.ParseSetValue(ft.GetElems(), setValue)
		if err != nil {
			log.Panic("parse set value failed", zap.Any("rawValue", rawValue),
				zap.Any("setValue", setValue), zap.Error(err))
		}
		return result
	case mysql.TypeBit:
		data, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		byteSize := (ft.GetFlen() + 7) >> 3
		return tiTypes.NewBinaryLiteralFromUint(data, byteSize)
	case mysql.TypeJSON:
		result, err := tiTypes.ParseBinaryJSONFromString(rawValue)
		if err != nil {
			log.Panic("invalid column value for json", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	case mysql.TypeTiDBVectorFloat32:
		result, err := tiTypes.ParseVectorFloat32(rawValue)
		if err != nil {
			log.Panic("cannot parse vector32 value from string", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		return result
	default:
	}
	log.Panic("unknown column type", zap.Any("type", ft.GetType()), zap.Any("rawValue", rawValue))
	return nil
}
