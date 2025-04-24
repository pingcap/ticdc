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

package canal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
	"golang.org/x/text/encoding/charmap"
)

type tableKey struct {
	schema string
	table  string
}

type bufferedJSONDecoder struct {
	buf     *bytes.Buffer
	decoder *json.Decoder
}

func newBufferedJSONDecoder() *bufferedJSONDecoder {
	buf := new(bytes.Buffer)
	decoder := json.NewDecoder(buf)
	return &bufferedJSONDecoder{
		buf:     buf,
		decoder: decoder,
	}
}

// Write writes data to the buffer.
func (b *bufferedJSONDecoder) Write(data []byte) (n int, err error) {
	return b.buf.Write(data)
}

// Decode decodes the buffer into the original message.
func (b *bufferedJSONDecoder) Decode(v interface{}) error {
	return b.decoder.Decode(v)
}

// Len returns the length of the buffer.
func (b *bufferedJSONDecoder) Len() int {
	return b.buf.Len()
}

// Bytes returns the buffer content.
func (b *bufferedJSONDecoder) Bytes() []byte {
	return b.buf.Bytes()
}

// canalJSONDecoder decodes the byte into the original message.
type canalJSONDecoder struct {
	msg     canalJSONMessageInterface
	decoder *bufferedJSONDecoder

	config *common.Config

	storage      storage.ExternalStorage
	upstreamTiDB *sql.DB

	tableInfoCache   map[tableKey]*commonType.TableInfo
	tableIDAllocator *common.FakeTableIDAllocator
}

// NewCanalJSONDecoder return a decoder for canal-json
func NewCanalJSONDecoder(
	ctx context.Context, codecConfig *common.Config, db *sql.DB,
) (common.RowEventDecoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if codecConfig.LargeMessageHandle.EnableClaimCheck() {
		storageURI := codecConfig.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorageWithDefaultTimeout(ctx, storageURI)
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
	}

	if codecConfig.LargeMessageHandle.HandleKeyOnly() {
		if db == nil {
			log.Warn("handle-key-only is enabled, but upstream TiDB is not provided, may in the unit test")
		}
	}

	return &canalJSONDecoder{
		config:           codecConfig,
		decoder:          newBufferedJSONDecoder(),
		storage:          externalStorage,
		upstreamTiDB:     db,
		tableInfoCache:   make(map[tableKey]*commonType.TableInfo),
		tableIDAllocator: common.NewFakeTableIDAllocator(),
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *canalJSONDecoder) AddKeyValue(_, value []byte) error {
	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		log.Error("decompress data failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Error(err))

		return errors.Trace(err)
	}
	if _, err = b.decoder.Write(value); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *canalJSONDecoder) HasNext() (common.MessageType, bool, error) {
	if b.decoder.Len() == 0 {
		return common.MessageTypeUnknown, false, nil
	}

	var msg canalJSONMessageInterface = &JSONMessage{}
	if b.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}

	if err := b.decoder.Decode(msg); err != nil {
		log.Error("canal-json decoder decode failed",
			zap.Error(err), zap.ByteString("data", b.decoder.Bytes()))
		return common.MessageTypeUnknown, false, err
	}
	b.msg = msg
	return b.msg.messageType(), true, nil
}

func (b *canalJSONDecoder) assembleClaimCheckDMLEvent(
	ctx context.Context, claimCheckLocation string,
) (*commonEvent.DMLEvent, error) {
	_, claimCheckFileName := filepath.Split(claimCheckLocation)
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		return nil, err
	}

	if !b.config.LargeMessageHandle.ClaimCheckRawValue {
		claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
		if err != nil {
			return nil, err
		}
		data = claimCheckM.Value
	}

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, data)
	if err != nil {
		return nil, err
	}
	message := &canalJSONMessageWithTiDBExtension{}
	err = json.Unmarshal(value, message)
	if err != nil {
		return nil, err
	}

	b.msg = message
	return b.NextDMLEvent()
}

func buildData(holder *common.ColumnsHolder) (map[string]interface{}, map[string]string, error) {
	columnsCount := holder.Length()
	data := make(map[string]interface{}, columnsCount)
	mysqlTypeMap := make(map[string]string, columnsCount)

	for i := 0; i < columnsCount; i++ {
		t := holder.Types[i]
		name := holder.Types[i].Name()
		mysqlType := strings.ToLower(t.DatabaseTypeName())

		var value string
		rawValue := holder.Values[i].([]uint8)
		if common.IsBinaryMySQLType(mysqlType) {
			rawValue, err := bytesDecoder.Bytes(rawValue)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			value = string(rawValue)
		} else if strings.Contains(mysqlType, "bit") || strings.Contains(mysqlType, "set") {
			bitValue := common.MustBinaryLiteralToInt(rawValue)
			value = strconv.FormatUint(bitValue, 10)
		} else {
			value = string(rawValue)
		}
		mysqlTypeMap[name] = mysqlType
		data[name] = value
	}

	return data, mysqlTypeMap, nil
}

func (b *canalJSONDecoder) assembleHandleKeyOnlyDMLEvent(
	ctx context.Context, message *canalJSONMessageWithTiDBExtension,
) (*commonEvent.DMLEvent, error) {
	var (
		commitTs  = message.Extensions.CommitTs
		schema    = message.Schema
		table     = message.Table
		eventType = message.EventType
	)
	conditions := make(map[string]interface{}, len(message.pkNameSet()))
	for name := range message.pkNameSet() {
		conditions[name] = message.getData()[name]
	}
	result := &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			Schema:  schema,
			Table:   table,
			PKNames: message.PKNames,

			EventType: eventType,
		},
		Extensions: &tidbExtension{
			CommitTs: commitTs,
		},
	}
	switch eventType {
	case "INSERT":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		data, mysqlType, err := buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	case "UPDATE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		data, mysqlType, err := buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}

		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		old, _, err := buildData(holder)
		if err != nil {
			return nil, err
		}
		result.Old = []map[string]interface{}{old}
	case "DELETE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		data, mysqlType, err := buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	}

	b.msg = result
	return b.NextDMLEvent()
}

// NextDMLEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeRow {
		return nil, errors.ErrCodecDecode.GenWithStack("not found row changed event message")
	}

	message, withExtension := b.msg.(*canalJSONMessageWithTiDBExtension)
	if withExtension {
		ctx := context.Background()
		if message.Extensions.OnlyHandleKey && b.upstreamTiDB != nil {
			return b.assembleHandleKeyOnlyDMLEvent(ctx, message)
		}
		if message.Extensions.ClaimCheckLocation != "" {
			return b.assembleClaimCheckDMLEvent(ctx, message.Extensions.ClaimCheckLocation)
		}
	}

	result := b.canalJSONMessage2DMLEvent()
	return result, nil
}

func (b *canalJSONDecoder) canalJSONMessage2DMLEvent() *commonEvent.DMLEvent {
	msg := b.msg
	tableInfo := b.queryTableInfo(msg)

	result := new(commonEvent.DMLEvent)
	result.Length++
	result.StartTs = msg.getCommitTs()
	result.ApproximateSize = 0
	result.TableInfo = tableInfo
	result.CommitTs = msg.getCommitTs()

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	columns := tableInfo.GetColumns()
	switch msg.eventType() {
	case canal.EventType_DELETE:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
		result.Length += 1
	case canal.EventType_INSERT:
		data := formatAllColumnsValue(msg.getData(), columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
		result.Length += 1
	case canal.EventType_UPDATE:
		previous := formatAllColumnsValue(msg.getOld(), columns)
		data := formatAllColumnsValue(msg.getData(), columns)
		for k, v := range data {
			if _, ok := previous[k]; !ok {
				previous[k] = v
			}
		}
		common.AppendRow2Chunk(previous, columns, chk)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.Length += 1
	default:
		log.Panic("unknown event type for the DML event", zap.Any("eventType", msg.eventType()))
	}
	result.Rows = chk
	// todo: may fix this later.
	result.PhysicalTableID = result.TableInfo.TableName.TableID
	return result
}

// NextDDLEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeDDL {
		return nil, errors.ErrDecodeFailed.GenWithStack("not found ddl event message")
	}

	result := canalJSONMessage2DDLEvent(b.msg)
	schemaName := result.GetSchemaName()
	tableName := result.GetTableName()
	// if receive a table level DDL, just remove the table info to trigger create a new one.
	if schemaName != "" && tableName != "" {
		cacheKey := tableKey{
			schema: schemaName,
			table:  tableName,
		}
		delete(b.tableInfoCache, cacheKey)
	}
	return result, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeResolved {
		return 0, errors.ErrDecodeFailed.GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, errors.ErrDecodeFailed.GenWithStack("MessageTypeResolved tidb extension not found")
	}
	return withExtensionEvent.Extensions.WatermarkTs, nil
}

func canalJSONMessage2DDLEvent(msg canalJSONMessageInterface) *commonEvent.DDLEvent {
	result := new(commonEvent.DDLEvent)
	result.Query = msg.getQuery()
	result.BlockedTables = nil // todo: set this
	result.Type = byte(getDDLActionType(result.Query))
	result.FinishedTs = msg.getCommitTs()
	result.SchemaName = *msg.getSchema()
	result.TableName = *msg.getTable()
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
		// todo: shall we also convert timezone for the mysql.TypeTimestamp ?
		//if mysqlType == mysql.TypeTimestamp && decoder.loc != nil && !t.IsZero() {
		//	err = t.ConvertTimeZone(time.UTC, decoder.loc)
		//	if err != nil {
		//		log.Panic("convert timestamp to local timezone failed", zap.Any("rawValue", rawValue), zap.Error(err))
		//	}
		//}
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

func (b *canalJSONDecoder) queryTableInfo(msg canalJSONMessageInterface) *commonType.TableInfo {
	schema := *msg.getSchema()
	table := *msg.getTable()
	cacheKey := tableKey{
		schema: schema,
		table:  table,
	}
	tableInfo, ok := b.tableInfoCache[cacheKey]
	if !ok {
		tableID := b.tableIDAllocator.AllocateTableID(schema, table)
		tableInfo = newTableInfo(msg, tableID)
		b.tableInfoCache[cacheKey] = tableInfo
	}
	return tableInfo
}

func newTableInfo(msg canalJSONMessageInterface, tableID int64) *commonType.TableInfo {
	tableInfo := new(timodel.TableInfo)
	tableInfo.ID = tableID
	tableInfo.Name = pmodel.NewCIStr(*msg.getTable())

	columns := newTiColumns(msg)
	tableInfo.Columns = columns
	tableInfo.Indices = newTiIndices(columns, msg.pkNameSet())
	return commonType.NewTableInfo4Decoder(*msg.getSchema(), tableInfo)
}

func newTiColumns(msg canalJSONMessageInterface) []*timodel.ColumnInfo {
	var nextColumnID int64
	result := make([]*timodel.ColumnInfo, 0, len(msg.getMySQLType()))
	for name, mysqlType := range msg.getMySQLType() {
		col := new(timodel.ColumnInfo)
		col.ID = nextColumnID
		col.Name = pmodel.NewCIStr(name)
		basicType := common.ExtractBasicMySQLType(mysqlType)
		col.FieldType = *types.NewFieldType(basicType)
		if common.IsBinaryMySQLType(mysqlType) {
			col.AddFlag(mysql.BinaryFlag)
			col.SetCharset("binary")
			col.SetCollate("binary")
		}
		if strings.HasPrefix(mysqlType, "char") ||
			strings.HasPrefix(mysqlType, "varchar") ||
			strings.Contains(mysqlType, "text") ||
			strings.Contains(mysqlType, "enum") ||
			strings.Contains(mysqlType, "set") {
			col.SetCharset("utf8mb4")
			col.SetCollate("utf8mb4_bin")
		}

		if _, ok := msg.pkNameSet()[name]; ok {
			col.AddFlag(mysql.PriKeyFlag)
			col.AddFlag(mysql.UniqueKeyFlag)
			col.AddFlag(mysql.NotNullFlag)
		}
		if common.IsUnsignedFlag(mysqlType) {
			col.AddFlag(mysql.UnsignedFlag)
		}
		flen, decimal := common.ExtractFlenDecimal(mysqlType)
		col.FieldType.SetFlen(flen)
		col.FieldType.SetDecimal(decimal)
		switch basicType {
		case mysql.TypeEnum, mysql.TypeSet:
			elements := common.ExtractElements(mysqlType)
			col.SetElems(elements)
		case mysql.TypeDuration:
			decimal = common.ExtractDecimal(mysqlType)
			col.FieldType.SetDecimal(decimal)
		default:
		}
		result = append(result, col)
		nextColumnID++
	}
	return result
}

func newTiIndices(columns []*timodel.ColumnInfo, keys map[string]struct{}) []*timodel.IndexInfo {
	indexColumns := make([]*timodel.IndexColumn, 0, len(keys))
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

// return DDL ActionType by the prefix
// see https://github.com/pingcap/tidb/blob/6dbf2de2f/parser/model/ddl.go#L101-L102
func getDDLActionType(query string) timodel.ActionType {
	query = strings.ToLower(query)
	if strings.HasPrefix(query, "create schema") || strings.HasPrefix(query, "create database") {
		return timodel.ActionCreateSchema
	}
	if strings.HasPrefix(query, "drop schema") || strings.HasPrefix(query, "drop database") {
		return timodel.ActionDropSchema
	}
	if strings.HasPrefix(query, "create table") {
		return timodel.ActionCreateTable
	}
	if strings.Contains(query, "exchange partition") {
		return timodel.ActionExchangeTablePartition
	}
	return timodel.ActionNone
}
