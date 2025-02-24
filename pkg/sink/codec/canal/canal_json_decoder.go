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
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	tiTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/util"
	canal "github.com/pingcap/tiflow/proto/canal"
	"go.uber.org/zap"
	"golang.org/x/text/encoding"
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

	storage storage.ExternalStorage

	upstreamTiDB *sql.DB
	bytesDecoder *encoding.Decoder

	tableInfoCache     map[tableKey]*commonType.TableInfo
	partitionInfoCache map[tableKey]*timodel.PartitionInfo

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
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
	}

	if codecConfig.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, errors.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &canalJSONDecoder{
		config:             codecConfig,
		decoder:            newBufferedJSONDecoder(),
		storage:            externalStorage,
		upstreamTiDB:       db,
		bytesDecoder:       charmap.ISO8859_1.NewDecoder(),
		tableInfoCache:     make(map[tableKey]*commonType.TableInfo),
		partitionInfoCache: make(map[tableKey]*timodel.PartitionInfo),
		tableIDAllocator:   common.NewFakeTableIDAllocator(),
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

func (b *canalJSONDecoder) assembleClaimCheckRowChangedEvent(
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
	return b.NextRowChangedEvent()
}

func (b *canalJSONDecoder) buildData(holder *common.ColumnsHolder) (map[string]interface{}, map[string]string, error) {
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
			rawValue, err := b.bytesDecoder.Bytes(rawValue)
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

func (b *canalJSONDecoder) assembleHandleKeyOnlyRowChangedEvent(
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
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	case "UPDATE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}

		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		old, _, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.Old = []map[string]interface{}{old}
	case "DELETE":
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		data, mysqlType, err := b.buildData(holder)
		if err != nil {
			return nil, err
		}
		result.MySQLType = mysqlType
		result.Data = []map[string]interface{}{data}
	}

	b.msg = result
	return b.NextRowChangedEvent()
}

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextRowChangedEvent() (*commonEvent.DMLEvent, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeRow {
		return nil, errors.ErrCodecDecode.
			GenWithStack("not found row changed event message")
	}

	message, withExtension := b.msg.(*canalJSONMessageWithTiDBExtension)
	if withExtension {
		ctx := context.Background()
		if message.Extensions.OnlyHandleKey {
			return b.assembleHandleKeyOnlyRowChangedEvent(ctx, message)
		}
		if message.Extensions.ClaimCheckLocation != "" {
			return b.assembleClaimCheckRowChangedEvent(ctx, message.Extensions.ClaimCheckLocation)
		}
	}

	result := b.canalJSONMessage2RowChange()
	return result, nil
}

func (b *canalJSONDecoder) canalJSONMessage2RowChange() *commonEvent.DMLEvent {
	msg := b.msg
	tableInfo := b.queryTableInfo(msg)

	result := new(commonEvent.DMLEvent)
	result.Length++                    // todo: set this field correctly
	result.StartTs = msg.getCommitTs() // todo: how to set this correctly?
	result.ApproximateSize = 0
	result.TableInfo = tableInfo
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	result.CommitTs = msg.getCommitTs()

	columns := tableInfo.GetColumns()
	switch msg.eventType() {
	case canal.EventType_DELETE:
		data := msg.getData()
		appendRow2Chunk(data, columns, chk)
	case canal.EventType_INSERT:
		data := msg.getData()
		appendRow2Chunk(data, columns, chk)
	case canal.EventType_UPDATE:
		previous := msg.getOld()
		data := msg.getData()
		for k, v := range data {
			if _, ok := previous[k]; !ok {
				previous[k] = v
			}
		}
		appendRow2Chunk(previous, columns, chk)
		appendRow2Chunk(data, columns, chk)
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
		return nil, errors.ErrDecodeFailed.
			GenWithStack("not found ddl event message")
	}

	result := canalJSONMessage2DDLEvent(b.msg)
	schema := *b.msg.getSchema()
	table := *b.msg.getTable()
	cacheKey := tableKey{
		schema: schema,
		table:  table,
	}
	// if receive a table level DDL, just remove the table info to trigger create a new one.
	if schema != "" && table != "" {
		delete(b.tableInfoCache, cacheKey)
		delete(b.partitionInfoCache, cacheKey)
	}

	stmt, err := parser.New().ParseOneStmt(result.Query, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if v, ok := stmt.(*ast.CreateTableStmt); ok {
		tableInfo, err := ddl.BuildTableInfoFromAST(metabuild.NewContext(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		partitions := tableInfo.GetPartitionInfo()
		if partitions != nil {
			b.partitionInfoCache[cacheKey] = partitions
		}
	}

	return result, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeResolved {
		return 0, errors.ErrDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, errors.ErrDecodeFailed.
			GenWithStack("MessageTypeResolved tidb extension not found")
	}
	return withExtensionEvent.Extensions.WatermarkTs, nil
}

func canalJSONMessage2DDLEvent(msg canalJSONMessageInterface) *commonEvent.DDLEvent {
	result := new(commonEvent.DDLEvent)
	result.Query = msg.getQuery()
	result.BlockedTables = nil // todo: set this
	// todo: getDDLActionType can handle ActionExchangeTablePartition,
	// it's used by the mysql sink.
	result.Type = byte(getDDLActionType(result.Query))
	result.FinishedTs = msg.getCommitTs()
	result.SchemaName = *msg.getSchema()
	result.TableName = *msg.getTable()
	return result
}

func appendCol2Chunk(idx int, raw interface{}, columnInfo *timodel.ColumnInfo, chk *chunk.Chunk) {
	mysqlType := columnInfo.FieldType.GetType()
	if raw == nil {
		chk.AppendNull(idx)
		return
	}
	rawValue, ok := raw.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}
	if mysql.HasBinaryFlag(columnInfo.FieldType.GetFlag()) {
		// when encoding the `JavaSQLTypeBLOB`, use `ISO8859_1` decoder, now reverse it back.
		encoder := charmap.ISO8859_1.NewEncoder()
		rawValue, err := encoder.String(rawValue)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("rawValue", rawValue), zap.Error(err))
		}
	}
	switch mysqlType {
	case mysql.TypeBit, mysql.TypeSet:
		value, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendUint64(idx, value)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		value, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for int", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendInt64(idx, value)
	case mysql.TypeEnum:
		value, err := strconv.ParseInt(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendInt64(idx, value)
	case mysql.TypeLonglong:
		value, err := strconv.ParseInt(rawValue, 10, 64)
		if err == nil {
			chk.AppendInt64(idx, value)
			return
		}
		uValue, err := strconv.ParseUint(rawValue, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bigint", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendUint64(idx, uValue)
	case mysql.TypeFloat:
		value, err := strconv.ParseFloat(rawValue, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendFloat32(idx, float32(value))
	case mysql.TypeDouble:
		value, err := strconv.ParseFloat(rawValue, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendFloat64(idx, value)
	case mysql.TypeTiDBVectorFloat32:
		value, err := tiTypes.ParseVectorFloat32(rawValue)
		if err != nil {
			log.Panic("cannot parse vector32 value from string", zap.Any("rawValue", rawValue), zap.Error(err))
		}
		chk.AppendVectorFloat32(idx, value)
	default:
	}
	log.Panic("unknown column type", zap.Any("mysqlType", mysqlType), zap.Any("rawValue", rawValue))
}

func appendRow2Chunk(data map[string]interface{}, columns []*timodel.ColumnInfo, chk *chunk.Chunk) {
	for idx, col := range columns {
		raw := data[col.Name.O]
		appendCol2Chunk(idx, raw, col, chk)
	}
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
		partitionInfo := b.partitionInfoCache[cacheKey]
		tableID := b.tableIDAllocator.AllocateTableID(schema, table)
		tableInfo = newTableInfo(msg, tableID, partitionInfo)
		//if tableInfo.Partition != nil {
		//	for idx, partition := range tableInfo.Partition.Definitions {
		//		partitionID := b.tableIDAllocator.AllocatePartitionID(schema, table, partition.Name.O)
		//		tableInfo.Partition.Definitions[idx].ID = partitionID
		//	}
		//}
		b.tableInfoCache[cacheKey] = tableInfo
	}
	return tableInfo
}

func newTableInfo(msg canalJSONMessageInterface, tableID int64, partitionInfo *timodel.PartitionInfo) *commonType.TableInfo {
	tableInfo := new(timodel.TableInfo)
	tableInfo.ID = tableID
	tableInfo.Name = pmodel.NewCIStr(*msg.getTable())

	columns := newTiColumns(msg)
	indices := newTiIndices(columns, msg.pkNameSet())
	tableInfo.Columns = columns
	tableInfo.Indices = indices
	return commonType.NewTableInfo4Decoder(*msg.getSchema(), tableInfo)
}

func newTiColumns(msg canalJSONMessageInterface) []*timodel.ColumnInfo {
	var nextColumnID int64
	result := make([]*timodel.ColumnInfo, 0, len(msg.getMySQLType()))
	for name, mysqlType := range msg.getMySQLType() {
		col := new(timodel.ColumnInfo)
		col.ID = nextColumnID
		col.Name = pmodel.NewCIStr(name)
		col.FieldType = *types.NewFieldType(types.StrToType(mysqlType))
		if utils.IsBinaryMySQLType(mysqlType) {
			col.AddFlag(mysql.BinaryFlag)
			col.SetCharset("binary")
		}
		if _, ok := msg.pkNameSet()[name]; ok {
			col.AddFlag(mysql.PriKeyFlag)
			col.AddFlag(mysql.UniqueKeyFlag)
			col.AddFlag(mysql.NotNullFlag)
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

	return timodel.ActionNone
}
