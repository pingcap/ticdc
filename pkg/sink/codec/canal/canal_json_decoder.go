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
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/log"
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
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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

	tableInfoCache     map[tableKey]*model.TableInfo
	partitionInfoCache map[tableKey]*timodel.PartitionInfo

	tableIDAllocator *common.FakeTableIDAllocator
}

// NewBatchDecoder return a decoder for canal-json
func NewBatchDecoder(
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
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if codecConfig.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &canalJSONDecoder{
		config:             codecConfig,
		decoder:            newBufferedJSONDecoder(),
		storage:            externalStorage,
		upstreamTiDB:       db,
		bytesDecoder:       charmap.ISO8859_1.NewDecoder(),
		tableInfoCache:     make(map[tableKey]*model.TableInfo),
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

func (b *canalJSONDecoder) assembleClaimCheckRowChangedEvent(ctx context.Context, claimCheckLocation string) (*model.RowChangedEvent, error) {
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
		if utils.IsBinaryMySQLType(mysqlType) {
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
) (*model.RowChangedEvent, error) {
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

func setColumnInfos(
	tableInfo *timodel.TableInfo,
	rawColumns map[string]interface{},
	mysqlType map[string]string,
	pkNames map[string]struct{},
) {
	mockColumnID := int64(100)
	for name := range rawColumns {
		columnInfo := new(timodel.ColumnInfo)
		columnInfo.ID = mockColumnID
		columnInfo.Name = pmodel.NewCIStr(name)
		if utils.IsBinaryMySQLType(mysqlType[name]) {
			columnInfo.AddFlag(mysql.BinaryFlag)
		}
		if _, isPK := pkNames[name]; isPK {
			columnInfo.AddFlag(mysql.PriKeyFlag)
		}
		tableInfo.Columns = append(tableInfo.Columns, columnInfo)
		mockColumnID++
	}
}

func setIndexes(
	tableInfo *timodel.TableInfo,
	pkNames map[string]struct{},
) {
	indexColumns := make([]*timodel.IndexColumn, 0, len(pkNames))
	for idx, col := range tableInfo.Columns {
		name := col.Name.O
		if _, ok := pkNames[name]; ok {
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   pmodel.NewCIStr(name),
				Offset: idx,
			})
		}
	}
	indexInfo := &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("primary"),
		Columns: indexColumns,
		Unique:  true,
		Primary: true,
	}
	tableInfo.Indices = append(tableInfo.Indices, indexInfo)
}

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeRow {
		return nil, cerror.ErrCanalDecodeFailed.
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

	result, err := b.canalJSONMessage2RowChange()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// NextDDLEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.msg == nil || b.msg.messageType() != common.MessageTypeDDL {
		return nil, cerror.ErrCanalDecodeFailed.
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
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("MessageTypeResolved tidb extension not found")
	}
	return withExtensionEvent.Extensions.WatermarkTs, nil
}

func canalJSONMessage2DDLEvent(msg canalJSONMessageInterface) *model.DDLEvent {
	result := new(model.DDLEvent)
	// we lost the startTs from kafka message
	result.CommitTs = msg.getCommitTs()

	result.TableInfo = new(model.TableInfo)
	result.TableInfo.TableName = model.TableName{
		Schema: *msg.getSchema(),
		Table:  *msg.getTable(),
	}

	// we lost DDL type from canal json format, only got the DDL SQL.
	result.Query = msg.getQuery()

	// hack the DDL Type to be compatible with MySQL sink's logic
	// see https://github.com/pingcap/tiflow/blob/0578db337d/cdc/sink/mysql.go#L362-L370
	result.Type = getDDLActionType(result.Query)
	return result
}

func canalJSONColumnMap2RowChangeColumns(
	cols map[string]interface{},
	mysqlType map[string]string,
	tableInfo *model.TableInfo,
) ([]*model.ColumnData, error) {
	result := make([]*model.ColumnData, 0, len(cols))
	for _, columnInfo := range tableInfo.Columns {
		name := columnInfo.Name.O
		value, ok := cols[name]
		if !ok {
			continue
		}
		mysqlTypeStr, ok := mysqlType[name]
		if !ok {
			// this should not happen, else we have to check encoding for mysqlType.
			return nil, errors.ErrCodecDecode.GenWithStack(
				"mysql type does not found, column: %+v, mysqlType: %+v", name, mysqlType)
		}
		col := canalJSONFormatColumn(columnInfo.ID, value, mysqlTypeStr)
		result = append(result, col)
	}
	return result, nil
}

func canalJSONFormatColumn(columnID int64, value interface{}, mysqlTypeStr string) *model.ColumnData {
	mysqlType := utils.ExtractBasicMySQLType(mysqlTypeStr)
	result := &model.ColumnData{
		ColumnID: columnID,
		Value:    value,
	}
	if result.Value == nil {
		return result
	}

	data, ok := value.(string)
	if !ok {
		log.Panic("canal-json encoded message should have type in `string`")
	}

	var err error
	if utils.IsBinaryMySQLType(mysqlTypeStr) {
		// when encoding the `JavaSQLTypeBLOB`, use `ISO8859_1` decoder, now reverse it back.
		encoder := charmap.ISO8859_1.NewEncoder()
		value, err = encoder.String(data)
		if err != nil {
			log.Panic("invalid column value, please report a bug", zap.Any("col", result), zap.Error(err))
		}
		result.Value = value
		return result
	}

	switch mysqlType {
	case mysql.TypeBit, mysql.TypeSet:
		value, err = strconv.ParseUint(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeLong, mysql.TypeInt24, mysql.TypeYear:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for int", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeEnum:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeLonglong:
		value, err = strconv.ParseInt(data, 10, 64)
		if err != nil {
			value, err = strconv.ParseUint(data, 10, 64)
			if err != nil {
				log.Panic("invalid column value for bigint", zap.Any("col", result), zap.Error(err))
			}
		}
	case mysql.TypeFloat:
		value, err = strconv.ParseFloat(data, 32)
		if err != nil {
			log.Panic("invalid column value for float", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeDouble:
		value, err = strconv.ParseFloat(data, 64)
		if err != nil {
			log.Panic("invalid column value for double", zap.Any("col", result), zap.Error(err))
		}
	case mysql.TypeTiDBVectorFloat32:
	}

	result.Value = value
	return result
}

func (b *canalJSONDecoder) canalJSONMessage2RowChange() (*model.RowChangedEvent, error) {
	msg := b.msg
	result := new(model.RowChangedEvent)
	result.TableInfo = b.queryTableInfo(msg)
	result.CommitTs = msg.getCommitTs()

	mysqlType := msg.getMySQLType()
	var err error
	if msg.eventType() == canal.EventType_DELETE {
		// for `DELETE` event, `data` contain the old data, set it as the `PreColumns`
		result.PreColumns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		err = b.setPhysicalTableID(result)
		if err != nil {
			return nil, err
		}
		return result, nil
	}

	// for `INSERT` and `UPDATE`, `data` contain fresh data, set it as the `Columns`
	result.Columns, err = canalJSONColumnMap2RowChangeColumns(msg.getData(), mysqlType, result.TableInfo)
	if err != nil {
		return nil, err
	}

	// for `UPDATE`, `old` contain old data, set it as the `PreColumns`
	if msg.eventType() == canal.EventType_UPDATE {
		preCols, err := canalJSONColumnMap2RowChangeColumns(msg.getOld(), mysqlType, result.TableInfo)
		if err != nil {
			return nil, err
		}
		if len(preCols) < len(result.Columns) {
			newPreCols := make([]*model.ColumnData, 0, len(preCols))
			j := 0
			// Columns are ordered by name
			for _, col := range result.Columns {
				if j < len(preCols) && col.ColumnID == preCols[j].ColumnID {
					newPreCols = append(newPreCols, preCols[j])
					j += 1
				} else {
					newPreCols = append(newPreCols, col)
				}
			}
			preCols = newPreCols
		}
		result.PreColumns = preCols
		if len(preCols) != len(result.Columns) {
			log.Panic("column count mismatch", zap.Any("preCols", preCols), zap.Any("cols", result.Columns))
		}
	}
	err = b.setPhysicalTableID(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (b *canalJSONDecoder) queryTableInfo(msg canalJSONMessageInterface) *model.TableInfo {
	schema := *msg.getSchema()
	table := *msg.getTable()
	cacheKey := tableKey{
		schema: schema,
		table:  table,
	}
	tableInfo, ok := b.tableInfoCache[cacheKey]
	if !ok {
		partitionInfo := b.partitionInfoCache[cacheKey]
		tableInfo = newTableInfo(msg, partitionInfo)
		tableInfo.ID = b.tableIDAllocator.AllocateTableID(schema, table)
		if tableInfo.Partition != nil {
			for idx, partition := range tableInfo.Partition.Definitions {
				partitionID := b.tableIDAllocator.AllocatePartitionID(schema, table, partition.Name.O)
				tableInfo.Partition.Definitions[idx].ID = partitionID
			}
		}
		b.tableInfoCache[cacheKey] = tableInfo
	}
	return tableInfo
}

func newTableInfo(msg canalJSONMessageInterface, partitionInfo *timodel.PartitionInfo) *model.TableInfo {
	schema := *msg.getSchema()
	table := *msg.getTable()
	tidbTableInfo := &timodel.TableInfo{}
	tidbTableInfo.Name = pmodel.NewCIStr(table)

	rawColumns := msg.getData()
	pkNames := msg.pkNameSet()
	mysqlType := msg.getMySQLType()
	setColumnInfos(tidbTableInfo, rawColumns, mysqlType, pkNames)
	setIndexes(tidbTableInfo, pkNames)
	tidbTableInfo.Partition = partitionInfo
	return model.WrapTableInfo(100, schema, 1000, tidbTableInfo)
}

func (b *canalJSONDecoder) setPhysicalTableID(event *model.RowChangedEvent) error {
	if event.TableInfo.Partition == nil {
		event.PhysicalTableID = event.TableInfo.ID
		return nil
	}
	switch event.TableInfo.Partition.Type {
	case pmodel.PartitionTypeRange:
		targetColumnID := event.TableInfo.ForceGetColumnIDByName(strings.ReplaceAll(event.TableInfo.Partition.Expr, "`", ""))
		columns := event.Columns
		if columns == nil {
			columns = event.PreColumns
		}
		var columnValue string
		for _, col := range columns {
			if col.ColumnID == targetColumnID {
				columnValue = model.ColumnValueString(col.Value)
				break
			}
		}
		for _, partition := range event.TableInfo.Partition.Definitions {
			lessThan := partition.LessThan[0]
			if lessThan == "MAXVALUE" {
				event.PhysicalTableID = partition.ID
				return nil
			}
			if len(columnValue) < len(lessThan) {
				event.PhysicalTableID = partition.ID
				return nil
			}
			if strings.Compare(columnValue, lessThan) == -1 {
				event.PhysicalTableID = partition.ID
				return nil
			}
		}
		return fmt.Errorf("cannot found partition for column value %s", columnValue)
	// todo: support following rule if meet the corresponding workload
	case pmodel.PartitionTypeHash:
		targetColumnID := event.TableInfo.ForceGetColumnIDByName(strings.ReplaceAll(event.TableInfo.Partition.Expr, "`", ""))
		columns := event.Columns
		if columns == nil {
			columns = event.PreColumns
		}
		var columnValue int64
		for _, col := range columns {
			if col.ColumnID == targetColumnID {
				columnValue = col.Value.(int64)
				break
			}
		}
		result := columnValue % int64(len(event.TableInfo.Partition.Definitions))
		partitionID := event.TableInfo.GetPartitionInfo().Definitions[result].ID
		event.PhysicalTableID = partitionID
		return nil
	case pmodel.PartitionTypeKey:
	case pmodel.PartitionTypeList:
	case pmodel.PartitionTypeNone:
	default:
	}
	return fmt.Errorf("manually set partition id for partition type %s not supported yet", event.TableInfo.Partition.Type)
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
