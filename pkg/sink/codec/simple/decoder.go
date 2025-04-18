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

package simple

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// decoder implement the RowEventDecoder interface
type decoder struct {
	config *common.Config

	marshaller marshaller

	upstreamTiDB *sql.DB
	storage      storage.ExternalStorage

	value []byte
	msg   *message
	memo  TableInfoProvider

	// cachedMessages is used to store the messages which does not have received corresponding table info yet.
	cachedMessages *list.List
	// CachedRowChangedEvents are events just decoded from the cachedMessages
	CachedRowChangedEvents []*commonEvent.DMLEvent
}

// NewDecoder returns a new Decoder
func NewDecoder(ctx context.Context, config *common.Config, db *sql.DB) (common.RowEventDecoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, errors.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	m, err := newMarshaller(config)
	return &decoder{
		config:     config,
		marshaller: m,

		storage:      externalStorage,
		upstreamTiDB: db,

		memo:           newMemoryTableInfoProvider(),
		cachedMessages: list.New(),
	}, errors.Trace(err)
}

// AddKeyValue add the received key and values to the Decoder,
func (d *decoder) AddKeyValue(_, value []byte) (err error) {
	if d.value != nil {
		return errors.ErrCodecDecode.GenWithStack(
			"Decoder value already exists, not consumed yet")
	}
	d.value, err = common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	return err
}

// HasNext returns whether there is any event need to be consumed
func (d *decoder) HasNext() (common.MessageType, bool, error) {
	if d.value == nil {
		return common.MessageTypeUnknown, false, nil
	}

	m := new(message)
	err := d.marshaller.Unmarshal(d.value, m)
	if err != nil {
		return common.MessageTypeUnknown, false, errors.WrapError(errors.ErrDecodeFailed, err)
	}
	d.msg = m
	d.value = nil

	if d.msg.Data != nil || d.msg.Old != nil {
		return common.MessageTypeRow, true, nil
	}

	if m.Type == MessageTypeWatermark {
		return common.MessageTypeResolved, true, nil
	}

	return common.MessageTypeDDL, true, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if d.msg.Type != MessageTypeWatermark {
		return 0, errors.ErrCodecDecode.GenWithStack(
			"not found resolved event message")
	}

	ts := d.msg.CommitTs
	d.msg = nil

	return ts, nil
}

// NextDMLEvent returns the next dml event if exists
func (d *decoder) NextDMLEvent() (*commonEvent.DMLEvent, error) {
	if d.msg == nil || (d.msg.Data == nil && d.msg.Old == nil) {
		return nil, errors.ErrCodecDecode.GenWithStack(
			"invalid row changed event message")
	}

	if d.msg.ClaimCheckLocation != "" {
		return d.assembleClaimCheckRowChangedEvent(d.msg.ClaimCheckLocation)
	}

	if d.msg.HandleKeyOnly {
		return d.assembleHandleKeyOnlyRowChangedEvent(d.msg)
	}

	tableInfo := d.memo.Read(d.msg.Schema, d.msg.Table, d.msg.SchemaVersion)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", d.msg.Schema),
			zap.String("table", d.msg.Table),
			zap.Uint64("version", d.msg.SchemaVersion))
		d.cachedMessages.PushBack(d.msg)
		d.msg = nil
		return nil, nil
	}

	event, err := buildDMLEvent(d.msg, tableInfo, d.config.EnableRowChecksum, d.upstreamTiDB)
	d.msg = nil

	log.Debug("row changed event assembled", zap.Any("event", event))
	return event, err
}

func (d *decoder) assembleClaimCheckRowChangedEvent(claimCheckLocation string) (*commonEvent.DMLEvent, error) {
	_, claimCheckFileName := filepath.Split(claimCheckLocation)
	data, err := d.storage.ReadFile(context.Background(), claimCheckFileName)
	if err != nil {
		return nil, err
	}

	if !d.config.LargeMessageHandle.ClaimCheckRawValue {
		claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
		if err != nil {
			return nil, err
		}
		data = claimCheckM.Value
	}

	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, data)
	if err != nil {
		return nil, err
	}

	m := new(message)
	err = d.marshaller.Unmarshal(value, m)
	if err != nil {
		return nil, err
	}
	d.msg = m
	return d.NextDMLEvent()
}

func (d *decoder) assembleHandleKeyOnlyRowChangedEvent(m *message) (*commonEvent.DMLEvent, error) {
	tableInfo := d.memo.Read(m.Schema, m.Table, m.SchemaVersion)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", d.msg.Schema),
			zap.String("table", d.msg.Table),
			zap.Uint64("version", d.msg.SchemaVersion))
		d.cachedMessages.PushBack(d.msg)
		d.msg = nil
		return nil, nil
	}

	fieldTypeMap := make(map[string]*types.FieldType, len(tableInfo.GetColumns()))
	for _, col := range tableInfo.GetColumns() {
		fieldTypeMap[col.Name.O] = &col.FieldType
	}

	result := &message{
		Version:       defaultVersion,
		Schema:        m.Schema,
		Table:         m.Table,
		TableID:       m.TableID,
		Type:          m.Type,
		CommitTs:      m.CommitTs,
		SchemaVersion: m.SchemaVersion,
	}

	ctx := context.Background()
	timezone := common.MustQueryTimezone(ctx, d.upstreamTiDB)
	switch m.Type {
	case DMLTypeInsert:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Schema, m.Table, m.Data)
		result.Data = d.buildData(holder, fieldTypeMap, timezone)
	case DMLTypeUpdate:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Schema, m.Table, m.Data)
		result.Data = d.buildData(holder, fieldTypeMap, timezone)

		holder = common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Schema, m.Table, m.Old)
		result.Old = d.buildData(holder, fieldTypeMap, timezone)
	case DMLTypeDelete:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Schema, m.Table, m.Old)
		result.Old = d.buildData(holder, fieldTypeMap, timezone)
	}

	d.msg = result
	return d.NextDMLEvent()
}

func (d *decoder) buildData(
	holder *common.ColumnsHolder, fieldTypeMap map[string]*types.FieldType, timezone string,
) map[string]interface{} {
	columnsCount := holder.Length()
	result := make(map[string]interface{}, columnsCount)

	for i := 0; i < columnsCount; i++ {
		col := holder.Types[i]
		value := holder.Values[i]

		fieldType := fieldTypeMap[col.Name()]
		result[col.Name()] = parseValue(value, fieldType, timezone)
	}
	return result
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*commonEvent.DDLEvent, error) {
	if d.msg == nil {
		return nil, errors.ErrCodecDecode.GenWithStack(
			"no message found when decode DDL event")
	}
	ddl := buidDDLEvent(d.msg)
	d.msg = nil

	d.memo.Write(ddl.TableInfo)
	d.memo.Write(ddl.MultipleTableInfos[1])

	for ele := d.cachedMessages.Front(); ele != nil; {
		d.msg = ele.Value.(*message)
		event, err := d.NextDMLEvent()
		if err != nil {
			return nil, err
		}
		d.CachedRowChangedEvents = append(d.CachedRowChangedEvents, event)

		next := ele.Next()
		d.cachedMessages.Remove(ele)
		ele = next
	}
	return ddl, nil
}

// GetCachedEvents returns the cached events
func (d *decoder) GetCachedEvents() []*commonEvent.DMLEvent {
	result := d.CachedRowChangedEvents
	d.CachedRowChangedEvents = nil
	return result
}

// TableInfoProvider is used to store and read table info
// It works like a schema cache when consuming simple protocol messages
// It will store multiple versions of table info for a table
// The table info which has the exact (schema, table, version) will be returned when reading
type TableInfoProvider interface {
	Write(info *commonType.TableInfo)
	Read(schema, table string, version uint64) *commonType.TableInfo
}

type memoryTableInfoProvider struct {
	memo map[tableSchemaKey]*commonType.TableInfo
}

func newMemoryTableInfoProvider() *memoryTableInfoProvider {
	return &memoryTableInfoProvider{
		memo: make(map[tableSchemaKey]*commonType.TableInfo),
	}
}

func (m *memoryTableInfoProvider) Write(info *commonType.TableInfo) {
	if info == nil || info.TableName.Schema == "" || info.TableName.Table == "" {
		return
	}
	key := tableSchemaKey{
		schema:  info.TableName.Schema,
		table:   info.TableName.Table,
		version: info.UpdateTS(),
	}

	_, ok := m.memo[key]
	if ok {
		log.Debug("table info not stored, since it already exists",
			zap.String("schema", info.TableName.Schema),
			zap.String("table", info.TableName.Table),
			zap.Uint64("version", info.UpdateTS()))
		return
	}

	m.memo[key] = info
	log.Info("table info stored",
		zap.String("schema", info.TableName.Schema),
		zap.String("table", info.TableName.Table),
		zap.Uint64("version", info.UpdateTS()))
}

// Read returns the table info with the exact (schema, table, version)
// Note: It's a blocking call, it will wait until the table info is stored
func (m *memoryTableInfoProvider) Read(schema, table string, version uint64) *commonType.TableInfo {
	key := tableSchemaKey{
		schema:  schema,
		table:   table,
		version: version,
	}
	return m.memo[key]
}

type tableSchemaKey struct {
	schema  string
	table   string
	version uint64
}

// newTiColumnInfo uses columnSchema and IndexSchema to construct a tidb column info.
func newTiColumnInfo(
	column *columnSchema, colID int64, indexes []*IndexSchema,
) *timodel.ColumnInfo {
	col := new(timodel.ColumnInfo)
	col.ID = colID
	col.Name = pmodel.NewCIStr(column.Name)
	col.FieldType = *types.NewFieldType(ptypes.StrToType(column.DataType.MySQLType))
	col.SetCharset(column.DataType.Charset)
	col.SetCollate(column.DataType.Collate)
	if column.DataType.Unsigned {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if column.DataType.Zerofill {
		col.AddFlag(mysql.ZerofillFlag)
	}
	col.SetFlen(column.DataType.Length)
	col.SetDecimal(column.DataType.Decimal)
	col.SetElems(column.DataType.Elements)
	if column.DataType.Charset == charset.CollationBin {
		switch col.GetType() {
		case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob,
			mysql.TypeDuration, mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime,
			mysql.TypeString, mysql.TypeVarchar,
			mysql.TypeTiDBVectorFloat32, mysql.TypeJSON:
			col.AddFlag(mysql.BinaryFlag)
		}
	}
	if !column.Nullable {
		col.AddFlag(mysql.NotNullFlag)
	}
	defaultValue := column.Default
	if defaultValue != nil && col.GetType() == mysql.TypeBit {
		switch v := defaultValue.(type) {
		case float64:
			byteSize := (col.GetFlen() + 7) >> 3
			defaultValue = types.NewBinaryLiteralFromUint(uint64(v), byteSize)
			defaultValue = defaultValue.(types.BinaryLiteral).ToString()
		default:
		}
	}
	for _, index := range indexes {
		for _, name := range index.Columns {
			if name == column.Name {
				if index.Primary {
					col.AddFlag(mysql.PriKeyFlag)
				} else if index.Unique {
					if index.Columns[0] == name {
						// Only the first column can be set
						// if unique index has multi columns,
						// the flag should be MultipleKeyFlag.
						// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
						if len(index.Columns) > 1 {
							col.AddFlag(mysql.MultipleKeyFlag)
						} else {
							col.AddFlag(mysql.UniqueKeyFlag)
						}
					}
				} else {
					col.AddFlag(mysql.MultipleKeyFlag)
				}
			}
		}
	}
	err := col.SetDefaultValue(defaultValue)
	if err != nil {
		log.Panic("set default value failed", zap.Any("column", col), zap.Any("default", defaultValue))
	}
	return col
}

// newTiIndexInfo convert IndexSchema to a tidb index info.
func newTiIndexInfo(indexSchema *IndexSchema, columns []*timodel.ColumnInfo) *timodel.IndexInfo {
	indexColumns := make([]*timodel.IndexColumn, len(indexSchema.Columns))
	for i, col := range indexSchema.Columns {
		var offset int
		for idx, column := range columns {
			if column.Name.O == col {
				offset = idx
				break
			}
		}
		indexColumns[i] = &timodel.IndexColumn{
			Name:   pmodel.NewCIStr(col),
			Offset: offset,
		}
	}
	return &timodel.IndexInfo{
		ID:      1,
		Name:    pmodel.NewCIStr(indexSchema.Name),
		Columns: indexColumns,
		Unique:  indexSchema.Unique,
		Primary: indexSchema.Primary,
	}
}

func newTableInfo(m *TableSchema) *commonType.TableInfo {
	tidbTableInfo := &timodel.TableInfo{}
	if m != nil {
		tidbTableInfo.ID = m.TableID
		tidbTableInfo.Name = pmodel.NewCIStr(m.Table)
		tidbTableInfo.UpdateTS = m.Version
		nextMockID := int64(1)
		for _, col := range m.Columns {
			tiCol := newTiColumnInfo(col, nextMockID, m.Indexes)
			nextMockID += 1
			tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
		}
		for _, idx := range m.Indexes {
			index := newTiIndexInfo(idx, tidbTableInfo.Columns)
			tidbTableInfo.Indices = append(tidbTableInfo.Indices, index)
		}
	}
	return commonType.NewTableInfo4Decoder(m.Schema, tidbTableInfo)
}

func buidDDLEvent(msg *message) *commonEvent.DDLEvent {
	var (
		tableInfo    *commonType.TableInfo
		preTableInfo *commonType.TableInfo
	)
	tableInfo = newTableInfo(msg.TableSchema)
	if msg.PreTableSchema != nil {
		preTableInfo = newTableInfo(msg.PreTableSchema)
	}
	return &commonEvent.DDLEvent{
		FinishedTs:         msg.CommitTs,
		Query:              msg.SQL,
		TableInfo:          tableInfo,
		MultipleTableInfos: []*commonType.TableInfo{preTableInfo, tableInfo},
	}
}

func parseValue(
	value interface{}, ft *types.FieldType, location string,
) interface{} {
	if value == nil {
		return nil
	}
	var err error
	switch ft.GetType() {
	case mysql.TypeBit:
		switch v := value.(type) {
		case []uint8:
			value = common.MustBinaryLiteralToInt(v)
		default:
		}
	case mysql.TypeTimestamp:
		var ts string
		switch v := value.(type) {
		case string:
			ts = v
		// the timestamp value maybe []uint8 if it's queried from upstream TiDB.
		case []uint8:
			ts = string(v)
		}
		return map[string]interface{}{
			"location": location,
			"value":    ts,
		}
	case mysql.TypeEnum:
		switch v := value.(type) {
		case []uint8:
			data := string(v)
			var enum types.Enum
			enum, err = types.ParseEnumName(ft.GetElems(), data, ft.GetCollate())
			value = enum.Value
		}
	case mysql.TypeSet:
		switch v := value.(type) {
		case []uint8:
			data := string(v)
			var set types.Set
			set, err = types.ParseSetName(ft.GetElems(), data, ft.GetCollate())
			value = set.Value
		}
	default:
	}
	if err != nil {
		log.Panic("parse enum / set name failed",
			zap.Any("elems", ft.GetElems()), zap.Any("name", value), zap.Error(err))
	}
	var result string
	switch v := value.(type) {
	case int64:
		result = strconv.FormatInt(v, 10)
	case uint64:
		result = strconv.FormatUint(v, 10)
	case float32:
		result = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		result = strconv.FormatFloat(v, 'f', -1, 64)
	case string:
		result = v
	case []byte:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			result = base64.StdEncoding.EncodeToString(v)
		} else {
			result = string(v)
		}
	case types.VectorFloat32:
		result = v.String()
	default:
		result = fmt.Sprintf("%v", v)
	}
	return result
}

func buildDMLEvent(msg *message, tableInfo *commonType.TableInfo, enableRowChecksum bool, db *sql.DB) (*commonEvent.DMLEvent, error) {
	result := &commonEvent.DMLEvent{
		CommitTs:        msg.CommitTs,
		PhysicalTableID: msg.TableID,
		TableInfo:       tableInfo,
	}

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	columns := tableInfo.GetColumns()
	switch msg.Type {
	case DMLTypeDelete:
		data := formatAllColumnsValue(msg.Old, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
	case DMLTypeInsert:
		data := formatAllColumnsValue(msg.Data, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
	case DMLTypeUpdate:
		previous := formatAllColumnsValue(msg.Old, columns)
		data := formatAllColumnsValue(msg.Data, columns)
		for k, v := range data {
			if _, ok := previous[k]; !ok {
				previous[k] = v
			}
		}
		common.AppendRow2Chunk(previous, columns, chk)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
	default:
		log.Panic("unknown event type for the DML event", zap.Any("eventType", msg.Type))
	}
	result.Rows = chk

	// TODO: enableRowChecksum
	// if enableRowChecksum && msg.Checksum != nil {
	// 	result.Checksum = &integrity.Checksum{
	// 		Current:   msg.Checksum.Current,
	// 		Previous:  msg.Checksum.Previous,
	// 		Corrupted: msg.Checksum.Corrupted,
	// 		Version:   msg.Checksum.Version,
	// 	}

	// 	err := common.VerifyChecksum(result, db)
	// 	if err != nil || msg.Checksum.Corrupted {
	// 		log.Warn("consumer detect checksum corrupted",
	// 			zap.String("schema", msg.Schema), zap.String("table", msg.Table), zap.Error(err))
	// 		return nil, cerror.ErrDecodeFailed.GenWithStackByArgs("checksum corrupted")

	// 	}
	// }

	return result, nil
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

// formatValue formats the value according to the field type
// both avro and json
func formatValue(value any, ft types.FieldType) any {
	if value == nil {
		return nil
	}
	var err error
	switch ft.GetType() {
	case mysql.TypeBit:
		v, err := strconv.ParseUint(value.(string), 10, 64)
		if err != nil {
			log.Panic("invalid column value for bit", zap.Any("value", value), zap.Error(err))
		}
		value = types.NewBinaryLiteralFromUint(v, -1)
	case mysql.TypeTimestamp:
		v := value.(map[string]interface{})["value"]
		value, err = types.ParseTime(types.DefaultStmtNoWarningContext, v.(string), ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Panic("invalid column value for time", zap.Any("value", value), zap.Error(err))
		}
	case mysql.TypeEnum:
		var v uint64
		switch val := value.(type) {
		case string:
			v, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				log.Panic("invalid column value for enum", zap.Any("value", value), zap.Error(err))
			}
		case int64:
			v = uint64(val)
		}
		value, err = types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			log.Panic("invalid column value for enum", zap.Any("value", value), zap.Error(err))
		}
	case mysql.TypeSet:
		var v uint64
		switch val := value.(type) {
		case string:
			v, err = strconv.ParseUint(val, 10, 64)
			if err != nil {
				log.Panic("invalid column value for set", zap.Any("value", value), zap.Error(err))
			}
		case int64:
			v = uint64(val)
		}
		value, err = types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			log.Panic("invalid column value for set", zap.Any("value", value), zap.Error(err))
		}
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob,
		mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		switch val := value.(type) {
		case string:
			if mysql.HasBinaryFlag(ft.GetFlag()) {
				value, err = base64.StdEncoding.DecodeString(val)
				if err != nil {
					log.Panic("invalid column value for binary char", zap.Any("value", value), zap.Error(err))
				}
			} else {
				value = []byte(val)
			}
		}
	case mysql.TypeLonglong:
		switch val := value.(type) {
		case map[string]interface{}:
			value = uint64(val["value"].(int64))
		case string:
			if mysql.HasUnsignedFlag(ft.GetFlag()) {
				value, err = strconv.ParseUint(val, 10, 64)
			} else {
				value, err = strconv.ParseInt(val, 10, 64)
			}
			if err != nil {
				log.Panic("cannot parse int64 value from string", zap.Any("value", value), zap.Error(err))
			}
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
		var v int64
		switch val := value.(type) {
		case string:
			v, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Panic("cannot parse int64 value from string", zap.Any("value", value), zap.Error(err))
			}
		}
		if mysql.HasUnsignedFlag(ft.GetFlag()) {
			value = uint64(v)
		} else {
			value = v
		}
	case mysql.TypeYear:
		switch val := value.(type) {
		case string:
			value, err = strconv.ParseInt(val, 10, 64)
			if err != nil {
				log.Panic("cannot parse int64 value from string", zap.Any("value", value), zap.Error(err))
			}
		}
	case mysql.TypeFloat:
		switch val := value.(type) {
		case string:
			var v float64
			v, err = strconv.ParseFloat(val, 32)
			if err != nil {
				log.Panic("cannot parse float32 value from string", zap.Any("value", value), zap.Error(err))
			}
			value = float32(v)
		}
	case mysql.TypeDouble:
		switch val := value.(type) {
		case string:
			value, err = strconv.ParseFloat(val, 64)
			if err != nil {
				log.Panic("cannot parse float64 value from string", zap.Any("value", value), zap.Error(err))
			}
		}
	case mysql.TypeJSON:
		value, err = types.ParseBinaryJSONFromString(value.(string))
		if err != nil {
			log.Warn("invalid column value for json. Use zero json instead", zap.Any("value", value), zap.Error(err))
		}
	case mysql.TypeNewDecimal:
		result := new(types.MyDecimal)
		err = result.FromString([]byte(value.(string)))
		if err != nil {
			log.Panic("invalid column value for decimal", zap.Any("value", value), zap.Error(err))
		}
		// workaround the decimal `digitInt` field incorrect problem.
		bin, err := result.ToBin(ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert decimal to binary failed", zap.Any("value", value), zap.Error(err))
		}
		_, err = result.FromBin(bin, ft.GetFlen(), ft.GetDecimal())
		if err != nil {
			log.Panic("convert binary to decimal failed", zap.Any("value", value), zap.Error(err))
		}
		value = result
	case mysql.TypeDuration:
		value, _, err = types.ParseDuration(types.DefaultStmtNoWarningContext, value.(string), ft.GetDecimal())
		if err != nil {
			log.Warn("invalid column value for duration. Use zero value instead", zap.Any("value", value))
		}
	case mysql.TypeDate, mysql.TypeDatetime:
		value, err = types.ParseTime(types.DefaultStmtNoWarningContext, value.(string), ft.GetType(), ft.GetDecimal())
		if err != nil {
			log.Warn("invalid column value for time. Use zero value instead", zap.Any("value", value), zap.Error(err))
		}
	case mysql.TypeTiDBVectorFloat32:
		value, err = types.ParseVectorFloat32(value.(string))
		if err != nil {
			log.Warn("cannot parse vector32 value from string. Use zero value instead", zap.Any("value", value), zap.Error(err))
		}
	default:
	}
	return value
}
