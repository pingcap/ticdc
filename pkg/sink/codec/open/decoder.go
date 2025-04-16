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
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey *messageKey
	nextRow *messageRow

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
		tableInfoCache:   make(map[tableKey]*commonType.TableInfo),
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
	b.nextRow = new(messageRow)
	b.nextRow.decode(value)

	return common.MessageTypeRow, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey.Type != common.MessageTypeResolved {
		return 0, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
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
		return nil, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}

	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]

	value, err := common.Decompress(b.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return nil, errors.ErrOpenProtocolCodecInvalidData.GenWithStack("decompress DDL event failed")
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

	if b.nextKey.OnlyHandleKey && b.upstreamTiDB != nil {
		return b.assembleHandleKeyOnlyDMLEvent(ctx), nil
	}

	result := b.assembleDMLEvent()

	b.nextKey = nil
	b.nextRow = nil
	return result, nil
}

func buildColumns(
	holder *common.ColumnsHolder, handleKeyColumns map[string]column,
) map[string]column {
	columnsCount := holder.Length()
	result := make(map[string]column, columnsCount)
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

		col := column{
			Type:  mysqlType,
			Value: value,
		}
		if _, ok := handleKeyColumns[name]; ok {
			col.Flag |= binaryFlag
		}
		result[name] = col
	}
	return result
}

func (b *BatchDecoder) assembleHandleKeyOnlyDMLEvent(ctx context.Context) *commonEvent.DMLEvent {
	key := b.nextKey
	row := b.nextRow
	var (
		schema   = key.Schema
		table    = key.Table
		commitTs = key.Ts
	)
	conditions := make(map[string]interface{}, 1)
	if len(row.Delete) != 0 {
		for name, col := range row.Delete {
			conditions[name] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		columns := buildColumns(holder, row.Delete)
		b.nextRow.Delete = columns
	} else if len(row.PreColumns) != 0 {
		for name, col := range row.PreColumns {
			conditions[name] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		b.nextRow.PreColumns = buildColumns(holder, row.PreColumns)
		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		b.nextRow.Update = buildColumns(holder, row.PreColumns)
	} else if len(row.Update) != 0 {
		for name, col := range row.Update {
			conditions[name] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		b.nextRow.Update = buildColumns(holder, row.Update)
	} else {
		log.Panic("unknown event type")
	}
	b.nextKey.OnlyHandleKey = false
	return b.assembleDMLEvent()
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
	if version != batchVersion1 {
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

	b.nextKey = msgKey
	b.nextRow = rowMsg

	return b.assembleDMLEvent(), nil
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

		if isPrimary(raw.Flag) {
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

		switch col.GetType() {
		case mysql.TypeVarchar, mysql.TypeString,
			mysql.TypeTinyBlob, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
			if !mysql.HasBinaryFlag(col.GetFlag()) {
				col.SetCharset("utf8mb4")
				col.SetCollate("utf8mb4_bin")
			}
		case mysql.TypeDuration:
			// todo: how to find the correct decimal for the duration type ?
			_, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
			col.FieldType.SetDecimal(defaultDecimal)
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

func (b *BatchDecoder) assembleDMLEvent() *commonEvent.DMLEvent {
	key := b.nextKey
	value := b.nextRow

	b.nextKey = nil
	b.nextRow = nil

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
		data := collectAllColumnsValue(value.Delete, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeDelete)
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
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeUpdate)
	} else if len(value.Update) != 0 {
		data := collectAllColumnsValue(value.Update, columns)
		common.AppendRow2Chunk(data, columns, chk)
		result.RowTypes = append(result.RowTypes, commonEvent.RowTypeInsert)
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
