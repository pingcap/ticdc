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

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

var tableIDAllocator = common.NewTableIDAllocator()

type decoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey *messageKey

	storage storage.ExternalStorage

	config *common.Config

	upstreamTiDB *sql.DB

	idx int
}

// NewDecoder creates a new decoder.
func NewDecoder(
	ctx context.Context, idx int, config *common.Config, db *sql.DB,
) (common.Decoder, error) {
	var (
		externalStorage storage.ExternalStorage
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
			zap.Any("value", value), zap.Error(err))
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
	result.TableID = tableIDAllocator.Allocate(result.SchemaName, result.TableName)

	// only the DDL comes from the first partition will be processed.
	if b.idx == 0 {
		tableIDAllocator.AddBlockTableID(result.SchemaName, result.TableName, result.TableID)
		result.BlockedTables = common.GetBlockedTables(tableIDAllocator, result)
	}

	b.nextKey = nil
	b.valueBytes = nil
	return result
}

// NextDMLEvent implements the Decoder interface
func (b *decoder) NextDMLEvent() *commonEvent.DMLEvent {
	if b.nextKey.Type != common.MessageTypeRow {
		log.Panic("message type is not row", zap.Any("messageType", b.nextKey.Type))
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

	nextRow := new(messageRow)
	nextRow.decode(value)

	ctx := context.Background()
	// claim-check message found
	if b.nextKey.ClaimCheckLocation != "" {
		return b.assembleEventFromClaimCheckStorage(ctx)
	}

	if b.nextKey.OnlyHandleKey && b.upstreamTiDB != nil {
		return b.assembleHandleKeyOnlyDMLEvent(ctx, nextRow)
	}

	return b.assembleDMLEvent(nextRow)
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

func (b *decoder) assembleHandleKeyOnlyDMLEvent(ctx context.Context, row *messageRow) *commonEvent.DMLEvent {
	key := b.nextKey
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
		row.Delete = buildColumns(holder, row.Delete)
	} else if len(row.PreColumns) != 0 {
		for name, col := range row.PreColumns {
			conditions[name] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		row.PreColumns = buildColumns(holder, row.PreColumns)
		holder = common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		row.Update = buildColumns(holder, row.Update)
	} else if len(row.Update) != 0 {
		for name, col := range row.Update {
			conditions[name] = col.Value
		}
		holder := common.MustSnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		row.Update = buildColumns(holder, row.Update)
	} else {
		log.Panic("unknown event type")
	}
	b.nextKey.OnlyHandleKey = false
	return b.assembleDMLEvent(row)
}

func (b *decoder) assembleEventFromClaimCheckStorage(ctx context.Context) *commonEvent.DMLEvent {
	_, claimCheckFileName := filepath.Split(b.nextKey.ClaimCheckLocation)
	b.nextKey = nil
	data, err := b.storage.ReadFile(ctx, claimCheckFileName)
	if err != nil {
		log.Panic("read claim check file failed", zap.String("fileName", claimCheckFileName), zap.Error(err))
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		log.Panic("unmarshal claim check message failed", zap.Any("data", data), zap.Error(err))
	}

	version := binary.BigEndian.Uint64(claimCheckM.Key[:8])
	if version != batchVersion1 {
		log.Panic("the batch version is not supported", zap.Uint64("version", version))
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
		log.Panic("decompress large message failed",
			zap.String("compression", b.config.LargeMessageHandle.LargeMessageHandleCompression),
			zap.Any("value", value), zap.Error(err))
	}

	rowMsg := new(messageRow)
	rowMsg.decode(value)

	b.nextKey = msgKey
	return b.assembleDMLEvent(rowMsg)
}

func (b *decoder) queryTableInfo(key *messageKey, value *messageRow) *commonType.TableInfo {
	tableInfo := b.newTableInfo(key, value)
	return tableInfo
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
	if len(tableInfo.Indices) != 0 {
		tableInfo.PKIsHandle = true
	}
	return commonType.NewTableInfo4Decoder(key.Schema, tableInfo)
}

func newTiColumns(rawColumns map[string]column) []*timodel.ColumnInfo {
	result := make([]*timodel.ColumnInfo, 0)
	var nextColumnID int64
	for name, raw := range rawColumns {
		col := new(timodel.ColumnInfo)
		col.ID = nextColumnID
		col.Name = ast.NewCIStr(name)
		col.FieldType = *types.NewFieldType(raw.Type)

		if isPrimary(raw.Flag) || isHandle(raw.Flag) {
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
		if isNullable(raw.Flag) {
			col.AddFlag(mysql.NotNullFlag)
		}
		if isGenerated(raw.Flag) {
			col.AddFlag(mysql.GeneratedColumnFlag)
			col.GeneratedExprString = "holder" // just to make it not empty
			col.GeneratedStored = true
		}
		if isUnique(raw.Flag) {
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
			// todo: how to find the correct decimal for the duration type ?
			_, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
			col.SetDecimal(defaultDecimal)
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
	indices := make([]*timodel.IndexInfo, 0, 1)
	multiColumns := make([]*timodel.IndexColumn, 0, 2)
	for idx, col := range columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			indexColumns := make([]*timodel.IndexColumn, 0)
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
			indices = append(indices, &timodel.IndexInfo{
				ID:      1,
				Name:    ast.NewCIStr("primary"),
				Columns: indexColumns,
				Primary: true,
				Unique:  true,
			})
		} else if mysql.HasUniKeyFlag(col.GetFlag()) {
			indexColumns := make([]*timodel.IndexColumn, 0)
			indexColumns = append(indexColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
			indices = append(indices, &timodel.IndexInfo{
				ID:      1 + int64(len(indices)),
				Name:    ast.NewCIStr(col.Name.O + "_idx"),
				Columns: indexColumns,
				Unique:  true,
			})
		}
		if mysql.HasMultipleKeyFlag(col.GetFlag()) {
			multiColumns = append(multiColumns, &timodel.IndexColumn{
				Name:   col.Name,
				Offset: idx,
			})
		}
	}
	// if there are multiple multi-column indices, consider as one.
	if len(multiColumns) != 0 {
		indices = append(indices, &timodel.IndexInfo{
			ID:      1 + int64(len(indices)),
			Name:    ast.NewCIStr("multi_idx"),
			Columns: multiColumns,
			Unique:  false,
		})
	}
	return indices
}

func (b *decoder) assembleDMLEvent(value *messageRow) *commonEvent.DMLEvent {
	key := b.nextKey
	b.nextKey = nil

	tableInfo := b.queryTableInfo(key, value)
	result := new(commonEvent.DMLEvent)
	result.TableInfo = tableInfo
	result.PhysicalTableID = tableInfo.TableName.TableID
	result.StartTs = key.Ts
	result.CommitTs = key.Ts
	result.Length++

	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
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
		result.RowTypes = append(result.RowTypes, commonType.RowTypeUpdate)
		result.RowTypes = append(result.RowTypes, commonType.RowTypeUpdate)
	} else if len(value.Update) != 0 {
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
