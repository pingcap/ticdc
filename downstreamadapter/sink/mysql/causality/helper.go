// Copyright 2022 PingCAP, Inc.
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

package causality

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"go.uber.org/zap"
)

// TiDB enables new collations during package initialization. Cache the supported
// collators before CDC disables the global switch in main for handle decoding.
// Unsupported collations must use a shared index key instead of TiDB's binary
// fallback, which could miss dependencies under the downstream collation.
var conflictCollators = func() map[string]collate.Collator {
	collators := make(map[string]collate.Collator)
	for _, collation := range collate.GetSupportedCollations() {
		collators[collation.Name] = collate.GetCollatorWithCollate(true, collation.Name)
	}
	return collators
}()

// ConflictKeys implements causality.txnEvent interface.
func ConflictKeys(event *commonEvent.DMLEvent) map[uint64]struct{} {
	if event.Len() == 0 {
		return nil
	}

	hashRes := make(map[uint64]struct{}, event.Len())
	hasher := fnv.New32a()

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		keys := genRowKeys(row, event.TableInfo, event.DispatcherID)
		for _, key := range keys {
			if n, err := hasher.Write(key); n != len(key) || err != nil {
				log.Panic("transaction key hash fail")
			}
			hashRes[uint64(hasher.Sum32())] = struct{}{}
			hasher.Reset()
		}
	}

	return hashRes
}

func genRowKeys(row commonEvent.RowChange, tableInfo *common.TableInfo, dispatcherID common.DispatcherID) [][]byte {
	var keys [][]byte

	for _, rowData := range []chunk.Row{row.Row, row.PreRow} {
		if rowData.IsEmpty() {
			continue
		}
		// An integer primary key (or an explicit extra handle) may not have
		// an IndexInfo, so retain its dependency separately.
		pkIndex := tableInfo.GetPKIndex()
		if tableInfo.PKIsHandle() || (len(pkIndex) == 1 && pkIndex[0] == model.ExtraHandleID) {
			pkColumns := []*model.IndexColumn{{
				Offset: tableInfo.MustGetColumnOffsetByID(pkIndex[0]),
				Length: types.UnspecifiedLength,
			}}
			if key := genKeyList(&rowData, -1, pkColumns, dispatcherID, tableInfo); len(key) > 0 {
				keys = append(keys, key)
			}
		}
		// Use the actual index metadata to preserve prefix lengths.
		for _, index := range tableInfo.GetIndices() {
			if !index.Primary && !index.Unique {
				continue
			}
			if key := genKeyList(&rowData, index.ID, index.Columns, dispatcherID, tableInfo); len(key) > 0 {
				keys = append(keys, key)
			}
		}
	}
	if len(keys) == 0 {
		// use dispatcherID as key if no key generated (no PK/UK),
		// no concurrence for rows in the same dispatcher.
		log.Debug("Use dispatcherID as the key", zap.Any("dispatcherID", dispatcherID))
		tableKey := make([]byte, 8)
		binary.BigEndian.PutUint64(tableKey, uint64(dispatcherID.GetLow()))
		keys = [][]byte{tableKey}
	}
	return keys
}

func genKeyList(
	row *chunk.Row, indexID int64, indexColumns []*model.IndexColumn, dispatcherID common.DispatcherID, tableInfo *common.TableInfo,
) []byte {
	indexKey := make([]byte, 16)
	binary.BigEndian.PutUint64(indexKey[:8], uint64(indexID))
	binary.BigEndian.PutUint64(indexKey[8:], dispatcherID.GetLow())

	var key []byte
	for _, indexColumn := range indexColumns {
		// IndexColumn.Offset and chunk.Row both use schema column order,
		// including placeholders for virtual generated columns.
		offsetIdx := indexColumn.Offset
		info := tableInfo.GetColumns()[offsetIdx]
		if info == nil || info.IsVirtualGenerated() {
			// Virtual values are not carried in row events. Serialize this
			// index even when another index, such as the PK, is available.
			return indexKey
		}
		value := common.ExtractColVal(row, info, offsetIdx)

		// if a column value is null, we can ignore this index
		if value == nil {
			return nil
		}

		if types.IsString(info.GetType()) {
			collator, ok := conflictCollators[info.GetCollate()]
			if !ok {
				return indexKey
			}
			// Match TiDB's index encoding: truncate by characters (or bytes
			// for binary/ASCII columns), then apply the collation's weights.
			datum := row.GetDatum(offsetIdx, &info.FieldType)
			tablecodec.TruncateIndexValue(&datum, indexColumn, info)
			key = append(key, collator.ImmutableKey(datum.GetString())...)
		} else {
			key = append(key, common.ColumnValueString(value)...)
		}
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil
	}
	key = append(key, indexKey...)
	return key
}
