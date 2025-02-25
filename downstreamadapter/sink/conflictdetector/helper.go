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

package conflictdetector

import (
	"encoding/binary"
	"hash/fnv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// ConflictKeys implements causality.txnEvent interface.
func ConflictKeys(event *commonEvent.DMLEvent) ([]uint64, error) {
	if event.Len() == 0 {
		return nil, nil
	}

	hashRes := make(map[uint64]struct{}, event.Len())
	hasher := fnv.New32a()

	for {
		row, ok := event.GetNextRow()
		if !ok {
			break
		}
		keys, err := genRowKeys(row, event.TableInfo, event.DispatcherID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, key := range keys {
			if n, err := hasher.Write(key); n != len(key) || err != nil {
				log.Panic("transaction key hash fail")
			}
			hashRes[uint64(hasher.Sum32())] = struct{}{}
			hasher.Reset()
		}
	}

	event.FinishGetRow()

	keys := make([]uint64, 0, len(hashRes))
	for key := range hashRes {
		keys = append(keys, key)
	}
	return keys, nil
}

func genRowKeys(row commonEvent.RowChange, tableInfo *common.TableInfo, dispatcherID common.DispatcherID) ([][]byte, error) {
	var keys [][]byte

	if !row.Row.IsEmpty() {
		for iIdx, idxCol := range tableInfo.GetIndexColumnsOffset() {
			key, err := genKeyList(&row.Row, iIdx, idxCol, dispatcherID, tableInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
		}
	}
	if !row.PreRow.IsEmpty() {
		for iIdx, idxCol := range tableInfo.GetIndexColumnsOffset() {
			key, err := genKeyList(&row.PreRow, iIdx, idxCol, dispatcherID, tableInfo)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if len(key) == 0 {
				continue
			}
			keys = append(keys, key)
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
	return keys, nil
}

func genKeyList(
	row *chunk.Row, iIdx int, colIdx []int, dispatcherID common.DispatcherID, tableInfo *common.TableInfo,
) ([]byte, error) {
	var key []byte
	columnInfos := tableInfo.GetColumns()
	for _, i := range colIdx {
		// If the index contain generated column, we can't use this key to detect conflict with other DML,
		if columnInfos[i] == nil || tableInfo.GetColumnFlags()[columnInfos[i].ID].IsGeneratedColumn() {
			return nil, nil
		}

		value, err := common.FormatColVal(row, columnInfos[i], i)
		if err != nil {
			return nil, err
		}
		// if a column value is null, we can ignore this index
		if value == nil {
			return nil, nil
		}

		val := model.ColumnValueString(value)
		if columnNeeds2LowerCase(columnInfos[i].GetType(), columnInfos[i].GetCollate()) {
			val = strings.ToLower(val)
		}

		key = append(key, []byte(val)...)
		key = append(key, 0)
	}
	if len(key) == 0 {
		return nil, nil
	}
	tableKey := make([]byte, 16)
	binary.BigEndian.PutUint64(tableKey[:8], uint64(iIdx))
	binary.BigEndian.PutUint64(tableKey[8:], uint64(dispatcherID.GetLow()))
	key = append(key, tableKey...)
	return key, nil
}

func columnNeeds2LowerCase(mysqlType byte, collation string) bool {
	switch mysqlType {
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob:
		return collationNeeds2LowerCase(collation)
	}
	return false
}

func collationNeeds2LowerCase(collation string) bool {
	return strings.HasSuffix(collation, "_ci")
}
