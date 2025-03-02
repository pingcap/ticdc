// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"bytes"
	"encoding/binary"
	"hash/fnv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tiflow/cdc/model"
)

func compareKeys(firstKey, secondKey [][]byte) bool {
	if len(firstKey) != len(secondKey) {
		return false
	}

	for i := range firstKey {
		if len(firstKey[i]) != len(secondKey[i]) {
			return false
		}
		if !bytes.Equal(firstKey[i], secondKey[i]) {
			return false
		}
	}

	return true
}

func genKeyAndHash(row *chunk.Row, tableInfo *common.TableInfo) (uint64, [][]byte, error) {
	var keys [][]byte
	for iIdx, idxCol := range tableInfo.GetIndexColumnsOffset() {
		//log.Info("genKeyAndHash", zap.Any("idxCol", idxCol), zap.Any("iIdx", iIdx))
		key, err := genKeyList(row, iIdx, idxCol, tableInfo)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		if len(key) == 0 {
			continue
		}
		keys = append(keys, key)
	}

	hasher := fnv.New32a()
	for _, key := range keys {
		if n, err := hasher.Write(key); n != len(key) || err != nil {
			log.Panic("transaction key hash fail")
		}
	}

	return uint64(hasher.Sum32()), keys, nil
}

func genKeyList(row *chunk.Row, iIdx int, colIdx []int, tableInfo *common.TableInfo) ([]byte, error) {
	var key []byte
	columnInfos := tableInfo.GetColumns()
	for _, i := range colIdx {
		if columnInfos[i] == nil {
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
	tableKey := make([]byte, 8)
	binary.BigEndian.PutUint64(tableKey, uint64(iIdx))
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
