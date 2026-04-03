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
	"hash/fnv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

func compareKeys(firstKey, secondKey []byte) bool {
	return bytes.Equal(firstKey, secondKey)
}

func genKeyAndHash(row *chunk.Row, tableInfo *common.TableInfo) (uint64, []byte) {
	key := genKeyList(row, tableInfo)
	if len(key) == 0 {
		log.Panic("the table has no primary key or not-null unique key", zap.Any("tableInfo", tableInfo))
	}

	hasher := fnv.New32a()
	if n, err := hasher.Write(key); n != len(key) || err != nil {
		log.Panic("transaction key hash fail")
	}

	return uint64(hasher.Sum32()), key
}

func genKeyList(row *chunk.Row, tableInfo *common.TableInfo) []byte {
	var key []byte
	keyColumns := tableInfo.GetOrderedHandleKeyColumnIDs()
	if len(keyColumns) == 0 {
		return nil
	}
	for _, colID := range keyColumns {
		// chunk.Row is laid out in schema column order (TableInfo.GetColumns()).
		// RowColumnsOffset is based on CDC-visible columns and may skip virtual generated columns,
		// which would cause extracting a different column value and break row identity.
		//  Thus, we should not use RowColumnsOffset here.
		schemaIdx := tableInfo.MustGetColumnOffsetByID(colID)
		info := tableInfo.GetColumns()[schemaIdx]
		if info == nil || info.ID != colID {
			return nil
		}

		value := common.ExtractColVal(row, info, schemaIdx)
		// if a column value is null, we can ignore this index
		if value == nil {
			return nil
		}

		val := common.ColumnValueString(value)
		if columnNeeds2LowerCase(info.GetType(), info.GetCollate()) {
			val = strings.ToLower(val)
		}

		key = append(key, []byte(val)...)
		key = append(key, 0)
	}
	return key
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
