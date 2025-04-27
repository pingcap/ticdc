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

package partition

import (
	"strconv"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/hash"
	"go.uber.org/zap"
)

type IndexValuePartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	IndexName string
}

func newIndexValuePartitionGenerator(indexName string) *IndexValuePartitionGenerator {
	return &IndexValuePartitionGenerator{
		hasher:    hash.NewPositionInertia(),
		IndexName: indexName,
	}
}

func (r *IndexValuePartitionGenerator) GeneratePartitionIndexAndKey(row *commonEvent.RowChange,
	partitionNum int32,
	tableInfo *common.TableInfo,
	commitTs uint64,
) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()

	fields := make([]zap.Field, 0, 4)
	r.hasher.Write([]byte(tableInfo.GetSchemaName()), []byte(tableInfo.GetTableName()))

	fields = append(fields, zap.String("schema", tableInfo.GetSchemaName()))
	fields = append(fields, zap.String("table", tableInfo.GetTableName()))

	rowData := row.Row
	if rowData.IsEmpty() {
		rowData = row.PreRow
	}

	fields = append(fields, zap.String("indexName", r.IndexName))

	// the most normal case, index-name is not set, use the handle key columns.
	if r.IndexName == "" {
		for idx, col := range tableInfo.GetColumns() {
			if col == nil {
				continue
			}
			if tableInfo.IsHandleKey(col.ID) {
				colName := []byte(col.Name.L)
				value := []byte(common.ColumnValueString(common.ExtractColVal(&rowData, col, idx)))
				r.hasher.Write(colName, value)

				fields = append(fields, zap.String("col", string(colName)))
				fields = append(fields, zap.Any("value", value))
			}
		}
	} else {
		names, offsets, ok := tableInfo.IndexByName(r.IndexName)
		if !ok {
			log.Error("index not found when dispatch event",
				zap.Any("tableName", tableInfo.GetTableName()),
				zap.String("indexName", r.IndexName))
			return 0, "", errors.ErrDispatcherFailed.GenWithStack(
				"index not found when dispatch event, table: %v, index: %s", tableInfo.GetTableName(), r.IndexName)
		}
		for idx := 0; idx < len(names); idx++ {
			colInfo := tableInfo.GetColumns()[offsets[idx]]
			value := common.ExtractColVal(&rowData, colInfo, idx)
			if value == nil {
				continue
			}

			colName := []byte(names[idx])
			v := []byte(common.ColumnValueString(value))
			r.hasher.Write(colName, v)

			fields = append(fields, zap.String("col", string(colName)))
			fields = append(fields, zap.Any("value", v))
		}
	}

	sum32 := r.hasher.Sum32()
	partition := int32(sum32 % uint32(partitionNum))

	fields = append(fields, zap.Int32("partition", partition))

	log.Info("index-value dispatch event", fields...)

	return partition, strconv.FormatInt(int64(sum32), 10), nil
}
