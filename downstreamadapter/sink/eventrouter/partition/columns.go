// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/pkg/hash"
	"go.uber.org/zap"
)

// ColumnsDispatcher is a partition dispatcher
// which dispatches events based on the given columns.
type ColumnsPartitionGenerator struct {
	hasher *hash.PositionInertia
	lock   sync.Mutex

	Columns []string
}

// NewColumnsDispatcher creates a ColumnsDispatcher.
func newColumnsPartitionGenerator(columns []string) *ColumnsPartitionGenerator {
	return &ColumnsPartitionGenerator{
		hasher:  hash.NewPositionInertia(),
		Columns: columns,
	}
}

func (r *ColumnsPartitionGenerator) GeneratePartitionIndexAndKey(row *commonEvent.RowChange, partitionNum int32, tableInfo *common.TableInfo, commitTs uint64) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()

	r.hasher.Write([]byte(tableInfo.GetSchemaName()), []byte(tableInfo.GetTableName()))

	rowData := row.Row
	if rowData.IsEmpty() {
		rowData = row.PreRow
	}

	offsets, err := tableInfo.OffsetsByNames(r.Columns)
	if err != nil {
		log.Error("dispatch event failed", zap.Error(err))
		return 0, "", err
	}

	for _, idx := range offsets {
		colInfo := tableInfo.GetColumns()[idx]
		value := common.ExtractColVal(&rowData, colInfo, idx)
		if value == nil {
			continue
		}
		r.hasher.Write([]byte(colInfo.Name.O), []byte(common.ColumnValueString(value)))
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
