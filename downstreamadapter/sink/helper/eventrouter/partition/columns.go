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

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/hash"
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

func (r *ColumnsPartitionGenerator) GeneratePartitionIndexAndKey(row *common.RowChangedEvent, partitionNum int32) (int32, string, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.hasher.Reset()

	r.hasher.Write([]byte(row.TableInfo.GetSchemaName()), []byte(row.TableInfo.GetTableName()))

	dispatchCols := row.Columns
	if len(dispatchCols) == 0 {
		dispatchCols = row.PreColumns
	}

	offsets, ok := row.TableInfo.OffsetsByNames(r.Columns)
	if !ok {
		log.Error("columns not found when dispatch event",
			zap.Any("tableName", row.TableInfo.GetTableName()),
			zap.Strings("columns", r.Columns))
		return 0, "", errors.ErrDispatcherFailed.GenWithStack(
			"columns not found when dispatch event, table: %v, columns: %v", row.TableInfo.GetTableName(), r.Columns)
	}

	for idx := 0; idx < len(r.Columns); idx++ {
		col := dispatchCols[offsets[idx]]
		if col == nil {
			continue
		}
		r.hasher.Write([]byte(r.Columns[idx]), []byte(model.ColumnValueString(col.Value)))
	}

	sum32 := r.hasher.Sum32()
	return int32(sum32 % uint32(partitionNum)), strconv.FormatInt(int64(sum32), 10), nil
}
