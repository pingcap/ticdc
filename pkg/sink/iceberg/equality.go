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

package iceberg

import (
	"fmt"

	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

// GetEqualityFieldIDs returns the Iceberg field IDs used for equality deletes (upsert mode).
// It is based on TiDB's ordered handle key column IDs (PK or NOT NULL unique key).
func GetEqualityFieldIDs(tableInfo *common.TableInfo) ([]int, error) {
	if tableInfo == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	keyIDs := tableInfo.GetOrderedHandleKeyColumnIDs()
	if len(keyIDs) == 0 {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("table has no handle key columns")
	}

	visibleColumns := make(map[int64]struct{}, len(tableInfo.GetColumns()))
	for _, col := range tableInfo.GetColumns() {
		if col == nil || col.IsVirtualGenerated() {
			continue
		}
		visibleColumns[col.ID] = struct{}{}
	}

	out := make([]int, 0, len(keyIDs))
	for _, id := range keyIDs {
		if _, ok := visibleColumns[id]; !ok {
			return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs(
				fmt.Sprintf("handle key column id is not present in table columns: %d", id),
			)
		}
		out = append(out, int(id))
	}
	return out, nil
}
