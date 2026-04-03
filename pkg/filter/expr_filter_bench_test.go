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

package filter

import (
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// cmd: go test -benchmem -run=^$ -bench ^BenchmarkSkipDML$ github.com/pingcap/ticdc/pkg/filter
// goos: maxOS 12.3.1
// goarch: arm64
// cpu: Apple M1 Pro
// BenchmarkSkipDML/insert-1
// BenchmarkSkipDML/insert-1-10              990166              1151 ns/op             768 B/op         24 allocs/op
// BenchmarkSkipDML/insert-2
// BenchmarkSkipDML/insert-2-10             1000000              1187 ns/op             768 B/op         24 allocs/op
// BenchmarkSkipDML/update
// BenchmarkSkipDML/update-10                698208              1637 ns/op            1480 B/op         43 allocs/op
// BenchmarkSkipDML/delete
// BenchmarkSkipDML/delete-10               1000000              1112 ns/op             768 B/op         24 allocs/op
func BenchmarkSkipDML(b *testing.B) {
	cfg := &config.FilterConfig{
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:                  []string{"test.student"},
				IgnoreInsertValueExpr:    util.AddressOf("name = 'Will'"),
				IgnoreDeleteValueExpr:    util.AddressOf("age >= 32"),
				IgnoreUpdateOldValueExpr: util.AddressOf("gender = 'female'"),
				IgnoreUpdateNewValueExpr: util.AddressOf("age > 28"),
			},
		},
	}

	f, err := newExprFilter("UTC", cfg)
	require.NoError(b, err)

	// Build a table info matching the filter rules.
	cols := []*timodel.ColumnInfo{
		newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag|mysql.NotNullFlag),
		newColumnInfo(2, "name", mysql.TypeString, 0),
		newColumnInfo(3, "age", mysql.TypeLong, 0),
		newColumnInfo(4, "gender", mysql.TypeString, 0),
	}
	tableInfo := mustNewCommonTableInfo("test", "student", cols, nil)

	insertRow := datumsToChunkRow([]types.Datum{
		types.NewIntDatum(999),
		types.NewStringDatum("Will"),
		types.NewIntDatum(39),
		types.NewStringDatum("male"),
	}, tableInfo)
	updatePreRow := datumsToChunkRow([]types.Datum{
		types.NewIntDatum(876),
		types.NewStringDatum("Li"),
		types.NewIntDatum(45),
		types.NewStringDatum("female"),
	}, tableInfo)
	updateRow := datumsToChunkRow([]types.Datum{
		types.NewIntDatum(1),
		types.NewStringDatum("Dongmen"),
		types.NewIntDatum(20),
		types.NewStringDatum("male"),
	}, tableInfo)

	type benchCase struct {
		name       string
		dmlType    commonType.RowType
		preRow     chunk.Row
		row        chunk.Row
		shouldSkip bool
	}

	cases := []benchCase{
		{
			name:       "insert",
			dmlType:    commonType.RowTypeInsert,
			preRow:     chunk.Row{},
			row:        insertRow,
			shouldSkip: true,
		},
		{
			name:       "update",
			dmlType:    commonType.RowTypeUpdate,
			preRow:     updatePreRow,
			row:        updateRow,
			shouldSkip: true,
		},
		{
			name:       "delete",
			dmlType:    commonType.RowTypeDelete,
			preRow:     updatePreRow,
			row:        chunk.Row{},
			shouldSkip: true,
		},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ignore, err := f.shouldSkipDML(c.dmlType, c.preRow, c.row, tableInfo)
				require.NoError(b, err)
				require.Equal(b, c.shouldSkip, ignore)
			}
		})
	}
}
