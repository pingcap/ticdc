// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func BenchmarkMemoryAccounting(b *testing.B) {
	ctx := b.Context()
	memory := &memoryUsage{}
	schemas := &schemaUsage{memory: memory}
	table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
		ID:      1,
		Name:    ast.NewCIStr("t"),
		Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
	})
	if err := schemas.retain(ctx, table); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := schemas.retain(ctx, table); err != nil {
				b.Fatal(err)
			}
			if err := memory.reserve(ctx, 1024); err != nil {
				b.Fatal(err)
			}
			schemas.release(table)
			memory.release(1024)
		}
	})
	schemas.release(table)
}
