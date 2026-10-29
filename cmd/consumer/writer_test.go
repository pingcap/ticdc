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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestWriterFiltersReplay(t *testing.T) {
	for name, rowType := range map[string]common.RowType{
		"insert": common.RowTypeInsert,
		"update": common.RowTypeUpdate,
		"delete": common.RowTypeDelete,
	} {
		t.Run(name, func(t *testing.T) {
			memory := newMemoryUsage()
			column := &timodel.ColumnInfo{
				ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			}
			column.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
			table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
				ID: 1, Name: ast.NewCIStr("t"), PKIsHandle: true,
				Columns: []*timodel.ColumnInfo{column},
			})
			require.Equal(t, []int64{1}, table.GetOrderedHandleKeyColumnIDs())
			completed := 0
			row := func(ts uint64, ids ...int64) *dmlWrite {
				dml := event.NewDMLEvent(common.DispatcherID{}, 1, 0, ts, table)
				dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), len(ids)*2)
				for _, id := range ids {
					dml.Rows.AppendInt64(0, id)
					dml.RowTypes = append(dml.RowTypes, rowType)
					if rowType == common.RowTypeUpdate {
						dml.Rows.AppendInt64(0, id+100)
						dml.RowTypes = append(dml.RowTypes, rowType)
					}
					dml.Length++
				}
				dml.AddPostFlushFunc(func() { completed++ })
				bytes := dml.Rows.MemoryUsage()
				memory.reserve(bytes)
				return &dmlWrite{dml: dml, bytes: bytes}
			}
			downstream := mock.NewMockSink(gomock.NewController(t))
			downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
				require.Equal(t, int32(2), dml.Len())
				physicalRows := 2
				if rowType == common.RowTypeUpdate {
					physicalRows = 4
				}
				require.Equal(t, physicalRows, dml.Rows.NumRows())
				dml.PostFlush()
			}).Times(1)
			w := &writer{
				downstream: downstream, memory: memory,
				checkpoint: 100, tableCheckpoints: map[int64]uint64{1: 150},
				mutations: make(map[mutationKey]*writeBatch),
			}
			w.addDML(row(100, 1, 2))    // Global checkpoint, inclusive.
			w.addDML(row(150, 3, 4))    // Table checkpoint, inclusive.
			w.addDML(row(160, 5, 5, 6)) // Partial duplicate within a batch.
			require.NoError(t, w.flushDML(t.Context()))
			require.Equal(t, 3, completed)

			w.addDML(row(160, 5, 6)) // Entire event repeats durable mutations.
			require.NoError(t, w.flushDML(t.Context()))
			require.Equal(t, 4, completed)
			require.Zero(t, memory.used())

			w.watermark, w.progressChanged = 200, true
			w.advanceCheckpoint()
			require.Empty(t, w.mutations)
		})
	}
}
