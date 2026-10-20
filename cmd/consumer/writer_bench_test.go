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
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func BenchmarkWriterBacklog(b *testing.B) {
	for name, count := range map[string]int{"1024": 1024, "32768": 32768, "32768/watermark": 32768} {
		b.Run(name, func(b *testing.B) {
			ctx := b.Context()
			tables := make([]*common.TableInfo, 100)
			for i := range tables {
				tables[i] = common.NewTableInfo4Decoder("test", &timodel.TableInfo{
					ID:      int64(i + 1),
					Name:    ast.NewCIStr(strconv.Itoa(i)),
					Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
				})
			}
			events := make([]*dmlWrite, count)
			for i := range events {
				id := int64(i%100 + 1)
				events[i] = &dmlWrite{dml: &event.DMLEvent{
					PhysicalTableID: id,
					CommitTs:        10,
					Length:          1,
					TableInfo:       tables[i%len(tables)],
				}}
			}
			downstream := mock.NewMockSink(gomock.NewController(b))
			downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
				dml.PostFlush()
				dml.ClearPostFlushFunc()
			}).AnyTimes()
			w := &writer{
				downstream: downstream,
				memory:     newMemoryUsage(),
			}
			pending := make([]*dmlWrite, count)
			b.ReportAllocs()
			for b.Loop() {
				copy(pending, events)
				w.pendingDML = pending
				if name == "32768/watermark" {
					w.watermark, w.checkpoint, w.progressChanged = 20, 0, true
				}
				if err := w.flushDML(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
