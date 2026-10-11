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
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestDecoderGroupOrderingAndConfirmation(t *testing.T) {
	for name, mergeRows := range map[string]bool{"messages": false, "rows": true} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ctx, cancelWorkers := context.WithCancelCause(ctx)
			memory := &memoryUsage{}
			completed := make(chan struct{}, 1)
			g := &decoderGroup{
				memory:    memory,
				completed: completed,
				input:     make(chan *decodeTask, 8),
				workers:   2,
			}
			a := &assembler{
				memory:    memory,
				decoder:   g,
				tables:    make(map[[2]string]*common.TableInfo),
				mergeRows: mergeRows,
			}
			downstream := mock.NewMockSink(gomock.NewController(t))
			metadata := make(map[int64]*common.TableInfo)
			var (
				written []string
				commits []uint64
			)
			downstream.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
				if mergeRows {
					require.EqualValues(t, 100, dml.TableInfo.UpdateTS)
					require.NotSame(t, metadata[dml.PhysicalTableID], dml.TableInfo)
				}
				written = append(written, dml.TableInfo.GetTableName())
				commits = append(commits, dml.CommitTs)
				dml.PostFlush()
			}).Times(3)
			c := &consumer{
				assembler: a,
				memory:    memory,
				completed: completed,
				writer: &writer{
					memory:     memory,
					downstream: downstream,
					completed:  completed,
				},
			}
			defer c.wg.Wait()
			defer cancelWorkers(nil)
			for range g.workers {
				c.wg.Go(func() { cancelWorkers(g.decode(ctx)) })
			}
			record, err := memory.newAck(ctx, 4096, nil)
			require.NoError(t, err)
			data := &readData{record: record, retainedBytes: 128, dmlBoundary: &readBoundary{reached: true}}
			r := &pulsarReader{memory: memory}
			release := make(chan struct{})
			for i, id := range []int64{1, 1, 2} {
				name := "a"
				if id == 2 {
					name = "b"
				}
				table := metadata[id]
				if table == nil {
					table = common.NewTableInfo4Decoder("test", &timodel.TableInfo{
						ID:      id,
						Name:    ast.NewCIStr(name),
						Columns: []*timodel.ColumnInfo{{ID: 1, Name: ast.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLonglong)}},
					})
					metadata[id] = table
				}
				if mergeRows {
					// A CSV selector can return shared metadata without the
					// source schema version. Conversion must keep it immutable.
					data.table = table.CloneWithRouting("test", name)
					data.table.UpdateTS = 100
				}
				message := codecCommon.NewDMLMessage(id, "test", name, uint64(10+i), common.RowTypeInsert, func() *event.DMLEvent {
					if i == 0 {
						select {
						case <-release:
						case <-ctx.Done():
						}
					}
					dml := event.NewDMLEvent(common.NewDispatcherID(), id, 0, uint64(10+i), table)
					dml.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
					dml.Rows.AppendInt64(0, int64(i))
					dml.RowTypes, dml.Length = []common.RowType{common.RowTypeInsert}, 1
					return dml
				})
				require.NoError(t, a.assembleMessage(ctx, data, message, record, r))
				a.decoding = append(a.decoding, a.nextReady(0))
			}
			memory.decoded(record, 128)
			select {
			case <-a.decoding[2].decoding.done:
			case <-ctx.Done():
				t.Fatal("independent table could not convert while table a was blocked")
			}
			// An input shared by several rows retains its memory and remains unconfirmed.
			require.EqualValues(t, 4096, record.memory.Load())
			require.Greater(t, record.refs.Load(), int64(0))
			watermark := &writeEvent{watermark: 20, hasWatermark: true}
			items, err := a.assemble(ctx, nil, watermark)
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, c.writer.consume(ctx, item))
			}
			require.NoError(t, c.writer.flushDML(ctx))
			require.Equal(t, []string{"b"}, written)
			require.Len(t, a.decoding, 3)
			require.Zero(t, c.writer.writtenBefore)
			first, second := a.decoding[0].decoding, a.decoding[1].decoding
			close(release)
			for _, task := range []*decodeTask{first, second} {
				select {
				case <-task.done:
				case <-ctx.Done():
					t.Fatal("table a conversion did not finish")
				}
			}
			require.EqualValues(t, 128, record.memory.Load())
			require.Greater(t, record.refs.Load(), int64(0))
			items, err = a.collectDecoded(ctx)
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, c.writer.consume(ctx, item))
			}
			require.NoError(t, c.writer.flushDML(ctx))
			require.Equal(t, []string{"b", "a", "a"}, written)
			require.Equal(t, []uint64{12, 10, 11}, commits)
			require.Empty(t, a.decoding)
			require.Zero(t, record.refs.Load())
			require.EqualValues(t, 20, c.writer.writtenBefore)
			for _, table := range metadata {
				require.Zero(t, table.UpdateTS)
			}
			memory.confirm(record)
		})
	}
}

func TestDecodingDDLScope(t *testing.T) {
	for _, name := range []string{"a", "b", ""} {
		t.Run(name, func(t *testing.T) {
			table := &common.TableInfo{TableName: common.TableName{Schema: "test", Table: "a", TableID: 1}}
			pending := &writeEvent{dml: &event.DMLEvent{TableInfo: table, PhysicalTableID: 1, CommitTs: 10}, decoding: &decodeTask{done: make(chan struct{})}}
			ddl := &writeEvent{ddl: &event.DDLEvent{SchemaName: "test", TableName: name, FinishedTs: 20, Type: byte(timodel.ActionAddColumn)}}
			a := &assembler{decoding: []*writeEvent{pending, ddl}}
			w := &writer{
				memory:  &memoryUsage{},
				ddlJobs: make(chan *writeEvent, 1),
			}
			items, err := a.collectDecoded(t.Context())
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, w.consume(t.Context(), item))
			}
			require.NoError(t, w.flushDML(t.Context()))
			if name == "b" {
				require.Len(t, w.ddlJobs, 1)
				require.Equal(t, ddl, <-w.ddlJobs)
				require.Equal(t, []*writeEvent{pending}, a.decoding)
			} else {
				require.Empty(t, w.ddlJobs)
				require.Equal(t, []*writeEvent{pending, ddl}, a.decoding)
			}
		})
	}
}
