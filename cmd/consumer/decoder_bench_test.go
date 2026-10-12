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
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/canal"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

func BenchmarkDecoderGroupCSV(b *testing.B) {
	for _, tables := range []int{1, 100} {
		b.Run(strconv.Itoa(tables), func(b *testing.B) {
			ctx, cancel := context.WithCancel(b.Context())
			g, ctx := errgroup.WithContext(ctx)
			defer func() {
				cancel()
				_ = g.Wait()
			}()
			memory := &memoryUsage{}
			progress := &inputProgress{memory: memory}
			d := newDecoderGroup(&schemaUsage{memory: memory}, nil)
			d.start(ctx, g)
			messages := make([]*codecCommon.DMLMessage, tables)
			metadata := make([]*common.TableInfo, tables)
			cfg := &codecCommon.Config{
				Delimiter:       ",",
				Quote:           "\"",
				Terminator:      "\n",
				NullString:      "\\N",
				IncludeCommitTs: true,
			}
			for i := range tables {
				columns := make([]*timodel.ColumnInfo, 32)
				for j := range columns {
					columns[j] = &timodel.ColumnInfo{
						ID:        int64(j + 1),
						Name:      ast.NewCIStr("c" + strconv.Itoa(j)),
						Offset:    j,
						State:     timodel.StatePublic,
						FieldType: *types.NewFieldType(mysql.TypeLonglong),
					}
				}
				name := "t" + strconv.Itoa(i)
				table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
					Name:    ast.NewCIStr(name),
					Columns: columns,
				})
				decoder, err := csv.NewDecoder(ctx, cfg, table, []byte("I,"+name+",test,10"+strings.Repeat(",12345", len(columns))+"\n"))
				if err != nil {
					b.Fatal(err)
				}
				_, hasNext := decoder.HasNext()
				if !hasNext {
					b.Fatal("CSV fixture has no row")
				}
				messages[i], metadata[i] = decoder.NextDMLMessage(), table
			}
			tasks := make([]*decodeTask, batchRows)
			b.ReportAllocs()
			for b.Loop() {
				record, err := progress.newAck(ctx, 128, nil)
				if err != nil {
					b.Fatal(err)
				}
				for i := range tasks {
					id := i % tables
					task := &decodeTask{
						message:       messages[id],
						record:        record,
						done:          make(chan struct{}),
						table:         metadata[id],
						tableID:       int64(id + 1),
						retainedBytes: 128,
					}
					tasks[i] = task
					if err := d.submit(ctx, task); err != nil {
						b.Fatal(err)
					}
				}
				record.decoded(128)
				for _, task := range tasks {
					select {
					case <-task.done:
					case <-ctx.Done():
						b.Fatal(context.Cause(ctx))
					}
					task.dml.PostFlush()
					memory.release(task.bytes)
				}
				record.confirm()
			}
		})
	}
}

// Compare queue policies with the same CPU, fixed table count and encoded data.
func BenchmarkDecoderGroupCanalJSON(b *testing.B) {
	level := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(level)
	for _, workload := range []string{"balanced", "skewed"} {
		b.Run("workload="+workload, func(b *testing.B) {
			for _, queue := range []string{"table", "shared"} {
				b.Run("queue="+queue, func(b *testing.B) {
					ctx, cancel := context.WithCancel(b.Context())
					g, ctx := errgroup.WithContext(ctx)
					defer func() {
						cancel()
						_ = g.Wait()
					}()
					memory := &memoryUsage{}
					progress := &inputProgress{memory: memory}
					cfg := codecCommon.NewConfig(config.ProtocolCanalJSON)
					cfg.EnableTiDBExtension = true
					decoder, err := canal.NewDecoder(ctx, cfg, nil)
					if err != nil {
						b.Fatal(err)
					}
					values := make([][]byte, 100)
					for id := range values {
						data := make(map[string]string, 32)
						mysqlTypes := make(map[string]string, 32)
						for column := range 32 {
							name := "c" + strconv.Itoa(column)
							data[name], mysqlTypes[name] = "12345", "bigint"
						}
						values[id], err = json.Marshal(map[string]any{
							"database":  "test",
							"table":     "t" + strconv.Itoa(id),
							"type":      "INSERT",
							"isDdl":     false,
							"mysqlType": mysqlTypes,
							"data":      []map[string]string{data},
							"_tidb":     map[string]uint64{"commitTs": 10},
						})
						if err != nil {
							b.Fatal(err)
						}
					}
					// Populate schemas before timing steady-state conversion.
					for _, value := range values {
						decoder.AddKeyValue(nil, value)
						if _, hasNext := decoder.HasNext(); !hasNext {
							b.Fatal("Canal JSON fixture has no row")
						}
						decoder.NextDMLMessage().ToDMLEvent().PostFlush()
					}
					d := newDecoderGroup(&schemaUsage{memory: memory}, nil)
					groups := make([]*decoderGroup, d.workers)
					for i := range groups {
						groups[i] = d
						if queue == "table" {
							groups[i] = &decoderGroup{
								input:   make(chan *decodeTask, 64),
								workers: 1,
								memory:  memory,
								schemas: d.schemas,
							}
						}
						if queue != "table" && i != 0 {
							continue
						}
						groups[i].start(ctx, g)
					}
					tasks := make([]*decodeTask, batchRows)
					b.ReportAllocs()
					for b.Loop() {
						record, err := progress.newAck(ctx, 128, nil)
						if err != nil {
							b.Fatal(err)
						}
						for i := range tasks {
							id := i % len(values)
							if workload == "skewed" {
								id = 0
								if i%10 == 0 {
									id = (i / 10) % len(values)
								}
							}
							decoder.AddKeyValue(nil, values[id])
							if _, hasNext := decoder.HasNext(); !hasNext {
								b.Fatal("Canal JSON fixture has no row")
							}
							message := decoder.NextDMLMessage()
							task := &decodeTask{
								message:       message,
								record:        record,
								done:          make(chan struct{}),
								retainedBytes: 128,
							}
							tasks[i] = task
							if err := groups[uint64(message.TableID)%uint64(len(groups))].submit(ctx, task); err != nil {
								b.Fatal(err)
							}
						}
						record.decoded(128)
						for _, task := range tasks {
							select {
							case <-task.done:
							case <-ctx.Done():
								b.Fatal(context.Cause(ctx))
							}
							task.dml.PostFlush()
							memory.release(task.bytes)
						}
						record.confirm()
					}
				})
			}
		})
	}
}
