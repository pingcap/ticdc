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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/codec/csv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

func BenchmarkConductorMergeCSV(b *testing.B) {
	for _, count := range []int{1, 1024} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			column := &timodel.ColumnInfo{
				ID: 1, Name: ast.NewCIStr("id"), State: timodel.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			}
			table := common.NewTableInfo4Decoder("test", &timodel.TableInfo{
				ID: 1, Name: ast.NewCIStr("t"), Columns: []*timodel.ColumnInfo{column},
			})
			cfg := codecCommon.NewConfig(config.ProtocolCsv)
			cfg.Delimiter, cfg.Terminator, cfg.NullString = ",", "\n", "\\N"
			cfg.IncludeCommitTs = true
			var raw strings.Builder
			for index := range count {
				fmt.Fprintf(&raw, "I,t,test,150,%d\n", index)
			}
			decoder, err := csv.NewDecoder(b.Context(), cfg, table, []byte(raw.String()))
			if err != nil {
				b.Fatal(err)
			}
			var messages []*codecCommon.DMLMessage
			for {
				_, more := decoder.HasNext()
				if !more {
					break
				}
				messages = append(messages, decoder.NextDMLMessage())
			}
			memory := newMemoryUsage()
			c := &conductor{memory: memory, mergeRows: true}
			var retained int64
			b.ReportAllocs()
			for b.Loop() {
				items := make([]*dmlWrite, 0, len(messages))
				for _, message := range messages {
					row := message.ToDMLEvent()
					bytes := row.Rows.MemoryUsage()
					memory.reserve(bytes)
					items = append(items, &dmlWrite{dml: row, bytes: bytes})
				}
				prepared := c.prepare(items)
				retained = memory.used()
				w := &writer{memory: memory}
				for _, row := range prepared {
					w.addDML(row)
					row.dml.PostFlush()
				}
				if memory.used() != 0 {
					b.Fatalf("quota remains after flush: %d", memory.used())
				}
			}
			b.ReportMetric(float64(retained), "retained-B")
		})
	}
}

func BenchmarkConductorWatermark(b *testing.B) {
	for _, count := range []int{1024, 32768, 262144} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			c := &conductor{watermark: 20}
			w := &writer{}
			for i := range count {
				c.queueDML(&decodeTask{commitTs: 10, tableID: int64(i%100 + 1), stream: int32(i % 4)})
			}
			b.ReportAllocs()
			for b.Loop() {
				c.updateWatermark(w)
			}
		})
	}
}
