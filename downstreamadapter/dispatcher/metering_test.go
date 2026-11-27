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

package dispatcher

import (
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func newMeteringTestTableInfo(withVirtual bool, unknownCol bool) *common.TableInfo {
	thirdType := types.NewFieldType(mysql.TypeVarchar)
	if unknownCol {
		thirdType = types.NewFieldType(mysql.TypeGeometry)
	}
	cols := []*model.ColumnInfo{
		{
			ID:        1,
			Name:      ast.NewCIStr("id"),
			FieldType: *types.NewFieldType(mysql.TypeLong),
			State:     model.StatePublic,
		},
		{
			ID:        2,
			Name:      ast.NewCIStr("name"),
			FieldType: *types.NewFieldType(mysql.TypeVarchar),
			State:     model.StatePublic,
		},
		{
			ID:        3,
			Name:      ast.NewCIStr("secret"),
			FieldType: *thirdType,
			State:     model.StatePublic,
		},
	}
	if withVirtual {
		cols = append(cols, &model.ColumnInfo{
			ID:                  4,
			Name:                ast.NewCIStr("virt"),
			FieldType:           *types.NewFieldType(mysql.TypeLong),
			GeneratedExprString: "id + 1",
			GeneratedStored:     false,
			State:               model.StatePublic,
		})
	}
	return common.WrapTableInfo("test", &model.TableInfo{
		ID:      100,
		Name:    ast.NewCIStr("t"),
		Columns: cols,
		State:   model.StatePublic,
	})
}

func newMeteringTestDMLEvent(tableInfo *common.TableInfo) *commonEvent.DMLEvent {
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 4)
	// Row 0: Insert (id=1 [4B], name="ab" [2B], secret="xyz" [3B]) => 9B
	chk.AppendInt64(0, 1)
	chk.AppendString(1, "ab")
	chk.AppendString(2, "xyz")
	if len(tableInfo.GetColumns()) == 4 {
		chk.AppendNull(3)
	}
	// Row 1 (PreRow) & Row 2 (Row): Update => 9B + 9B = 18B
	chk.AppendInt64(0, 1)
	chk.AppendString(1, "ab")
	chk.AppendString(2, "xyz")
	if len(tableInfo.GetColumns()) == 4 {
		chk.AppendNull(3)
	}
	chk.AppendInt64(0, 1)
	chk.AppendString(1, "abcd")
	chk.AppendNull(2)
	if len(tableInfo.GetColumns()) == 4 {
		chk.AppendNull(3)
	}
	// Row 3: Delete PreRow (id=2 [4B], name="" [0B], secret="k" [1B]) => 5B
	chk.AppendInt64(0, 2)
	chk.AppendString(1, "")
	chk.AppendString(2, "k")
	if len(tableInfo.GetColumns()) == 4 {
		chk.AppendNull(3)
	}

	return &commonEvent.DMLEvent{
		TableInfo: tableInfo,
		Rows:      chk,
		RowTypes:  []common.RowType{common.RowTypeInsert, common.RowTypeUpdate, common.RowTypeUpdate, common.RowTypeDelete},
		Length:    3,
	}
}

func TestDispatcherTrafficMeteringAndColumnSelectors(t *testing.T) {
	tableInfo := newMeteringTestTableInfo(true, false)

	selectors, err := columnselector.New(&config.SinkConfig{
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"test.t"},
				Columns: []string{"id", "name"},
			},
		},
	}, false)
	require.NoError(t, err)

	for _, tc := range []struct {
		name      string
		mode      int64
		selectors *columnselector.ColumnSelectors
		expected  uint64
	}{
		{name: "mysql all columns", mode: common.DefaultMode, selectors: nil, expected: 32},
		{name: "mq filtered columns", mode: common.DefaultMode, selectors: selectors, expected: 24},
		{name: "redo excluded", mode: common.RedoMode, selectors: nil, expected: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var selectorFn func(*common.TableInfo) commonEvent.Selector
			if tc.selectors != nil {
				selectors := tc.selectors
				selectorFn = func(info *common.TableInfo) commonEvent.Selector {
					return selectors.GetForTableInfo(info)
				}
			}
			var counter atomic.Uint64
			sharedInfo := newTestSharedInfo(false, false, nil)
			sharedInfo.SetTrafficMetering(selectorFn, func(n uint64) { counter.Add(n) })

			testSink := newDispatcherTestSink(t, common.MysqlSinkType)
			d := &BasicDispatcher{
				mode:          tc.mode,
				sink:          testSink.sink,
				sharedInfo:    sharedInfo,
				tableProgress: NewTableProgress(),
			}

			event := newMeteringTestDMLEvent(tableInfo)
			ok := d.AddDMLEventsToSink([]*commonEvent.DMLEvent{event}, func() {})
			require.True(t, ok)
			require.Len(t, testSink.dmls, 1)

			// Counter must only increment when PostFlush runs, not before flush.
			require.Zero(t, counter.Load())

			// Sink must still see all 3 rows because measurement rewinds the event.
			rowsSeen := 0
			for {
				if _, hasNext := testSink.dmls[0].GetNextRow(); !hasNext {
					break
				}
				rowsSeen++
			}
			require.Equal(t, 3, rowsSeen)

			testSink.dmls[0].PostFlush()
			require.Equal(t, tc.expected, counter.Load())
		})
	}
}

func TestDispatcherTrafficMeteringFailOpenOnUnknownType(t *testing.T) {
	tableInfo := newMeteringTestTableInfo(false, true)
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	chk.AppendInt64(0, 7)     // 4B
	chk.AppendString(1, "ok") // 2B
	chk.AppendBytes(2, []byte{1, 2, 3})
	dml := &commonEvent.DMLEvent{
		TableInfo: tableInfo,
		Rows:      chk,
		RowTypes:  []common.RowType{common.RowTypeInsert},
		Length:    1,
	}

	var counter atomic.Uint64
	sharedInfo := newTestSharedInfo(false, false, nil)
	sharedInfo.SetTrafficMetering(nil, func(n uint64) { counter.Add(n) })
	testSink := newDispatcherTestSink(t, common.MysqlSinkType)
	d := &BasicDispatcher{
		mode:          common.DefaultMode,
		sink:          testSink.sink,
		sharedInfo:    sharedInfo,
		tableProgress: NewTableProgress(),
	}

	beforeUnsupported := testutil.ToFloat64(meteringMeasureErrorCounter.WithLabelValues("unsupported_type"))
	beforeValueErr := testutil.ToFloat64(meteringMeasureErrorCounter.WithLabelValues("value_error"))
	ok := d.AddDMLEventsToSink([]*commonEvent.DMLEvent{dml}, func() {})
	require.True(t, ok)
	testSink.dmls[0].PostFlush()
	require.Equal(t, uint64(6), counter.Load())
	require.Equal(t, beforeUnsupported+1, testutil.ToFloat64(meteringMeasureErrorCounter.WithLabelValues("unsupported_type")))
	require.Equal(t, beforeValueErr, testutil.ToFloat64(meteringMeasureErrorCounter.WithLabelValues("value_error")))
}
