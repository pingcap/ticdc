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

package event

import (
	"testing"
	"time"

	commonpkg "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestFilterDMLEventNormalTablePassthrough(t *testing.T) {
	ti := newTestTableInfo(t, false, false)
	event := newDMLEventForTest(t, ti, []commonpkg.RowType{commonpkg.RowTypeInsert}, [][]interface{}{
		{int64(1)},
	})

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.False(t, skip)
	require.Equal(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())
}

func TestFilterDMLEventActiveActiveWithEnableDropsDeletes(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeDelete, commonpkg.RowTypeInsert},
		[][]interface{}{
			{int64(1), nil}, // delete row pre image
			{int64(2), nil}, // insert row
		})

	filtered, skip := FilterDMLEvent(event, true, nil)
	require.False(t, skip)
	require.NotEqual(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeInsert, row.RowType)
	require.True(t, row.PreRow.IsEmpty())
	require.Equal(t, int64(2), row.Row.GetInt64(0))
	require.False(t, row.Row.IsEmpty())
	filtered.Rewind()
}

func TestFilterDMLEventActiveActiveSkipsDeleteButKeepsFollowingRows(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 3, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeDelete, commonpkg.RowTypeUpdate, commonpkg.RowTypeInsert},
		[][]interface{}{
			{int64(1), nil}, // delete row pre image
			{int64(2), nil}, // update pre row
			{int64(2), ts},  // update post row
			{int64(3), nil}, // insert row
		})

	filtered, skip := FilterDMLEvent(event, true, nil)
	require.False(t, skip)
	require.NotNil(t, filtered)
	require.NotEqual(t, event, filtered)
	require.Equal(t, int32(2), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeUpdate, row.RowType)
	require.Equal(t, int64(2), row.Row.GetInt64(0))

	row, ok = filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeInsert, row.RowType)
	require.Equal(t, int64(3), row.Row.GetInt64(0))

	_, ok = filtered.GetNextRow()
	require.False(t, ok)
	filtered.Rewind()
}

func TestFilterDMLEventSoftDeleteConvertUpdate(t *testing.T) {
	ti := newTestTableInfo(t, false, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 0, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(1), nil}, // pre row
			{int64(1), ts},  // post row with soft delete timestamp
		})

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.False(t, skip)
	require.NotEqual(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeDelete, row.RowType)
	require.True(t, row.Row.IsEmpty())
	require.False(t, row.PreRow.IsEmpty())
	require.Equal(t, int64(1), row.PreRow.GetInt64(0))
	filtered.Rewind()
}

func TestFilterDMLEventSoftDeleteTransitionNullSemantics(t *testing.T) {
	ti := newTestTableInfo(t, false, true)
	preTs := newTimestampValue(time.Date(2025, time.March, 10, 4, 0, 0, 0, time.UTC))
	postTs := newTimestampValue(time.Date(2025, time.March, 10, 5, 0, 0, 0, time.UTC))

	testCases := []struct {
		name        string
		preValue    interface{}
		postValue   interface{}
		wantConvert bool
	}{
		{
			name:        "null to non null",
			preValue:    nil,
			postValue:   postTs,
			wantConvert: true,
		},
		{
			name:        "non null to non null",
			preValue:    preTs,
			postValue:   postTs,
			wantConvert: false,
		},
		{
			name:        "non null to null",
			preValue:    preTs,
			postValue:   nil,
			wantConvert: false,
		},
		{
			name:        "null to null",
			preValue:    nil,
			postValue:   nil,
			wantConvert: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			event := newDMLEventForTest(t, ti,
				[]commonpkg.RowType{commonpkg.RowTypeUpdate},
				[][]interface{}{
					{int64(1), tc.preValue},
					{int64(1), tc.postValue},
				})

			filtered, skip := FilterDMLEvent(event, false, nil)
			require.False(t, skip)
			require.NotNil(t, filtered)

			row, ok := filtered.GetNextRow()
			require.True(t, ok)

			if tc.wantConvert {
				require.NotEqual(t, event, filtered)
				require.Equal(t, commonpkg.RowTypeDelete, row.RowType)
				require.True(t, row.Row.IsEmpty())
				require.False(t, row.PreRow.IsEmpty())
				require.True(t, row.PreRow.IsNull(1))
			} else {
				require.Equal(t, event, filtered)
				require.Equal(t, commonpkg.RowTypeUpdate, row.RowType)
				require.False(t, row.PreRow.IsEmpty())
				require.False(t, row.Row.IsEmpty())
				require.Equal(t, tc.preValue == nil, row.PreRow.IsNull(1))
				require.Equal(t, tc.postValue == nil, row.Row.IsNull(1))
			}

			filtered.Rewind()
		})
	}
}

func TestFilterDMLEventActiveActiveConvertWhenDisabled(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 1, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(2), nil},
			{int64(2), ts},
		})

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.False(t, skip)
	require.NotEqual(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeDelete, row.RowType)
	require.True(t, row.Row.IsEmpty())
	require.False(t, row.PreRow.IsEmpty())
	require.Equal(t, int64(2), row.PreRow.GetInt64(0))
	filtered.Rewind()
}

func TestFilterDMLEventConvertUpdateToDeleteClearsCurrentChecksum(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 1, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(2), nil},
			{int64(2), ts},
		})
	event.Checksum = []*integrity.Checksum{
		{Current: 1, Previous: 2},
	}

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.False(t, skip)
	require.NotEqual(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeDelete, row.RowType)
	require.True(t, row.Row.IsEmpty())
	require.False(t, row.PreRow.IsEmpty())
	require.NotNil(t, row.Checksum)
	require.Equal(t, uint32(0), row.Checksum.Current)
	require.Equal(t, uint32(2), row.Checksum.Previous)
	filtered.Rewind()
}

func TestFilterDMLEventActiveActiveKeepUpdateWhenEnabled(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 2, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(3), nil},
			{int64(3), ts},
		})

	filtered, skip := FilterDMLEvent(event, true, nil)
	require.False(t, skip)
	require.Equal(t, event, filtered)
	require.Equal(t, int32(1), filtered.Len())

	row, ok := filtered.GetNextRow()
	require.True(t, ok)
	require.Equal(t, commonpkg.RowTypeUpdate, row.RowType)
	require.False(t, row.PreRow.IsEmpty())
	require.False(t, row.Row.IsEmpty())
	require.Equal(t, int64(3), row.Row.GetInt64(0))
	filtered.Rewind()
}

func TestFilterDMLEventAllRowsSkipped(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeDelete},
		[][]interface{}{
			{int64(1), nil},
		})

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.True(t, skip)
	require.Nil(t, filtered)
}

func TestFilterDMLEventMissingSoftDeleteColumnReportsError(t *testing.T) {
	ti := newTestTableInfo(t, true, false)
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeInsert},
		[][]interface{}{
			{int64(1)},
		})

	var handledErr error
	filtered, skip := FilterDMLEvent(event, true, func(err error) { handledErr = err })
	require.True(t, skip)
	require.Nil(t, filtered)
	require.Error(t, handledErr)
	require.Contains(t, handledErr.Error(), SoftDeleteTimeColumn)
}

func TestFilterDMLEventSoftDeleteTableMissingColumnReportsError(t *testing.T) {
	ti := newTestTableInfo(t, false, false)
	ti.SoftDeleteTable = true
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(1)},
			{int64(1)},
		})

	var handledErr error
	filtered, skip := FilterDMLEvent(event, false, func(err error) { handledErr = err })
	require.True(t, skip)
	require.Nil(t, filtered)
	require.Error(t, handledErr)
	require.Contains(t, handledErr.Error(), SoftDeleteTimeColumn)
}

func TestFilterDMLEventKeepsPostEnqueueCallbacksOnFilteredEvent(t *testing.T) {
	ti := newTestTableInfo(t, true, true)
	ts := newTimestampValue(time.Date(2025, time.March, 10, 0, 0, 0, 0, time.UTC))
	event := newDMLEventForTest(t, ti,
		[]commonpkg.RowType{commonpkg.RowTypeUpdate},
		[][]interface{}{
			{int64(1), nil},
			{int64(1), ts},
		})

	var enqueueCalled atomic.Int64
	var flushCalled atomic.Int64
	event.AddPostEnqueueFunc(func() {
		enqueueCalled.Inc()
	})
	event.AddPostFlushFunc(func() {
		flushCalled.Inc()
	})

	filtered, skip := FilterDMLEvent(event, false, nil)
	require.False(t, skip)
	require.NotNil(t, filtered)
	require.NotEqual(t, event, filtered)

	filtered.PostEnqueue()
	filtered.PostFlush()
	require.Equal(t, int64(1), enqueueCalled.Load())
	require.Equal(t, int64(1), flushCalled.Load())
}

func newTestTableInfo(t *testing.T, activeActive, softDelete bool) *commonpkg.TableInfo {
	idCol := newTestColumn(1, "id", mysql.TypeLong, mysql.PriKeyFlag)
	cols := []*model.ColumnInfo{idCol}
	if softDelete {
		softCol := newTestColumn(2, SoftDeleteTimeColumn, mysql.TypeTimestamp, 0)
		softCol.FieldType.SetDecimal(tidbTypes.MaxFsp)
		cols = append(cols, softCol)
	}
	table := &model.TableInfo{
		ID:         time.Now().UnixNano(),
		Name:       ast.NewCIStr("t"),
		Columns:    cols,
		State:      model.StatePublic,
		PKIsHandle: true,
	}
	for i, col := range table.Columns {
		col.Offset = i
	}
	ti := commonpkg.WrapTableInfo("test", table)
	ti.ActiveActiveTable = activeActive
	ti.SoftDeleteTable = softDelete
	ti.InitPrivateFields()
	return ti
}

func newTestColumn(id int64, name string, tp byte, flag uint) *model.ColumnInfo {
	ft := tidbTypes.NewFieldType(tp)
	ft.AddFlag(flag)
	return &model.ColumnInfo{
		ID:        id,
		Name:      ast.NewCIStr(name),
		FieldType: *ft,
		State:     model.StatePublic,
		Version:   model.CurrLatestColumnInfoVersion,
	}
}

func newDMLEventForTest(t *testing.T, tableInfo *commonpkg.TableInfo, rowTypes []commonpkg.RowType, rows [][]interface{}) *DMLEvent {
	require.Equal(t, rowSlots(rowTypes), len(rows))
	chk := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), len(rows))
	for _, values := range rows {
		appendRowToChunk(t, chk, values)
	}
	event := NewDMLEvent(commonpkg.NewDispatcherID(), tableInfo.TableName.TableID, 1, 1, tableInfo)
	event.SetRows(chk)
	for _, rowType := range rowTypes {
		event.RowTypes = append(event.RowTypes, rowType)
		if rowType == commonpkg.RowTypeUpdate {
			event.RowTypes = append(event.RowTypes, rowType)
		}
	}
	event.Length = int32(len(rowTypes))
	return event
}

func appendRowToChunk(t *testing.T, chk *chunk.Chunk, values []interface{}) {
	require.Equal(t, chk.NumCols(), len(values))
	for idx, val := range values {
		switch v := val.(type) {
		case nil:
			chk.AppendNull(idx)
		case int64:
			chk.AppendInt64(idx, v)
		case uint64:
			chk.AppendUint64(idx, v)
		case tidbTypes.Time:
			chk.AppendTime(idx, v)
		default:
			require.Failf(t, "unsupported value type", "%T at column %d", val, idx)
		}
	}
}

func rowSlots(rowTypes []commonpkg.RowType) int {
	total := 0
	for _, rt := range rowTypes {
		if rt == commonpkg.RowTypeUpdate {
			total += 2
			continue
		}
		total++
	}
	return total
}

func newTimestampValue(ts time.Time) tidbTypes.Time {
	return tidbTypes.NewTime(tidbTypes.FromGoTime(ts.UTC()), mysql.TypeTimestamp, tidbTypes.MaxFsp)
}
