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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func TestDMLEventBasicEncodeAndDecode(t *testing.T) {
	mockDecodeRawKVToChunk := func(
		rawKV *common.RawKVEntry,
		tableInfo *common.TableInfo,
		chk *chunk.Chunk,
	) (int, *integrity.Checksum, error) {
		if rawKV.OpType == common.OpTypeDelete {
			return 1, nil, nil
		}
		if rawKV.IsUpdate() {
			return 2, nil, nil
		} else {
			return 1, nil, nil
		}
	}

	e := NewDMLEvent(common.NewDispatcherID(), 1, 100, 200, &common.TableInfo{})
	// append some rows to the event
	{
		// mock a chunk to pass e.Rows.GetRow(), otherwise it will panic
		e.Rows = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, 1)

		// insert
		err := e.AppendRow(&common.RawKVEntry{
			OpType: common.OpTypePut,
			Value:  []byte("value1"),
		}, mockDecodeRawKVToChunk, nil)
		require.Nil(t, err)
		// update
		err = e.AppendRow(&common.RawKVEntry{
			OpType:   common.OpTypePut,
			Value:    []byte("value1"),
			OldValue: []byte("old_value1"),
		}, mockDecodeRawKVToChunk, nil)
		require.Nil(t, err)
		// delete
		err = e.AppendRow(&common.RawKVEntry{
			OpType: common.OpTypeDelete,
		}, mockDecodeRawKVToChunk, nil)
		require.Nil(t, err)
	}
	// TableInfo is not encoded, for test comparison purpose, set it to nil.
	e.TableInfo = nil
	e.Rows = nil

	value, err := e.encode()
	require.Nil(t, err)
	reverseEvent := &DMLEvent{}
	err = reverseEvent.decode(value)
	require.Nil(t, err)
	require.Equal(t, e, reverseEvent)
}

// TestBatchDMLEvent test the Marshal and Unmarshal of BatchDMLEvent.
func TestBatchDMLEvent(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	dmlEvent.State = EventSenderStatePaused
	require.NotNil(t, dmlEvent)

	batchDMLEvent := &BatchDMLEvent{
		DMLEvents: []*DMLEvent{dmlEvent},
		Rows:      dmlEvent.Rows,
		TableInfo: dmlEvent.TableInfo,
	}
	data, err := batchDMLEvent.Marshal()
	require.NoError(t, err)

	reverseEvents := &BatchDMLEvent{}
	// Set the TableInfo before unmarshal, it is used in Unmarshal.
	err = reverseEvents.Unmarshal(data)
	require.NoError(t, err)
	reverseEvents.AssembleRows(batchDMLEvent.TableInfo)
	require.Equal(t, len(reverseEvents.DMLEvents), 1)
	reverseEvent := reverseEvents.DMLEvents[0]
	// Compare the content of the two event's rows.
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	require.True(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	require.Equal(t, dmlEvent.TableInfo.GetFieldSlice(), reverseEvent.TableInfo.GetFieldSlice())
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}

func TestEncodeAndDecodeV0(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.encodeV0()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{
		Version: DMLEventVersion,
	}
	// Set the TableInfo before decode, it is used in decode.
	err = reverseEvent.decodeV0(data)
	require.NoError(t, err)

	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}
