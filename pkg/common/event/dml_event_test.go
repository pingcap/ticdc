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
	"encoding/binary"
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

	value, err := e.Marshal()
	require.Nil(t, err)

	// Verify header format: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	require.Greater(t, len(value), 16, "data should include header")
	require.Equal(t, uint32(0xDA7A6A6A), binary.BigEndian.Uint32(value[0:4]), "magic bytes")
	require.Equal(t, uint16(TypeDMLEvent), binary.BigEndian.Uint16(value[4:6]), "event type")
	require.Equal(t, uint16(DMLEventVersion1), binary.BigEndian.Uint16(value[6:8]), "version")

	reverseEvent := &DMLEvent{}
	err = reverseEvent.Unmarshal(value)
	require.Nil(t, err)
	reverseEvent.eventSize = 0
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
	require.NotNil(t, dmlEvent)

	batchDMLEvent := &BatchDMLEvent{
		Version:       BatchDMLEventVersion1,
		DMLEventCount: 1,
		DMLEvents:     []*DMLEvent{dmlEvent},
		Rows:          dmlEvent.Rows,
		TableInfo:     dmlEvent.TableInfo,
	}
	data, err := batchDMLEvent.Marshal()
	require.NoError(t, err)

	// Verify header format: [MAGIC(4B)][EVENT_TYPE(2B)][VERSION(2B)][PAYLOAD_LENGTH(8B)]
	require.Greater(t, len(data), 16, "data should include header")
	require.Equal(t, uint32(0xDA7A6A6A), binary.BigEndian.Uint32(data[0:4]), "magic bytes")
	require.Equal(t, uint16(TypeBatchDMLEvent), binary.BigEndian.Uint16(data[4:6]), "event type")
	require.Equal(t, uint16(BatchDMLEventVersion1), binary.BigEndian.Uint16(data[6:8]), "version")

	reverseEvents := &BatchDMLEvent{}
	// Set the TableInfo before unmarshal, it is used in Unmarshal.
	err = reverseEvents.Unmarshal(data)
	require.NoError(t, err)
	reverseEvents.AssembleRows(batchDMLEvent.TableInfo)
	require.Equal(t, len(reverseEvents.DMLEvents), 1)
	require.Equal(t, reverseEvents.DMLEventCount, batchDMLEvent.DMLEventCount)
	reverseEvent := reverseEvents.DMLEvents[0]
	// Compare the content of the two event's rows.
	require.Equal(t, dmlEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()), reverseEvent.Rows.ToString(dmlEvent.TableInfo.GetFieldSlice()))
	for i := 0; i < dmlEvent.Rows.NumRows(); i++ {
		for j := 0; j < dmlEvent.Rows.NumCols(); j++ {
			require.Equal(t, dmlEvent.Rows.GetRow(i).GetRaw(j), reverseEvent.Rows.GetRow(i).GetRaw(j))
		}
	}

	require.False(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	require.Equal(t, dmlEvent.TableInfo.GetFieldSlice(), reverseEvent.TableInfo.GetFieldSlice())
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)

	// case 2: unsupported version
	batchDMLEvent.Version = 100
	data, err = batchDMLEvent.Marshal()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported BatchDMLEvent version")
}

func TestEncodeAnddecodeV1(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	ddlJob := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	dmlEvent.Seq = 1000
	dmlEvent.Epoch = 10
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.encodeV1()
	require.NoError(t, err)

	reverseEvent := &DMLEvent{
		Version: DMLEventVersion1,
	}
	// Set the TableInfo before decode, it is used in decode.
	err = reverseEvent.decodeV1(data)
	require.NoError(t, err)

	// Compare the remaining content of the two events.
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}

func TestBatchDMLEventAppendWithDifferentTableInfo(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")

	// Create the first table and get its DML event
	ddlJob1 := helper.DDL2Job(createTableSQL)
	require.NotNil(t, ddlJob1)
	dmlEvent1 := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent1)

	// Create a second table with different structure to get different TableInfo
	createTableSQL2 := `create table t2 (
		id int primary key,
		name varchar(50),
		age int
	);`
	ddlJob2 := helper.DDL2Job(createTableSQL2)
	require.NotNil(t, ddlJob2)
	dmlEvent2 := helper.DML2Event("test", "t2", "insert into t2 values (1, 'test', 25);")
	require.NotNil(t, dmlEvent2)

	// Ensure the two events have different TableInfo versions
	require.NotEqual(t, dmlEvent1.TableInfo.GetUpdateTS(), dmlEvent2.TableInfo.GetUpdateTS())

	// Create a BatchDMLEvent and append the first event
	batchEvent := &BatchDMLEvent{}
	err := batchEvent.AppendDMLEvent(dmlEvent1)
	require.NoError(t, err)

	// Try to append the second event with different TableInfo - should fail
	err = batchEvent.AppendDMLEvent(dmlEvent2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "table info version mismatch")
	require.Contains(t, err.Error(), "currentDMLEventTableInfoVersion")
	require.Contains(t, err.Error(), "batchDMLTableInfoVersion")
}

func TestDMLEventHeaderValidation(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	helper.DDL2Job(createTableSQL)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	data, err := dmlEvent.Marshal()
	require.NoError(t, err)

	// Make a copy for manipulation
	data2 := make([]byte, len(data))
	copy(data2, data)

	// Test 1: Invalid magic bytes
	data2[0] = 0xFF
	reverseEvent := &DMLEvent{}
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Restore for next test
	copy(data2, data)

	// Test 2: Wrong event type
	binary.BigEndian.PutUint16(data2[4:6], uint16(TypeBatchDMLEvent))
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected DMLEvent")

	// Restore for next test
	copy(data2, data)

	// Test 3: Unsupported version
	binary.BigEndian.PutUint16(data2[6:8], 99)
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported DMLEvent version")

	// Test 4: Data too short
	shortData := []byte{0xDA, 0x7A, 0x6A}
	err = reverseEvent.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test 5: Incomplete payload
	incompleteData := make([]byte, 16)
	binary.BigEndian.PutUint32(incompleteData[0:4], 0xDA7A6A6A)
	binary.BigEndian.PutUint16(incompleteData[4:6], uint16(TypeDMLEvent))
	binary.BigEndian.PutUint16(incompleteData[6:8], uint16(DMLEventVersion1))
	binary.BigEndian.PutUint64(incompleteData[8:16], 100) // Claim 100 bytes but don't provide them
	err = reverseEvent.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete data")
}

func TestBatchDMLEventHeaderValidation(t *testing.T) {
	helper := NewEventTestHelper(t)
	defer helper.Close()

	helper.tk.MustExec("use test")
	helper.DDL2Job(createTableSQL)

	dmlEvent := helper.DML2Event("test", "t", insertDataSQL)
	require.NotNil(t, dmlEvent)

	batchDMLEvent := &BatchDMLEvent{
		Version:   BatchDMLEventVersion1,
		DMLEvents: []*DMLEvent{dmlEvent},
		Rows:      dmlEvent.Rows,
		TableInfo: dmlEvent.TableInfo,
	}
	data, err := batchDMLEvent.Marshal()
	require.NoError(t, err)

	// Make a copy for manipulation
	data2 := make([]byte, len(data))
	copy(data2, data)

	// Test 1: Invalid magic bytes
	data2[0] = 0xFF
	reverseEvent := &BatchDMLEvent{}
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid magic bytes")

	// Restore for next test
	copy(data2, data)

	// Test 2: Wrong event type
	binary.BigEndian.PutUint16(data2[4:6], uint16(TypeDMLEvent))
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected BatchDMLEvent")

	// Restore for next test
	copy(data2, data)

	// Test 3: Unsupported version
	binary.BigEndian.PutUint16(data2[6:8], 99)
	err = reverseEvent.Unmarshal(data2)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported BatchDMLEvent version")

	// Test 4: Data too short
	shortData := []byte{0xDA, 0x7A, 0x6A}
	err = reverseEvent.Unmarshal(shortData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "data too short")

	// Test 5: Incomplete payload
	incompleteData := make([]byte, 16)
	binary.BigEndian.PutUint32(incompleteData[0:4], 0xDA7A6A6A)
	binary.BigEndian.PutUint16(incompleteData[4:6], uint16(TypeBatchDMLEvent))
	binary.BigEndian.PutUint16(incompleteData[6:8], uint16(BatchDMLEventVersion1))
	binary.BigEndian.PutUint64(incompleteData[8:16], 100) // Claim 100 bytes but don't provide them
	err = reverseEvent.Unmarshal(incompleteData)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incomplete data")
}
