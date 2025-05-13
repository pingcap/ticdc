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

	"github.com/stretchr/testify/require"
)

// TestDMLEvent test the Marshal and Unmarshal of DMLEvent.
func TestDMLEvent(t *testing.T) {
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

	reverseEvent := &DMLEvent{}
	// Set the TableInfo before decode, it is used in decode.
	err = reverseEvent.decodeV0(data)
	require.NoError(t, err)

	require.False(t, reverseEvent.IsPaused())

	// Compare the remaining content of the two events.
	require.Equal(t, dmlEvent.TableInfo.GetFieldSlice(), reverseEvent.TableInfo.GetFieldSlice())
	dmlEvent.Rows = nil
	reverseEvent.Rows = nil
	reverseEvent.eventSize = 0
	dmlEvent.TableInfo = nil
	reverseEvent.TableInfo = nil
	require.Equal(t, dmlEvent, reverseEvent)
}
