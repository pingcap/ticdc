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

package eventservice

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

type mockMounter struct {
	pevent.Mounter
}

func makeDispatcherReady(disp *dispatcherStat) {
	disp.isHandshaked.Store(true)
	disp.isRunning.Store(true)
	disp.resetTs.Store(disp.info.GetStartTs())
}

func (m *mockMounter) DecodeToChunk(rawKV *common.RawKVEntry, tableInfo *common.TableInfo, chk *chunk.Chunk) (int, *integrity.Checksum, error) {
	return 0, nil, nil
}

func TestEventScanner(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	broker.close()

	mockEventStore := broker.eventStore.(*mockEventStore)
	mockSchemaStore := broker.schemaStore.(*mockSchemaStore)

	disInfo := newMockDispatcherInfoForTest(t)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID
	dispatcherID := disInfo.GetID()

	startTs := uint64(100)
	disp := newDispatcherStat(startTs, disInfo, nil, 0, 0, changefeedStatus)
	makeDispatcherReady(disp)
	broker.addDispatcher(disp.info)

	scanner := newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{})

	// case 1: Only has resolvedTs event
	// Tests that the scanner correctly returns just the resolvedTs event
	// Expected result:
	// [Resolved(ts=102)]
	disp.eventStoreResolvedTs.Store(102)
	sl := scanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	needScan, dataRange := broker.checkNeedScan(disp, true)
	require.True(t, needScan)
	events, isBroken, err := scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 1, len(events))
	e := events[0]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, e.GetCommitTs(), uint64(102))

	// case 2: Contains DDL, DML and resolvedTs events
	// Tests that the scanner can handle mixed event types (DDL + DML + resolvedTs)
	// Event sequence:
	//   DDL(ts=x) -> DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4) -> Resolved(ts=x+5)
	// Expected result:
	// [DDL(x), DML(x+1), DML(x+2), DML(x+3), DML(x+4), Resolved(x+5)]
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
		`insert into test.t(id,c) values (3, "c3")`,
	}...)
	resolvedTs := kvEvents[len(kvEvents)-1].CRTs + 1
	err = mockEventStore.AppendEvents(dispatcherID, resolvedTs, kvEvents...)
	require.NoError(t, err)
	mockSchemaStore.AppendDDLEvent(tableID, ddlEvent)

	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange = broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	sl = scanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 4, len(events))

	// case 3: Reaches scan limit, only 1 DDL and 1 DML event scanned
	// Tests that when MaxBytes limit is reached, the scanner returns partial events with isBroken=true
	// Expected result:
	// [DDL(x), DML(x+1), Resolved(x+1)] (partial events due to size limit)
	//               ▲
	//               └── Scanning interrupted here
	sl = scanLimit{
		MaxBytes: 1,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 3, len(events))
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())

	// case4: Tests transaction atomicity during scanning
	// Tests that transactions with same commitTs are scanned atomically (not split even when limit is reached)
	// Modified events: first 3 DMLs have same commitTs=x:
	//   DDL(x) -> DML-1(x+1) -> DML-2(x+1) -> DML-3(x+1) -> DML-4(x+4)
	// Expected result (MaxBytes=1):
	// [DDL(x), DML-1(x+1), DML-2(x+1), DML-3(x+1), Resolved(x+1)]
	//                               ▲
	//                               └── Scanning interrupted here
	// The length of the result here is 4.
	// The DML-1(x+1) will appear separately because it encounters DDL(x), which will immediately append it.
	firstCommitTs := kvEvents[0].CRTs
	for i := 0; i < 3; i++ {
		kvEvents[i].CRTs = firstCommitTs
	}
	sl = scanLimit{
		MaxBytes: 1,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 4, len(events))

	// DDL
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// DML-1
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(e.(*pevent.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, firstCommitTs, e.GetCommitTs())
	// DML-2, DML-3
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(e.(*pevent.BatchDMLEvent).DMLEvents), 2)
	require.Equal(t, firstCommitTs, e.GetCommitTs())
	// resolvedTs
	e = events[3]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, firstCommitTs, e.GetCommitTs())

	// case 5: Tests timeout behavior
	// Tests that with Timeout=0, the scanner immediately returns scanned events
	// Expected result:
	// [DDL(x), DML(x+1), DML(x+1), DML(x+1), Resolved(x+1)]
	//                               ▲
	//                               └── Scanning interrupted due to timeout
	sl = scanLimit{
		MaxBytes: 1000,
		Timeout:  0 * time.Millisecond,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 4, len(events))

	// case 6: Tests DMLs are returned before DDLs when they share same commitTs
	// Tests that DMLs take precedence over DDLs with same commitTs
	// Event sequence after adding fakeDDL(ts=x):
	//   DDL(x) -> DML(x+1) -> DML(x+1) -> DML(x+1) -> fakeDDL(x+1) -> DML(x+4)
	// Expected result:
	// [DDL(x), DML(x+1), DML(x+1), DML(x+1), fakeDDL(x+1), DML(x+4), Resolved(x+5)]
	//                                ▲
	//                                └── DMLs take precedence over DDL with same ts
	fakeDDL := event.DDLEvent{
		FinishedTs: kvEvents[0].CRTs,
		TableInfo:  ddlEvent.TableInfo,
		TableID:    ddlEvent.TableID,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)
	sl = scanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 6, len(events))
	// First DML should appear before fake DDL
	firstDML := events[1]
	require.Equal(t, firstDML.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(firstDML.(*pevent.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, kvEvents[0].CRTs, firstDML.GetCommitTs())
	// DMLs
	dml := events[2]
	require.Equal(t, dml.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(dml.(*pevent.BatchDMLEvent).DMLEvents), 2)
	require.Equal(t, kvEvents[2].CRTs, dml.GetCommitTs())
	// Fake DDL should appear after DMLs
	ddl := events[3]
	require.Equal(t, ddl.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, ddl.GetCommitTs())
	require.Equal(t, fakeDDL.FinishedTs, firstDML.GetCommitTs())
	e = events[5]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}

// TestEventScannerWithDDL tests cases where scanning is interrupted at DDL events
// The scanner should return the DDL event plus any DML events sharing the same commitTs
// It should also return a resolvedTs event with the commitTs of the last DML event
func TestEventScannerWithDDL(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	broker.close()

	mockEventStore := broker.eventStore.(*mockEventStore)
	mockSchemaStore := broker.schemaStore.(*mockSchemaStore)

	disInfo := newMockDispatcherInfoForTest(t)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID
	dispatcherID := disInfo.GetID()

	startTs := uint64(100)
	disp := newDispatcherStat(startTs, disInfo, nil, 0, 0, changefeedStatus)
	makeDispatcherReady(disp)
	broker.addDispatcher(disp.info)

	scanner := newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{})

	// Construct events: dml2 and dml3 share commitTs, fakeDDL shares commitTs with them
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
		`insert into test.t(id,c) values (3, "c3")`,
	}...)
	resolvedTs := kvEvents[len(kvEvents)-1].CRTs + 1
	err := mockEventStore.AppendEvents(dispatcherID, resolvedTs, kvEvents...)
	require.NoError(t, err)
	mockSchemaStore.AppendDDLEvent(tableID, ddlEvent)

	// Create fake DDL event sharing commitTs with dml2 and dml3
	dml1 := kvEvents[0]
	dml2 := kvEvents[1]
	dml3 := kvEvents[2]
	dml3.CRTs = dml2.CRTs
	fakeDDL := event.DDLEvent{
		FinishedTs: dml2.CRTs,
		TableInfo:  ddlEvent.TableInfo,
		TableID:    ddlEvent.TableID,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)
	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange := broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	eSize := len(kvEvents[0].Key) + len(kvEvents[0].Value) + len(kvEvents[0].OldValue)

	// case 1: Scanning interrupted at dml1
	// Tests interruption at first DML due to size limit
	// Event sequence:
	//   DDL(x) -> DML1(x+1) -> DML2(x+2) ->fakeDDL(x+2) -> DML3(x+3)
	// Expected result (MaxBytes=1*eSize):
	// [DDL(x), DML1(x+1), Resolved(x+1)]
	//             ▲
	//             └── Scanning interrupted at DML1
	sl := scanLimit{
		MaxBytes: int64(1 * eSize),
		Timeout:  10 * time.Second,
	}
	events, isBroken, err := scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 3, len(events))
	// DDL
	e := events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// DML1
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	// resolvedTs
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	require.Equal(t, dml1.CRTs, e.GetCommitTs())

	// case 2: Scanning interrupted at dml2
	// Tests atomic return of DML2/DML3/fakeDDL sharing same commitTs
	// Expected result (MaxBytes=2*eSize):
	// [DDL(x), DML1(x+1), DML2(x+2), DML3(x+2), fakeDDL(x+2), Resolved(x+3)]
	//                                               ▲
	//                                               └── Events with same commitTs must be returned together
	sl = scanLimit{
		MaxBytes: int64(2 * eSize),
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 5, len(events))

	// DDL1
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// DML1
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	// DML2 DML3
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, dml3.CRTs, e.GetCommitTs())
	// fake DDL
	e = events[3]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, e.GetCommitTs())
	require.Equal(t, dml3.CRTs, e.GetCommitTs())
	// resolvedTs
	e = events[4]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, dml3.CRTs, e.GetCommitTs())

	// case 3: Tests handling of multiple DDL events
	// Tests that scanner correctly processes multiple DDL events
	// Event sequence after additions:
	//   ... -> fakeDDL2(x+5) -> fakeDDL3(x+6)
	// Expected result:
	// [..., fakeDDL2(x+5), fakeDDL3(x+6), Resolved(x+7)]
	sl = scanLimit{
		MaxBytes: int64(100 * eSize),
		Timeout:  10 * time.Second,
	}

	// Add more fake DDL events
	fakeDDL2 := event.DDLEvent{
		FinishedTs: resolvedTs + 1,
		TableInfo:  ddlEvent.TableInfo,
		TableID:    ddlEvent.TableID,
	}
	fakeDDL3 := event.DDLEvent{
		FinishedTs: resolvedTs + 2,
		TableInfo:  ddlEvent.TableInfo,
		TableID:    ddlEvent.TableID,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL2)
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL3)
	resolvedTs = resolvedTs + 3
	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange = broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 8, len(events))
	e = events[5]
	require.Equal(t, fakeDDL2.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL2.GetCommitTs(), fakeDDL2.FinishedTs)
	e = events[6]
	require.Equal(t, fakeDDL3.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL3.GetCommitTs(), fakeDDL3.FinishedTs)
	e = events[7]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}
