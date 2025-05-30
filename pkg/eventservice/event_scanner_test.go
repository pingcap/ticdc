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
		maxBytes: 1000,
		timeout:  10 * time.Second,
	}
	needScan, dataRange := broker.checkNeedScan(disp, true)
	require.True(t, needScan)
	events, isBroken, err := scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 1, len(events))
	e := events[0]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, e.GetCommitTs(), uint64(102))

	// case 2: Only has resolvedTs and DDL event
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
		`insert into test.t(id,c) values (3, "c3")`,
	}...)
	resolvedTs := kvEvents[len(kvEvents)-1].CRTs + 1
	mockSchemaStore.AppendDDLEvent(tableID, ddlEvent)

	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange = broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	sl = scanLimit{
		maxBytes: 1000,
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 2, len(events))
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())

	// case 3: Contains DDL, DML and resolvedTs events
	// Tests that the scanner can handle mixed event types (DDL + DML + resolvedTs)
	// Event sequence:
	//   DDL(ts=x) -> DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4) -> Resolved(ts=x+5)
	// Expected result:
	// [DDL(x), BatchDML_1[DML(x+1)], BatchDML_2[DML(x+2), DML(x+3), DML(x+4)], Resolved(x+5)]
	err = mockEventStore.AppendEvents(dispatcherID, resolvedTs, kvEvents...)
	require.NoError(t, err)
	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange = broker.checkNeedScan(disp, true)
	require.True(t, needScan)
	sl = scanLimit{
		maxBytes: 1000,
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 4, len(events))
	require.Equal(t, pevent.TypeDDLEvent, events[0].GetType())
	require.Equal(t, pevent.TypeBatchDMLEvent, events[1].GetType())
	require.Equal(t, pevent.TypeBatchDMLEvent, events[2].GetType())
	require.Equal(t, pevent.TypeResolvedEvent, events[3].GetType())
	batchDML1 := events[1].(*pevent.BatchDMLEvent)
	require.Equal(t, int32(1), batchDML1.Len())
	require.Equal(t, batchDML1.DMLEvents[0].GetCommitTs(), kvEvents[0].CRTs)
	batchDML2 := events[2].(*pevent.BatchDMLEvent)
	require.Equal(t, int32(3), batchDML2.Len())
	require.Equal(t, batchDML2.DMLEvents[0].GetCommitTs(), kvEvents[1].CRTs)
	require.Equal(t, batchDML2.DMLEvents[1].GetCommitTs(), kvEvents[2].CRTs)
	require.Equal(t, batchDML2.DMLEvents[2].GetCommitTs(), kvEvents[3].CRTs)

	// case 4: Reaches scan limit, only 1 DDL and 1 DML event scanned
	// Tests that when MaxBytes limit is reached, the scanner returns partial events with isBroken=true
	// Expected result:
	// [DDL(x), BatchDML[DML(x+1)], Resolved(x+1)] (partial events due to size limit)
	//               ▲
	//               └── Scanning interrupted here
	sl = scanLimit{
		maxBytes: 1,
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
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

	// case 5: Tests transaction atomicity during scanning
	// Tests that transactions with same commitTs are scanned atomically (not split even when limit is reached)
	// Modified events: first 3 DMLs have same commitTs=x:
	//   DDL(x) -> DML-1(x+1) -> DML-2(x+1) -> DML-3(x+1) -> DML-4(x+4)
	// Expected result (MaxBytes=1):
	// [DDL(x), BatchDML_1[DML-1(x+1)], BatchDML_2[DML-2(x+1), DML-3(x+1)], Resolved(x+1)]
	//                               ▲
	//                               └── Scanning interrupted here
	// The length of the result here is 4.
	// The DML-1(x+1) will appear separately because it encounters DDL(x), which will immediately append it.
	firstCommitTs := kvEvents[0].CRTs
	for i := 0; i < 3; i++ {
		kvEvents[i].CRTs = firstCommitTs
	}
	sl = scanLimit{
		maxBytes: 1,
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
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

	// case 6: Tests timeout behavior
	// Tests that with Timeout=0, the scanner immediately returns scanned events
	// Expected result:
	// [DDL(x), BatchDML_1[DML(x+1)], BatchDML_2[DML(x+1), DML(x+1)], Resolved(x+1)]
	//                               ▲
	//                               └── Scanning interrupted due to timeout
	sl = scanLimit{
		maxBytes: 1000,
		timeout:  0 * time.Millisecond,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 4, len(events))

	// case 7: Tests DMLs are returned before DDLs when they share same commitTs
	// Tests that DMLs take precedence over DDLs with same commitTs
	// Event sequence after adding fakeDDL(ts=x):
	//   DDL(x) -> DML(x+1) -> DML(x+1) -> DML(x+1) -> fakeDDL(x+1) -> DML(x+4)
	// Expected result:
	// [DDL(x), BatchDML_1[DML(x+1)], BatchDML_2[DML(x+1), DML(x+1)], fakeDDL(x+1), BatchDML_3[DML(x+4)], Resolved(x+5)]
	//                                ▲
	//                                └── DMLs take precedence over DDL with same ts
	fakeDDL := event.DDLEvent{
		FinishedTs: kvEvents[0].CRTs,
		TableInfo:  ddlEvent.TableInfo,
		TableID:    ddlEvent.TableID,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)
	sl = scanLimit{
		maxBytes: 1000,
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 6, len(events))
	// First 2 BatchDMLs should appear before fake DDL
	// BatchDML_1
	firstDML := events[1]
	require.Equal(t, firstDML.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(firstDML.(*pevent.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, kvEvents[0].CRTs, firstDML.GetCommitTs())
	// BatchDML_2
	dml := events[2]
	require.Equal(t, dml.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(dml.(*pevent.BatchDMLEvent).DMLEvents), 2)
	require.Equal(t, kvEvents[2].CRTs, dml.GetCommitTs())
	// Fake DDL should appear after DMLs
	ddl := events[3]
	require.Equal(t, ddl.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, ddl.GetCommitTs())
	require.Equal(t, fakeDDL.FinishedTs, firstDML.GetCommitTs())
	// BatchDML_3
	batchDML3 := events[4]
	require.Equal(t, batchDML3.GetType(), pevent.TypeBatchDMLEvent)
	require.Equal(t, len(batchDML3.(*pevent.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, kvEvents[3].CRTs, batchDML3.GetCommitTs())
	// Resolved
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
	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
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
		maxBytes: int64(1 * eSize),
		timeout:  10 * time.Second,
	}
	events, isBroken, err := scanner.scan(context.Background(), disp, dataRange, sl)
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
		maxBytes: int64(2 * eSize),
		timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
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
		maxBytes: int64(100 * eSize),
		timeout:  10 * time.Second,
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

	events, isBroken, err = scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 8, len(events))
	e = events[5]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, e.GetCommitTs(), fakeDDL2.FinishedTs)
	e = events[6]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, e.GetCommitTs(), fakeDDL3.FinishedTs)
	e = events[7]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}

// TestDMLProcessorProcessNewTransaction tests the processNewTransaction method of dmlProcessor
func TestDMLProcessorProcessNewTransaction(t *testing.T) {
	// Setup helper and table info
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
	}...)
	tableInfo := ddlEvent.TableInfo
	tableID := ddlEvent.TableID
	dispatcherID := common.NewDispatcherID()

	// Create a mock mounter and schema getter
	mockMounter := &mockMounter{}
	mockSchemaGetter := newMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	// Test case 1: Process first transaction without insert cache
	t.Run("FirstTransactionWithoutCache", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)
		rawEvent := kvEvents[0]

		err := processor.processNewTransaction(rawEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		// Verify that a new DML event was created
		require.NotNil(t, processor.currentDML)
		require.Equal(t, dispatcherID, processor.currentDML.GetDispatcherID())
		require.Equal(t, tableID, processor.currentDML.GetTableID())
		require.Equal(t, rawEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, rawEvent.CRTs, processor.currentDML.GetCommitTs())

		// Verify that the DML was added to the batch
		require.Equal(t, 1, len(processor.batchDML.DMLEvents))
		require.Equal(t, processor.currentDML, processor.batchDML.DMLEvents[0])

		// Verify insert cache is empty
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 2: Process new transaction when there are cached insert rows
	t.Run("NewTransactionWithInsertCache", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Setup first transaction
		firstEvent := kvEvents[0]
		err := processor.processNewTransaction(firstEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		// Add some insert rows to cache (simulating split update)
		insertRow1 := &common.RawKVEntry{
			StartTs: firstEvent.StartTs,
			CRTs:    firstEvent.CRTs,
			Key:     []byte("insert_key_1"),
			Value:   []byte("insert_value_1"),
		}
		insertRow2 := &common.RawKVEntry{
			StartTs: firstEvent.StartTs,
			CRTs:    firstEvent.CRTs,
			Key:     []byte("insert_key_2"),
			Value:   []byte("insert_value_2"),
		}
		processor.insertRowCache = append(processor.insertRowCache, insertRow1, insertRow2)

		// Process second transaction
		secondEvent := kvEvents[1]
		err = processor.processNewTransaction(secondEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		// Verify that insert cache has been processed and cleared
		require.Empty(t, processor.insertRowCache)

		// Verify new DML event was created for second transaction
		require.NotNil(t, processor.currentDML)
		require.Equal(t, secondEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, secondEvent.CRTs, processor.currentDML.GetCommitTs())

		// Verify batch now contains two DML events
		require.Equal(t, 2, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(4), processor.batchDML.Len())
	})

	// Test case 3: Process transaction with different table info
	t.Run("TransactionWithDifferentTableInfo", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Create a different table info by cloning and using it directly
		// (In real scenarios, this would come from schema store with different updateTS)
		differentTableInfo := tableInfo

		rawEvent := kvEvents[0]
		err := processor.processNewTransaction(rawEvent, tableID, differentTableInfo, dispatcherID)
		require.NoError(t, err)

		// Verify that the DML event uses the correct table info
		require.NotNil(t, processor.currentDML)
		require.Equal(t, differentTableInfo, processor.currentDML.TableInfo)
		require.Equal(t, differentTableInfo.UpdateTS(), processor.currentDML.TableInfo.UpdateTS())
	})

	// Test case 4: Multiple consecutive transactions
	t.Run("ConsecutiveTransactions", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Process multiple transactions
		for i, event := range kvEvents {
			err := processor.processNewTransaction(event, tableID, tableInfo, dispatcherID)
			require.NoError(t, err)

			// Verify current DML matches the event
			require.NotNil(t, processor.currentDML)
			require.Equal(t, event.StartTs, processor.currentDML.GetStartTs())
			require.Equal(t, event.CRTs, processor.currentDML.GetCommitTs())

			// Verify batch size increases
			require.Equal(t, int32(i+1), processor.batchDML.Len())
		}

		// Verify all events are in the batch
		require.Equal(t, int32(len(kvEvents)), processor.batchDML.Len())
		for i, dmlEvent := range processor.batchDML.DMLEvents {
			require.Equal(t, kvEvents[i].StartTs, dmlEvent.GetStartTs())
			require.Equal(t, kvEvents[i].CRTs, dmlEvent.GetCommitTs())
		}
	})

	// Test case 5: Process transaction with empty insert cache followed by one with cache
	t.Run("EmptyThenNonEmptyCache", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// First transaction - no cache
		firstEvent := kvEvents[0]
		err := processor.processNewTransaction(firstEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)
		require.Empty(t, processor.insertRowCache)

		// Add insert rows to cache
		insertRow := &common.RawKVEntry{
			StartTs: firstEvent.StartTs,
			CRTs:    firstEvent.CRTs,
			Key:     []byte("cached_insert"),
			Value:   []byte("cached_value"),
		}
		processor.insertRowCache = append(processor.insertRowCache, insertRow)

		// Second transaction - should process and clear cache
		secondEvent := kvEvents[1]
		err = processor.processNewTransaction(secondEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		// Verify cache was cleared
		require.Empty(t, processor.insertRowCache)

		// Verify all events are in the batch
		require.Equal(t, 2, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(3), processor.batchDML.Len())
	})
}

// TestDMLProcessorAppendRow tests the appendRow method of dmlProcessor
func TestDMLProcessorAppendRow(t *testing.T) {
	// Setup helper and table info
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, a char(50), b char(50), unique key uk_a(a))`, []string{
		`insert into test.t(id,a,b) values (0, "a0", "b0")`,
		`insert into test.t(id,a,b) values (1, "a1", "b1")`,
		`insert into test.t(id,a,b) values (2, "a2", "b2")`,
	}...)

	tableInfo := ddlEvent.TableInfo
	tableID := ddlEvent.TableID
	dispatcherID := common.NewDispatcherID()

	// Create a mock mounter and schema getter
	mockMounter := &mockMounter{}
	mockSchemaGetter := newMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	// Test case 1: appendRow when no current DML event exists - should return error
	t.Run("NoCurrentDMLEvent", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)
		rawEvent := kvEvents[0]

		err := processor.appendRow(rawEvent)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no current DML event to append to")
	})

	// Test case 2: appendRow for insert operation (non-update)
	t.Run("AppendInsertRow", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		firstEvent := kvEvents[0]
		err := processor.processNewTransaction(firstEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		secondEvent := kvEvents[1]
		err = processor.appendRow(secondEvent)
		require.NoError(t, err)

		// Verify insert cache remains empty (since it's not a split update)
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 3: appendRow for delete operation (non-update)
	t.Run("AppendDeleteRow", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)
		err := processor.processNewTransaction(rawEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)
		err = processor.appendRow(deleteRow)
		require.NoError(t, err)

		// Verify insert cache remains empty
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 4: appendRow for update operation without unique key change
	t.Run("AppendUpdateRowWithoutUKChange", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Create a current DML event first
		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)
		err := processor.processNewTransaction(rawEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)
		err = processor.appendRow(deleteRow)
		require.NoError(t, err)

		insertSQL, updateSQL := "insert into test.t(id,a,b) values (3, 'a3', 'b3')", "update test.t set b = 'b3_updated' where id = 3"
		_, updateEvent := helper.DML2UpdateEvent("test", "t", insertSQL, updateSQL)
		err = processor.appendRow(updateEvent)
		require.NoError(t, err)

		// For normal update without UK change, insert cache should remain empty
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 5: appendRow for update operation with unique key change (split update)
	t.Run("AppendUpdateRowWithUKChange", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Create a current DML event first
		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)
		err := processor.processNewTransaction(rawEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)
		err = processor.appendRow(deleteRow)
		require.NoError(t, err)

		// Generate a real update event that changes unique key using helper
		// This updates the unique key column 'a' from 'a1' to 'a1_new'
		insertSQL, updateSQL := "insert into test.t(id,a,b) values (4, 'a4', 'b4')", "update test.t set a = 'a4_updated' where id = 4"
		_, updateEvent := helper.DML2UpdateEvent("test", "t", insertSQL, updateSQL)
		err = processor.appendRow(updateEvent)
		require.NoError(t, err)

		// Verify insert cache
		require.Len(t, processor.insertRowCache, 1)
		require.Equal(t, common.OpTypePut, processor.insertRowCache[0].OpType)
		require.False(t, processor.insertRowCache[0].IsUpdate())
	})

	// Test case 6: Test multiple appendRow calls
	t.Run("MultipleAppendRows", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		// Create a current DML event first
		rawEvent := kvEvents[0]
		err := processor.processNewTransaction(rawEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		// Append multiple rows of different types using real events generated by helper
		// 1. Append a delete row (converted from insert)
		deleteRow := insertToDeleteRow(kvEvents[1])
		err = processor.appendRow(deleteRow)
		require.NoError(t, err)

		// 2. Append an insert row (use existing kvEvent)
		insertRow := kvEvents[2]
		err = processor.appendRow(insertRow)
		require.NoError(t, err)

		// 3. Append an update row using helper
		insertSQL, updateSQL := "insert into test.t(id,a,b) values (10, 'a10', 'b10')", "update test.t set b = 'b10_updated' where id = 10"
		_, updateEvent := helper.DML2UpdateEvent("test", "t", insertSQL, updateSQL)
		err = processor.appendRow(updateEvent)
		require.NoError(t, err)

		// All operations should succeed and insert cache should remain empty
		require.Empty(t, processor.insertRowCache)
	})
}
