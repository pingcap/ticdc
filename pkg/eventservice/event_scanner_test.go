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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
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
	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
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

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
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

	// Test 6: First event is update that changes UK
	t.Run("UpdateThatChangesUK", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter)

		helper.Tk().MustExec("use test")
		ddlEvent := helper.DDL2Event("create table t2 (id int primary key, a int(50), b char(50), unique key uk_a(a))")
		tableInfo := ddlEvent.TableInfo
		tableID := ddlEvent.TableID

		_, updateEvent := helper.DML2UpdateEvent("test", "t2", "insert into test.t2(id,a,b) values (0, 1, 'b0')", "update test.t2 set a = 2 where id = 0")
		err := processor.processNewTransaction(updateEvent, tableID, tableInfo, dispatcherID)
		require.NoError(t, err)

		require.NotNil(t, processor.currentDML)
		require.Equal(t, updateEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, updateEvent.CRTs, processor.currentDML.GetCommitTs())

		require.Equal(t, 1, len(processor.insertRowCache))
		require.Equal(t, common.OpTypePut, processor.insertRowCache[0].OpType)
		require.False(t, processor.insertRowCache[0].IsUpdate())
	})
}

// TestDMLProcessorAppendRow tests the appendRow method of dmlProcessor
func TestDMLProcessorAppendRow(t *testing.T) {
	// Setup helper and table info
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, a char(50), b char(50), unique key uk_a(a))`, []string{
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

func TestScanSession(t *testing.T) {
	// Test addBytes method
	t.Run("TestAddBytes", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxBytes: 1000, timeout: time.Second}

		scanner := &eventScanner{}
		session := scanner.newScanSession(ctx, dispStat, dataRange, limit)

		// Test initial totalBytes is 0
		require.Equal(t, int64(0), session.totalBytes)

		// Test adding bytes
		session.addBytes(100)
		require.Equal(t, int64(100), session.totalBytes)

		// Test adding more bytes
		session.addBytes(250)
		require.Equal(t, int64(350), session.totalBytes)

		// Test adding negative bytes (edge case)
		session.addBytes(-50)
		require.Equal(t, int64(300), session.totalBytes)

		// Test adding zero bytes
		session.addBytes(0)
		require.Equal(t, int64(300), session.totalBytes)
	})

	// Test isContextDone method
	t.Run("TestIsContextDone", func(t *testing.T) {
		// Test with normal context
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxBytes: 1000, timeout: time.Second}

		scanner := &eventScanner{}
		session := scanner.newScanSession(ctx, dispStat, dataRange, limit)

		// Context should not be done initially
		require.False(t, session.isContextDone())

		// Test with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		sessionWithCancelCtx := scanner.newScanSession(cancelCtx, dispStat, dataRange, limit)

		// Context should not be done before cancellation
		require.False(t, sessionWithCancelCtx.isContextDone())

		// Cancel the context
		cancel()

		// Context should be done after cancellation
		require.True(t, sessionWithCancelCtx.isContextDone())

		// Test with timeout context
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer timeoutCancel()
		sessionWithTimeoutCtx := scanner.newScanSession(timeoutCtx, dispStat, dataRange, limit)

		// Context should not be done initially
		require.False(t, sessionWithTimeoutCtx.isContextDone())

		// Wait for timeout
		time.Sleep(10 * time.Millisecond)

		// Context should be done after timeout
		require.True(t, sessionWithTimeoutCtx.isContextDone())
	})

	// Test scanSession initialization
	t.Run("TestSessionInitialization", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{
			Span:    &heartbeatpb.TableSpan{TableID: 123},
			StartTs: 100,
			EndTs:   200,
		}
		limit := scanLimit{maxBytes: 1000, timeout: time.Second}

		scanner := &eventScanner{}
		session := scanner.newScanSession(ctx, dispStat, dataRange, limit)

		// Verify initialization
		require.Equal(t, ctx, session.ctx)
		require.Equal(t, dispStat, session.dispatcherStat)
		require.Equal(t, dataRange, session.dataRange)
		require.Equal(t, limit, session.limit)
		require.Equal(t, int64(0), session.totalBytes)
		require.Equal(t, uint64(0), session.lastCommitTs)
		require.Equal(t, 0, session.dmlCount)
		require.NotNil(t, session.events)
		require.Equal(t, 0, len(session.events))
		require.True(t, time.Since(session.startTime) >= 0)
	})

	// Test session state tracking
	t.Run("TestSessionStateTracking", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxBytes: 1000, timeout: time.Second}

		scanner := &eventScanner{}
		session := scanner.newScanSession(ctx, dispStat, dataRange, limit)

		// Test updating state fields
		session.lastCommitTs = 150
		session.dmlCount = 5

		require.Equal(t, uint64(150), session.lastCommitTs)
		require.Equal(t, 5, session.dmlCount)

		// Test events collection
		require.NotNil(t, session.events)
		require.Equal(t, 0, len(session.events))

		// Events slice should be mutable
		session.events = append(session.events, nil) // Add a nil event for testing
		require.Equal(t, 1, len(session.events))
	})
}

func TestLimitChecker(t *testing.T) {
	// Test case 1: Test checkLimits method - byte limit exceeded
	t.Run("TestByteLimitExceeded", func(t *testing.T) {
		startTime := time.Now()
		maxBytes := int64(1000)
		timeout := 10 * time.Second

		checker := newLimitChecker(maxBytes, timeout, startTime)

		// Test bytes below limit
		totalBytes := int64(500)
		require.False(t, checker.checkLimits(totalBytes))

		// Test bytes at limit
		totalBytes = int64(1000)
		require.False(t, checker.checkLimits(totalBytes))

		// Test bytes exceeding limit
		totalBytes = int64(1001)
		require.True(t, checker.checkLimits(totalBytes))

		// Test significantly exceeding limit
		totalBytes = int64(2000)
		require.True(t, checker.checkLimits(totalBytes))
	})

	// Test case 2: Test checkLimits method - timeout exceeded
	t.Run("TestTimeoutExceeded", func(t *testing.T) {
		startTime := time.Now().Add(-5 * time.Second) // Simulate 5 seconds ago
		maxBytes := int64(1000)
		timeout := 3 * time.Second

		checker := newLimitChecker(maxBytes, timeout, startTime)

		// Test with bytes under limit but timeout exceeded
		totalBytes := int64(500)
		require.True(t, checker.checkLimits(totalBytes))

		// Test with both bytes and timeout exceeded
		totalBytes = int64(500)
		require.True(t, checker.checkLimits(totalBytes))

		// Test fresh checker (no timeout)
		freshStartTime := time.Now()
		freshChecker := newLimitChecker(maxBytes, timeout, freshStartTime)
		require.False(t, freshChecker.checkLimits(totalBytes))
	})

	// Test case 3: Test canInterrupt method - interrupt conditions
	t.Run("TestCanInterrupt", func(t *testing.T) {
		startTime := time.Now()
		maxBytes := int64(1000)
		timeout := 10 * time.Second

		checker := newLimitChecker(maxBytes, timeout, startTime)

		// Test cannot interrupt when currentTs <= lastCommitTs
		currentTs := uint64(100)
		lastCommitTs := uint64(100)
		dmlCount := 5
		require.False(t, checker.canInterrupt(currentTs, lastCommitTs, dmlCount))

		// Test cannot interrupt when currentTs < lastCommitTs
		currentTs = uint64(99)
		lastCommitTs = uint64(100)
		require.False(t, checker.canInterrupt(currentTs, lastCommitTs, dmlCount))

		// Test cannot interrupt when dmlCount = 0
		currentTs = uint64(101)
		lastCommitTs = uint64(100)
		dmlCount = 0
		require.False(t, checker.canInterrupt(currentTs, lastCommitTs, dmlCount))

		// Test can interrupt when currentTs > lastCommitTs and dmlCount > 0
		currentTs = uint64(101)
		lastCommitTs = uint64(100)
		dmlCount = 1
		require.True(t, checker.canInterrupt(currentTs, lastCommitTs, dmlCount))
	})
}

func TestEventMerger(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	mounter := pevent.NewMounter(time.UTC, &integrity.Config{})
	t.Run("NoDDLEvents", func(t *testing.T) {
		merger := newEventMerger(nil, dispatcherID)

		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()
		ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
			`insert into test.t(id,c) values (0, "c0")`,
		}...)

		batchDML := pevent.NewBatchDMLEvent()
		dmlEvent := pevent.NewDMLEvent(dispatcherID, ddlEvent.TableID, kvEvents[0].StartTs, kvEvents[0].CRTs, ddlEvent.TableInfo)
		batchDML.AppendDMLEvent(dmlEvent)
		dmlEvent.AppendRow(kvEvents[0], mounter.DecodeToChunk)

		var lastCommitTs uint64
		events := merger.appendDMLEvent(batchDML, &lastCommitTs)

		// Only return DML event
		require.Equal(t, 1, len(events))
		require.Equal(t, pevent.TypeBatchDMLEvent, events[0].GetType())
		require.Equal(t, kvEvents[0].CRTs, events[0].GetCommitTs())
		require.Equal(t, kvEvents[0].CRTs, lastCommitTs)

		// appendRemainingDDLs should only return resolvedTs event
		endTs := uint64(200)
		remainingEvents := merger.appendRemainingDDLs(endTs)
		require.Equal(t, 1, len(remainingEvents))
		require.Equal(t, pevent.TypeResolvedEvent, remainingEvents[0].GetType())
		require.Equal(t, endTs, remainingEvents[0].GetCommitTs())
	})

	t.Run("MixedDMLAndDDLEvents", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		ddlEvent1, kvEvents1 := genEvents(helper, `create table test.t1(id int primary key, c char(50))`, []string{
			`insert into test.t1(id,c) values (1, "c1")`,
		}...)
		ddlEvent2 := event.DDLEvent{
			FinishedTs: kvEvents1[0].CRTs + 10, // DDL2 after DML1
			TableInfo:  ddlEvent1.TableInfo,
			TableID:    ddlEvent1.TableID,
		}
		ddlEvent3 := event.DDLEvent{
			FinishedTs: kvEvents1[0].CRTs + 30, // DDL3 after DML1
			TableInfo:  ddlEvent1.TableInfo,
			TableID:    ddlEvent1.TableID,
		}

		ddlEvents := []pevent.DDLEvent{ddlEvent1, ddlEvent2, ddlEvent3}
		merger := newEventMerger(ddlEvents, dispatcherID)

		// Create first DML event (timestamp after DDL1, before DDL2)
		batchDML1 := pevent.NewBatchDMLEvent()
		dmlEvent1 := pevent.NewDMLEvent(dispatcherID, ddlEvent1.TableID, kvEvents1[0].StartTs, kvEvents1[0].CRTs, ddlEvent1.TableInfo)
		batchDML1.AppendDMLEvent(dmlEvent1)
		dmlEvent1.AppendRow(kvEvents1[0], mounter.DecodeToChunk)

		var lastCommitTs uint64
		events1 := merger.appendDMLEvent(batchDML1, &lastCommitTs)

		// Should return DDL1 + DML1
		require.Equal(t, 2, len(events1))
		require.Equal(t, pevent.TypeDDLEvent, events1[0].GetType())
		require.Equal(t, ddlEvent1.FinishedTs, events1[0].GetCommitTs())
		require.Equal(t, pevent.TypeBatchDMLEvent, events1[1].GetType())
		require.Equal(t, kvEvents1[0].CRTs, events1[1].GetCommitTs())

		// Create second DML event (timestamp after DDL2 and DDL3)
		batchDML2 := pevent.NewBatchDMLEvent()
		dmlEvent2 := pevent.NewDMLEvent(dispatcherID, ddlEvent1.TableID, kvEvents1[0].StartTs+50, kvEvents1[0].CRTs+50, ddlEvent1.TableInfo)
		batchDML2.AppendDMLEvent(dmlEvent2)
		dmlEvent2.AppendRow(kvEvents1[0], mounter.DecodeToChunk)

		events2 := merger.appendDMLEvent(batchDML2, &lastCommitTs)

		// Should return DDL2 + DDL3 + DML2
		require.Equal(t, 3, len(events2))
		require.Equal(t, pevent.TypeDDLEvent, events2[0].GetType())
		require.Equal(t, ddlEvent2.FinishedTs, events2[0].GetCommitTs())
		require.Equal(t, pevent.TypeDDLEvent, events2[1].GetType())
		require.Equal(t, ddlEvent3.FinishedTs, events2[1].GetCommitTs())
		require.Equal(t, pevent.TypeBatchDMLEvent, events2[2].GetType())
		require.Equal(t, kvEvents1[0].CRTs+50, events2[2].GetCommitTs())

		// Test appendRemainingDDLs, should only return resolvedTs (all DDLs are processed)
		endTs := uint64(300)
		remainingEvents := merger.appendRemainingDDLs(endTs)
		require.Equal(t, 1, len(remainingEvents))
		require.Equal(t, pevent.TypeResolvedEvent, remainingEvents[0].GetType())
		require.Equal(t, endTs, remainingEvents[0].GetCommitTs())
	})

	t.Run("AppendRemainingDDLsBoundary", func(t *testing.T) {
		helper := commonEvent.NewEventTestHelper(t)
		defer helper.Close()

		ddlEvent1, _ := genEvents(helper, `create table test.t1(id int primary key, c char(50))`, []string{
			`insert into test.t1(id,c) values (1, "c1")`,
		}...)
		ddlEvent2 := event.DDLEvent{
			FinishedTs: 100,
			TableInfo:  ddlEvent1.TableInfo,
			TableID:    ddlEvent1.TableID,
		}
		ddlEvent3 := event.DDLEvent{
			FinishedTs: 200,
			TableInfo:  ddlEvent1.TableInfo,
			TableID:    ddlEvent1.TableID,
		}
		ddlEvent4 := event.DDLEvent{
			FinishedTs: 300,
			TableInfo:  ddlEvent1.TableInfo,
			TableID:    ddlEvent1.TableID,
		}

		ddlEvents := []pevent.DDLEvent{ddlEvent2, ddlEvent3, ddlEvent4}
		merger := newEventMerger(ddlEvents, dispatcherID)

		// Test endTs is exactly equal to some DDL's FinishedTs
		endTs := uint64(200)
		events1 := merger.appendRemainingDDLs(endTs)
		require.Equal(t, 3, len(events1)) // DDL2 + DDL3 + ResolvedEvent
		require.Equal(t, pevent.TypeDDLEvent, events1[0].GetType())
		require.Equal(t, uint64(100), events1[0].GetCommitTs())
		require.Equal(t, pevent.TypeDDLEvent, events1[1].GetType())
		require.Equal(t, uint64(200), events1[1].GetCommitTs())
		require.Equal(t, pevent.TypeResolvedEvent, events1[2].GetType())
		require.Equal(t, endTs, events1[2].GetCommitTs())

		// Recreate merger to test endTs is less than all DDLs
		merger2 := newEventMerger(ddlEvents, dispatcherID)
		endTs2 := uint64(50)
		events2 := merger2.appendRemainingDDLs(endTs2)
		require.Equal(t, 1, len(events2)) // Only ResolvedEvent
		require.Equal(t, pevent.TypeResolvedEvent, events2[0].GetType())
		require.Equal(t, endTs2, events2[0].GetCommitTs())

		// Recreate merger to test endTs is greater than all DDLs
		merger3 := newEventMerger(ddlEvents, dispatcherID)
		endTs3 := uint64(500)
		events3 := merger3.appendRemainingDDLs(endTs3)
		require.Equal(t, 4, len(events3)) // all DDLs + ResolvedEvent
		require.Equal(t, pevent.TypeDDLEvent, events3[0].GetType())
		require.Equal(t, uint64(100), events3[0].GetCommitTs())
		require.Equal(t, pevent.TypeDDLEvent, events3[1].GetType())
		require.Equal(t, uint64(200), events3[1].GetCommitTs())
		require.Equal(t, pevent.TypeDDLEvent, events3[2].GetType())
		require.Equal(t, uint64(300), events3[2].GetCommitTs())
		require.Equal(t, pevent.TypeResolvedEvent, events3[3].GetType())
		require.Equal(t, endTs3, events3[3].GetCommitTs())
	})
}

func TestErrorHandler(t *testing.T) {
	dispatcherID := common.NewDispatcherID()

	// Test handleSchemaError method
	t.Run("HandleSchemaError", func(t *testing.T) {
		errorHandler := newErrorHandler(dispatcherID)

		// Create a mock dispatcher stat
		dispStat := &dispatcherStat{
			id: dispatcherID,
		}
		dispStat.isRemoved.Store(false)

		// Test case 1: Normal error (not TableDeletedError, dispatcher not removed)
		t.Run("NormalSchemaError", func(t *testing.T) {
			normalErr := errors.New("some schema error")
			shouldReturn, returnErr := errorHandler.handleSchemaError(normalErr, dispStat)

			require.True(t, shouldReturn)
			require.Equal(t, normalErr, returnErr)
		})

		// Test case 2: Wrapped TableDeletedError
		t.Run("WrappedTableDeletedError", func(t *testing.T) {
			wrappedErr := fmt.Errorf("wrapped error: %w", &schemastore.TableDeletedError{})
			shouldReturn, returnErr := errorHandler.handleSchemaError(wrappedErr, dispStat)

			require.True(t, shouldReturn)
			require.Nil(t, returnErr) // Should return nil error for wrapped TableDeletedError
		})

		// Test case 3: TableDeletedError when dispatcher is removed (should still return nil)
		t.Run("TableDeletedErrorWithDispatcherRemoved", func(t *testing.T) {
			dispStat.isRemoved.Store(true)
			tableDeletedErr := &schemastore.TableDeletedError{}
			shouldReturn, returnErr := errorHandler.handleSchemaError(tableDeletedErr, dispStat)

			require.True(t, shouldReturn)
			require.Nil(t, returnErr)

			// Reset for other tests
			dispStat.isRemoved.Store(false)
		})
	})

	// Test handleIteratorError method
	t.Run("HandleIteratorError", func(t *testing.T) {
		errorHandler := newErrorHandler(dispatcherID)

		// Test case 1: Normal iterator error
		t.Run("NormalIteratorError", func(t *testing.T) {
			originalErr := errors.New("iterator read failed")
			returnedErr := errorHandler.handleIteratorError(originalErr)

			// Should return the original error unchanged
			require.Equal(t, originalErr, returnedErr)
		})

		// Test case 2: Context canceled error
		t.Run("ContextCanceledError", func(t *testing.T) {
			canceledErr := context.Canceled
			returnedErr := errorHandler.handleIteratorError(canceledErr)

			// Should return the original error unchanged
			require.Equal(t, canceledErr, returnedErr)
		})

		// Test case 3: Wrapped error
		t.Run("WrappedIteratorError", func(t *testing.T) {
			wrappedErr := fmt.Errorf("wrapped: %w", errors.New("underlying iterator error"))
			returnedErr := errorHandler.handleIteratorError(wrappedErr)

			// Should return the original error unchanged
			require.Equal(t, wrappedErr, returnedErr)
		})

		// Test case 4: Nil error (edge case)
		t.Run("NilIteratorError", func(t *testing.T) {
			returnedErr := errorHandler.handleIteratorError(nil)

			// Should return nil unchanged
			require.Nil(t, returnedErr)
		})
	})

	// Test different error types and dispatcher states
	t.Run("ErrorTypesAndDispatcherStates", func(t *testing.T) {
		errorHandler := newErrorHandler(dispatcherID)

		// Test case 1: Multiple error handling scenarios with different dispatcher states
		t.Run("MultipleErrorScenarios", func(t *testing.T) {
			dispStat := &dispatcherStat{
				id: dispatcherID,
			}
			dispStat.isRemoved.Store(false)

			// Scenario 1: Regular error with active dispatcher
			err1 := errors.New("regular error")
			shouldReturn1, returnErr1 := errorHandler.handleSchemaError(err1, dispStat)
			require.True(t, shouldReturn1)
			require.Equal(t, err1, returnErr1)

			// Scenario 2: Change dispatcher state to removed
			dispStat.isRemoved.Store(true)
			shouldReturn2, returnErr2 := errorHandler.handleSchemaError(err1, dispStat)
			require.True(t, shouldReturn2)
			require.Nil(t, returnErr2)

			// Scenario 3: TableDeletedError with removed dispatcher
			tableErr := &schemastore.TableDeletedError{}
			shouldReturn3, returnErr3 := errorHandler.handleSchemaError(tableErr, dispStat)
			require.True(t, shouldReturn3)
			require.Nil(t, returnErr3)

			// Scenario 4: Reset dispatcher state and test TableDeletedError again
			dispStat.isRemoved.Store(false)
			shouldReturn4, returnErr4 := errorHandler.handleSchemaError(tableErr, dispStat)
			require.True(t, shouldReturn4)
			require.Nil(t, returnErr4)
		})
	})
}

// TestScanAndMergeEventsSingleUKUpdate tests scanAndMergeEvents function with a single event that updates UK
func TestScanAndMergeEventsSingleUKUpdate(t *testing.T) {
	// Setup helper and table info with unique key
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	// Create table with unique key on column 'a'
	helper.Tk().MustExec("use test")
	ddlEvent := helper.DDL2Event("create table t_uk (id int primary key, a int, b char(50), unique key uk_a(a))")
	tableID := ddlEvent.TableID
	dispatcherID := common.NewDispatcherID()

	// Generate update event that changes UK
	_, updateEvent := helper.DML2UpdateEvent("test", "t_uk",
		"insert into test.t_uk(id,a,b) values (1, 10, 'old_b')",
		"update test.t_uk set a = 20 where id = 1")

	// Create mock components
	mockSchemaGetter := newMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, *ddlEvent)

	// Create a mock iterator that returns only the single update event
	mockIter := &mockEventIterator{
		events: []*common.RawKVEntry{updateEvent},
	}

	// Create event scanner
	scanner := &eventScanner{
		mounter:      pevent.NewMounter(time.UTC, &integrity.Config{}),
		schemaGetter: mockSchemaGetter,
	}

	// Create scan session
	ctx := context.Background()
	dispatcherStat := &dispatcherStat{
		id:        dispatcherID,
		isRunning: atomic.Bool{},
		isRemoved: atomic.Bool{},
	}
	dispatcherStat.isRunning.Store(true)

	dataRange := common.DataRange{
		Span: &heartbeatpb.TableSpan{
			TableID: tableID,
		},
		StartTs: updateEvent.StartTs,
		EndTs:   updateEvent.CRTs + 100,
	}

	limit := scanLimit{
		maxBytes: 1000,
		timeout:  10 * time.Second,
	}

	session := &scanSession{
		ctx:            ctx,
		dispatcherStat: dispatcherStat,
		dataRange:      dataRange,
		limit:          limit,
		startTime:      time.Now(),
		events:         make([]event.Event, 0),
	}

	// Execute scanAndMergeEvents
	events, isBroken, err := scanner.scanAndMergeEvents(session, []pevent.DDLEvent{}, mockIter)

	// Verify results
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 2, len(events)) // BatchDML + ResolvedEvent

	// Verify first event is BatchDMLEvent
	batchDML, ok := events[0].(*pevent.BatchDMLEvent)
	require.True(t, ok)
	require.Equal(t, dispatcherID, batchDML.GetDispatcherID())
	require.Equal(t, updateEvent.CRTs, batchDML.GetCommitTs())
	require.Equal(t, 1, len(batchDML.DMLEvents)) // One DML event in the batch

	// Verify the DML event contains the update
	dmlEvent := batchDML.DMLEvents[0]
	require.Equal(t, 2, int(dmlEvent.Len()))
	require.Equal(t, updateEvent.StartTs, dmlEvent.GetStartTs())
	require.Equal(t, updateEvent.CRTs, dmlEvent.GetCommitTs())
	require.Equal(t, tableID, dmlEvent.GetTableID())
	row1, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, pevent.RowTypeDelete, row1.RowType)
	require.Equal(t, int64(1), row1.PreRow.GetInt64(0))
	require.Equal(t, int64(10), row1.PreRow.GetInt64(1))
	require.Equal(t, "old_b", string(row1.PreRow.GetBytes(2)))
	row2, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, pevent.RowTypeInsert, row2.RowType)
	require.Equal(t, int64(1), row2.Row.GetInt64(0))
	require.Equal(t, int64(20), row2.Row.GetInt64(1))
	require.Equal(t, "old_b", string(row2.Row.GetBytes(2)))

	// Verify second event is ResolvedEvent
	resolvedEvent, ok := events[1].(pevent.ResolvedEvent)
	require.True(t, ok)
	require.Equal(t, dispatcherID, resolvedEvent.DispatcherID)
	require.Equal(t, dataRange.EndTs, resolvedEvent.ResolvedTs)

	// Verify session state was updated correctly
	require.Equal(t, updateEvent.CRTs, session.lastCommitTs)
	require.Equal(t, 1, session.dmlCount)
	require.True(t, session.totalBytes > 0) // Some bytes were processed
}
