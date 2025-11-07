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
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type mockMounter struct {
	event.Mounter
}

func makeDispatcherReady(disp *dispatcherStat) {
	disp.setHandshaked()
}

func (m *mockMounter) DecodeToChunk(rawKV *common.RawKVEntry, tableInfo *common.TableInfo, chk *chunk.Chunk) (int, *integrity.Checksum, error) {
	if rawKV.IsUpdate() {
		return 2, nil, nil
	} else {
		return 1, nil, nil
	}
}

func TestEventScanner(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
		`insert into test.t(id,c) values (3, "c3")`,
	}...)

	broker, _, _, _ := newEventBrokerForTest()
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = uint64(100)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID
	dispatcherID := disInfo.GetID()

	ctx := context.Background()

	disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)
	makeDispatcherReady(disp)
	err := broker.addDispatcher(disp.info)
	require.NoError(t, err)

	// case 1: No DDL and DML events, should emit resolved-ts event
	// Expected result:
	// [Resolved(ts=102)]
	scanner := newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)

	disp.receivedResolvedTs.Store(102)
	sl := scanLimit{
		maxDMLBytes: 1000,
	}
	ok, dataRange := broker.getScanTaskDataRange(disp)
	require.True(t, ok)

	_, events, isInterrupted, err := scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 1, len(events))
	e := events[0]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, e.GetCommitTs(), uint64(102))

	// case 2: Has DDL and no DML, should emit DDL and resolved-ts event
	broker.schemaStore.(*mockSchemaStore).AppendDDLEvent(tableID, ddlEvent)

	resolvedTs := kvEvents[len(kvEvents)-1].CRTs + 1
	disp.receivedResolvedTs.Store(resolvedTs)
	ok, dataRange = broker.getScanTaskDataRange(disp)
	require.True(t, ok)

	sl = scanLimit{
		maxDMLBytes: 1000,
	}

	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 2, len(events))

	e = events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())

	e = events[1]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())

	// case 3: Has DML but no DDL, should emit BatchDML and resolved-ts event
	// Event sequence:
	//   DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4)
	// Expected result:
	// [BatchDML_1[DML(x+1), DML(x+2), DML(x+3), DML(x+4)], Resolved(x+5)]
	err = broker.eventStore.(*mockEventStore).AppendEvents(dispatcherID, resolvedTs, kvEvents...)
	require.NoError(t, err)

	disp.receivedResolvedTs.Store(resolvedTs)
	require.True(t, ok)
	dataRange.CommitTsStart = ddlEvent.GetCommitTs()

	sl = scanLimit{
		maxDMLBytes: 1000,
	}

	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 2, len(events))

	require.Equal(t, event.TypeBatchDMLEvent, events[0].GetType())
	require.Equal(t, event.TypeResolvedEvent, events[1].GetType())

	batchDMLEvent := events[0].(*event.BatchDMLEvent)
	require.Equal(t, int32(4), batchDMLEvent.Len())

	require.Equal(t, batchDMLEvent.DMLEvents[0].Len(), int32(1))
	require.Equal(t, batchDMLEvent.DMLEvents[0].GetCommitTs(), kvEvents[0].CRTs)

	require.Equal(t, batchDMLEvent.DMLEvents[1].Len(), int32(1))
	require.Equal(t, batchDMLEvent.DMLEvents[1].GetCommitTs(), kvEvents[1].CRTs)

	require.Equal(t, batchDMLEvent.DMLEvents[2].Len(), int32(1))
	require.Equal(t, batchDMLEvent.DMLEvents[2].GetCommitTs(), kvEvents[2].CRTs)

	require.Equal(t, batchDMLEvent.DMLEvents[3].Len(), int32(1))
	require.Equal(t, batchDMLEvent.DMLEvents[3].GetCommitTs(), kvEvents[3].CRTs)

	require.Equal(t, events[1].(event.ResolvedEvent).GetCommitTs(), resolvedTs)

	// case 4: Contains DDL, DML and resolvedTs events
	// Tests that the scanner can handle mixed event types (DDL + DML + resolvedTs)
	// Event sequence:
	//   DDL(ts=x) -> DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4) -> Resolved(ts=x+5)
	// Expected result:
	// [DDL(x), BatchDML_1[DML(x+1)], BatchDML_2[DML(x+2), DML(x+3), DML(x+4)], Resolved(x+5)]
	disp.receivedResolvedTs.Store(resolvedTs)
	ok, dataRange = broker.getScanTaskDataRange(disp)
	require.True(t, ok)

	sl = scanLimit{
		maxDMLBytes: 1000,
	}
	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 4, len(events))
	require.Equal(t, event.TypeDDLEvent, events[0].GetType())
	require.Equal(t, event.TypeBatchDMLEvent, events[1].GetType())
	require.Equal(t, event.TypeBatchDMLEvent, events[2].GetType())
	require.Equal(t, event.TypeResolvedEvent, events[3].GetType())
	batchDML1 := events[1].(*event.BatchDMLEvent)
	require.Equal(t, int32(1), batchDML1.Len())
	require.Equal(t, batchDML1.DMLEvents[0].GetCommitTs(), kvEvents[0].CRTs)
	batchDML2 := events[2].(*event.BatchDMLEvent)
	require.Equal(t, int32(3), batchDML2.Len())
	require.Equal(t, batchDML2.DMLEvents[0].GetCommitTs(), kvEvents[1].CRTs)
	require.Equal(t, batchDML2.DMLEvents[1].GetCommitTs(), kvEvents[2].CRTs)
	require.Equal(t, batchDML2.DMLEvents[2].GetCommitTs(), kvEvents[3].CRTs)

	// case 5: Reaches scan limit, only 1 DDL and 1 DML event scanned
	// Tests that when MaxBytes limit is reached, the scanner returns partial events with isInterrupted=true
	// Event sequence:
	//   DDL(ts=x) -> DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4)
	// Expected result:
	// [DDL(x), BatchDML[DML(x+1)], Resolved(x+1)] (partial events due to size limit)
	//               ▲
	//               └── Scanning interrupted here
	sl = scanLimit{
		maxDMLBytes: 1,
	}

	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isInterrupted)
	require.Equal(t, 3, len(events))

	e = events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.GetCommitTs(), e.GetCommitTs())

	e = events[1]
	require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())

	e = events[2]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())

	// case 6: Tests transaction atomicity during scanning, without resolved-ts
	// Tests that transactions with same startTs and commitTs are scanned atomically (not split even when limit is reached)
	// Modified events: first 3 DMLs have same startTs=x:
	//   DDL(x) -> DML-1(start-ts = x, commit-ts = x+4) -> DML-2(start-ts = x, commit-ts = x+4) -> DML-3(start-ts = x, commit-ts = x+4) -> DML-4(start-ts = x + 3, commit-ts = x+4)
	// Expected result (MaxBytes=1):
	// [DDL(x), BatchDML_1[DML-1(x+1), DML-2(x+1), DML-3(x+1)]]
	//                               ▲
	//                               └── Scanning interrupted here
	// The length of the result here is 2.
	// The DML-1(x+1) will appear separately because it encounters DDL(x), which will immediately append it.

	// the first 3 entries have the same startTs and commitTs, so they belong to the same transaction.
	// The last entry has different startTs, even though the commitTs is the same,
	// it should not be scanned, and trigger interrupt scan. and do not output resolved-ts
	firstStartTs := kvEvents[0].StartTs
	lastCommitTs := kvEvents[3].CRTs
	for i := 0; i < 3; i++ {
		kvEvents[i].StartTs = firstStartTs
		kvEvents[i].CRTs = lastCommitTs
	}

	sl = scanLimit{
		maxDMLBytes: 1,
	}

	require.True(t, ok)
	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isInterrupted)
	require.Equal(t, 2, len(events))

	// DDL
	e = events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.GetCommitTs(), e.GetCommitTs())

	// DML-1
	e = events[1]
	require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
	require.Equal(t, len(e.(*event.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, e.(*event.BatchDMLEvent).DMLEvents[0].Len(), int32(3))
	require.Equal(t, firstStartTs, e.GetStartTs())
	require.Equal(t, lastCommitTs, e.GetCommitTs())

	// case 7: Tests transaction atomicity during scanning, with resolved-ts
	// Tests that transactions with same startTs and commitTs are scanned atomically (not split even when limit is reached)
	// Modified events: first 3 DMLs have same startTs=x:
	//   DDL(x) -> DML-1(start-ts = x, commit-ts = x+4) -> DML-2(start-ts = x, commit-ts = x+4) -> DML-3(start-ts = x, commit-ts = x+4) -> DML-4(start-ts = x + 3, commit-ts = x+5)
	// Expected result (MaxBytes=1):
	// [DDL(x), BatchDML_1[DML-1(x+1), DML-2(x+1), DML-3(x+1)]]
	//                               ▲
	//                               └── Scanning interrupted here
	// The length of the result here is 2.
	// The DML-1(x+1) will appear separately because it encounters DDL(x), which will immediately append it.

	// the first 3 entries have the same startTs and commitTs, so they belong to the same transaction.
	// The last entry has different startTs and different commitTs,
	// it should not be scanned, and trigger interrupt scan. and should output resolved-ts
	kvEvents[3].CRTs++

	sl = scanLimit{
		maxDMLBytes: 1,
	}

	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isInterrupted)
	require.Equal(t, 3, len(events))

	// DDL
	e = events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.GetCommitTs(), e.GetCommitTs())

	// DML-1
	batchDMLEvent = events[1].(*event.BatchDMLEvent)
	require.Equal(t, len(batchDMLEvent.DMLEvents), 1)
	require.Equal(t, batchDMLEvent.DMLEvents[0].Len(), int32(3))
	require.Equal(t, firstStartTs, batchDMLEvent.GetStartTs())
	require.Equal(t, lastCommitTs, batchDMLEvent.GetCommitTs())

	resolvedEvent := events[2].(event.ResolvedEvent)
	require.Equal(t, resolvedEvent.GetType(), event.TypeResolvedEvent)
	require.Equal(t, lastCommitTs, resolvedEvent.GetCommitTs())

	// case 8: Tests DMLs are returned before DDLs when they share same commitTs
	// Tests that DMLs take precedence over DDLs with same commitTs
	// Event sequence after adding fakeDDL(ts=x):
	//   DDL(x) -> DML(x+1) -> DML(x+1) -> DML(x+1) -> fakeDDL(x+1) -> DML(x+4)
	// Expected result:
	// [DDL(x), BatchDML_1[DML(x+1), DML(x+1), DML(x+1)], fakeDDL(x+1), BatchDML_3[DML(x+4)], Resolved(x+5)]
	//                                ▲
	//                                └── DMLs take precedence over DDL with same ts
	fakeDDL := event.DDLEvent{
		FinishedTs: kvEvents[0].CRTs,
		TableInfo:  ddlEvent.TableInfo,
	}
	broker.schemaStore.(*mockSchemaStore).AppendDDLEvent(tableID, fakeDDL)

	sl = scanLimit{
		maxDMLBytes: 1000,
	}
	scanner = newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)
	_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 5, len(events))

	firstDDL := events[0].(*event.DDLEvent)
	require.Equal(t, firstDDL.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.GetCommitTs(), firstDDL.GetCommitTs())

	// First 2 BatchDMLs should appear before fake DDL
	// BatchDML_1
	firstDML := events[1].(*event.BatchDMLEvent)
	require.Equal(t, firstDML.GetType(), event.TypeBatchDMLEvent)
	require.Equal(t, len(firstDML.DMLEvents), 1)
	require.Equal(t, firstDML.DMLEvents[0].Len(), int32(3))
	require.Equal(t, kvEvents[0].CRTs, firstDML.GetCommitTs())

	// Fake DDL should appear after DMLs
	ddl := events[2]
	require.Equal(t, ddl.GetType(), event.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, ddl.GetCommitTs())
	require.Equal(t, fakeDDL.FinishedTs, firstDML.GetCommitTs())

	// BatchDML_3
	batchDML3 := events[3]
	require.Equal(t, batchDML3.GetType(), event.TypeBatchDMLEvent)
	require.Equal(t, len(batchDML3.(*event.BatchDMLEvent).DMLEvents), 1)
	require.Equal(t, kvEvents[3].CRTs, batchDML3.GetCommitTs())

	// Resolved
	e = events[4]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}

// Test the case where some DMLs have commit timestamps newer than the table's delete version
func TestEventScannerWithDeleteTable(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	broker.close()

	mockEventStore := broker.eventStore.(*mockEventStore)
	mockSchemaStore := broker.schemaStore.(*mockSchemaStore)

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = uint64(100)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID
	dispatcherID := disInfo.GetID()

	disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)
	makeDispatcherReady(disp)
	err := broker.addDispatcher(disp.info)
	require.NoError(t, err)

	scanner := newEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{}, 0)

	// Construct events: dml2 and dml3 share commitTs, fakeDDL shares commitTs with them
	helper := event.NewEventTestHelper(t)
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

	// delete table after dml2
	dml0 := kvEvents[0]
	dml1 := kvEvents[1]
	dml2 := kvEvents[2]
	mockSchemaStore.DeleteTable(tableID, dml2.CRTs)
	disp.receivedResolvedTs.Store(resolvedTs)
	ok, dataRange := broker.getScanTaskDataRange(disp)
	require.True(t, ok)

	sl := scanLimit{
		maxDMLBytes: 10000,
	}
	_, events, isInterrupted, err := scanner.scan(context.Background(), disp, dataRange, sl)
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 4, len(events))
	// DDL
	e := events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// DML0
	e = events[1]
	require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
	batchDML0 := e.(*event.BatchDMLEvent)
	require.Equal(t, int32(1), batchDML0.Len())
	require.Equal(t, batchDML0.DMLEvents[0].GetCommitTs(), dml0.CRTs)

	// DML1 & DML2
	e = events[2]
	require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
	batchDML1 := e.(*event.BatchDMLEvent)
	require.Equal(t, int32(2), batchDML1.Len())
	require.Equal(t, batchDML1.DMLEvents[0].GetCommitTs(), dml1.CRTs)
	require.Equal(t, batchDML1.DMLEvents[1].GetCommitTs(), dml2.CRTs)

	// resolvedTs
	e = events[3]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, dml2.CRTs, e.GetCommitTs())
}

// TestEventScannerWithDDL tests cases where scanning is interrupted at DDL events
// The scanner should return the DDL event plus any DML events sharing the same commitTs
// It should also return a resolvedTs event with the commitTs of the last DML event
func TestEventScannerWithDDL(t *testing.T) {
	broker, _, _, _ := newEventBrokerForTest()
	broker.close()

	mockEventStore := broker.eventStore.(*mockEventStore)
	mockSchemaStore := broker.schemaStore.(*mockSchemaStore)

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = uint64(100)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID
	dispatcherID := disInfo.GetID()

	disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)
	makeDispatcherReady(disp)

	err := broker.addDispatcher(disp.info)
	require.NoError(t, err)

	scanner := newEventScanner(broker.eventStore, broker.schemaStore, event.NewMounter(time.UTC, &integrity.Config{}), 0)

	// Construct events: dml2 and dml3 share commitTs, fakeDDL shares commitTs with them
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper,
		`create table test.t(id int primary key, c char(50))`,
		[]string{
			`insert into test.t(id,c) values (0, "c0")`,
			`insert into test.t(id,c) values (1, "c1")`,
			`insert into test.t(id,c) values (2, "c2")`,
			`insert into test.t(id,c) values (3, "c3")`,
		}...)

	resolvedTs := kvEvents[len(kvEvents)-1].CRTs + 1
	err = mockEventStore.AppendEvents(dispatcherID, resolvedTs, kvEvents...)
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
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)

	disp.receivedResolvedTs.Store(resolvedTs)
	ok, dataRange := broker.getScanTaskDataRange(disp)
	require.True(t, ok)

	// case 1: Scanning interrupted at dml1
	// Tests interruption at first DML due to size limit
	// Event sequence:
	//   DDL(x) -> DML1(x+1) -> DML2(x+2) -> DML3(x+2) -> fakeDDL(x+2) -> DML3(x+4)
	// [DDL(x), DML1(x+1), Resolved(x+1)]
	//             ▲
	//             └── Scanning interrupted at DML1
	sl := scanLimit{
		maxDMLBytes:  1, // count 1 events
		isInUnitTest: true,
	}

	ctx := context.Background()
	_, events, isInterrupted, err := scanner.scan(ctx, disp, dataRange, sl)
	require.NoError(t, err)
	require.True(t, isInterrupted)
	require.Equal(t, 3, len(events))
	// DDL
	e := events[0]
	require.Equal(t, e.GetType(), event.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// DML1
	e = events[1]
	require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	require.Equal(t, int32(1), e.(*event.BatchDMLEvent).Len())
	// resolvedTs
	e = events[2]
	require.Equal(t, e.GetType(), event.TypeResolvedEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	require.Equal(t, dml1.CRTs, e.GetCommitTs())

	log.Info("pass case 1")

	t.Run("Scanning interrupted at dml1", func(t *testing.T) {
		// case 2: Scanning interrupted at dml1
		// Tests atomic return of DML2/DML3/fakeDDL sharing same commitTs
		// Event sequence:
		//   DDL(x) -> DML1(x+1) -> DML2(x+2) -> DML3(x+2) -> fakeDDL(x+2) -> DML3(x+4)
		// Expected result:
		//   [DDL(x), DML1(x+1), Resolved(x+1)]
		// 					▲
		// 				    └── Scanning interrupted at DML1
		// DO NOT emit fakeDDL and ResolvedTs here, since there is still DML3 not scanned
		sl = scanLimit{
			maxDMLBytes:  2,
			isInUnitTest: true,
		}
		_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
		require.NoError(t, err)
		require.True(t, isInterrupted)
		require.Equal(t, 3, len(events))

		// DDL1
		e = events[0]
		require.Equal(t, e.GetType(), event.TypeDDLEvent)
		require.Equal(t, ddlEvent.GetCommitTs(), e.GetCommitTs())

		// DML1
		e = events[1]
		require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
		require.Equal(t, int32(1), e.(*event.BatchDMLEvent).Len())
		require.Equal(t, dml1.CRTs, e.GetCommitTs())

		// resolvedTs
		e = events[2]
		require.Equal(t, e.GetType(), event.TypeResolvedEvent)
		require.Equal(t, dml1.CRTs, e.GetCommitTs())

		log.Info("pass case 2")
	})

	t.Run("Scanning interrupted at dml3", func(t *testing.T) {
		// case 3: Scanning interrupted at dml3
		// Tests atomic return of DML2/DML3/fakeDDL sharing same commitTs
		// Event sequence:
		//   DDL(x) -> DML1(x+1) -> DML2(x+2) -> DML3(x+2) -> fakeDDL(x+2) -> DML4(x+4)
		// Expected result:
		//   [DDL(x), BatchDML1[DML1(x+1)], BatchDML2[DML2(x+2), DML3(x+2)], fakeDDL(x+2), Resolved(x+2)]
		//                                               ▲
		//                                               └── Events with same commitTs(x+2) must be returned together
		// 										                       ▲
		// 										                       └── Scanning interrupted at DML3
		sl = scanLimit{
			maxDMLBytes:  3, // Event if we set 3, it should not be interrupted at DML2
			isInUnitTest: true,
		}
		_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)

		require.NoError(t, err)
		require.True(t, isInterrupted)
		require.Equal(t, 5, len(events))

		// DDL1
		e = events[0]
		require.Equal(t, e.GetType(), event.TypeDDLEvent)
		require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
		// DML1
		e = events[1]
		require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
		require.Equal(t, int32(1), e.(*event.BatchDMLEvent).Len())
		require.Equal(t, dml1.CRTs, e.GetCommitTs())

		// DML2 DML3
		e = events[2]
		require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
		require.Equal(t, dml3.CRTs, e.GetCommitTs())
		require.Equal(t, int32(2), e.(*event.BatchDMLEvent).Len())
		require.Equal(t, dml2.CRTs, e.(*event.BatchDMLEvent).DMLEvents[0].GetCommitTs())
		require.Equal(t, dml3.CRTs, e.(*event.BatchDMLEvent).DMLEvents[1].GetCommitTs())

		// fake DDL
		e = events[3]
		require.Equal(t, e.GetType(), event.TypeDDLEvent)
		require.Equal(t, fakeDDL.FinishedTs, e.GetCommitTs())
		require.Equal(t, dml3.CRTs, e.GetCommitTs())
		// resolvedTs
		e = events[4]
		require.Equal(t, e.GetType(), event.TypeResolvedEvent)
		require.Equal(t, dml3.CRTs, e.GetCommitTs())

		log.Info("pass case 3")
	})

	t.Run("Scanning should not be interrupted", func(t *testing.T) {
		// case 4: Tests handling of multiple DDL events
		// Tests that scanner correctly processes multiple DDL events
		// Event sequence after additions:
		//   ... -> fakeDDL2(x+5) -> fakeDDL3(x+6)
		// Expected result:
		// [..., fakeDDL2(x+5), fakeDDL3(x+6), Resolved(x+7)]
		sl = scanLimit{
			maxDMLBytes:  100,
			isInUnitTest: true,
		}

		// Add more fake DDL events
		fakeDDL2 := event.DDLEvent{
			FinishedTs: resolvedTs + 1,
			TableInfo:  ddlEvent.TableInfo,
		}
		fakeDDL3 := event.DDLEvent{
			FinishedTs: resolvedTs + 2,
			TableInfo:  ddlEvent.TableInfo,
		}
		mockSchemaStore.AppendDDLEvent(tableID, fakeDDL2)
		mockSchemaStore.AppendDDLEvent(tableID, fakeDDL3)

		resolvedTs = resolvedTs + 3
		disp.receivedResolvedTs.Store(resolvedTs)

		ok, dataRange = broker.getScanTaskDataRange(disp)
		require.True(t, ok)

		_, events, isInterrupted, err = scanner.scan(ctx, disp, dataRange, sl)
		require.NoError(t, err)
		require.False(t, isInterrupted)

		require.Equal(t, 8, len(events))

		e = events[4]
		require.Equal(t, e.GetType(), event.TypeBatchDMLEvent)
		require.Equal(t, e.GetCommitTs(), kvEvents[3].CRTs)
		require.Equal(t, int32(1), e.(*event.BatchDMLEvent).Len())

		e = events[5]
		require.Equal(t, e.GetType(), event.TypeDDLEvent)
		require.Equal(t, e.GetCommitTs(), fakeDDL2.FinishedTs)

		e = events[6]
		require.Equal(t, e.GetType(), event.TypeDDLEvent)
		require.Equal(t, e.GetCommitTs(), fakeDDL3.FinishedTs)

		e = events[7]
		require.Equal(t, e.GetType(), event.TypeResolvedEvent)
		require.Equal(t, resolvedTs, e.GetCommitTs())
	})
}

// TestDMLProcessor tests the processNewTransaction method of dmlProcessor
func TestDMLProcessor(t *testing.T) {
	// Setup helper and table info
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
	}...)

	tableInfo := ddlEvent.TableInfo
	tableID := ddlEvent.TableInfo.TableName.TableID
	dispatcherID := common.NewDispatcherID()

	// Create a mock mounter and schema getter
	mockMounter := &mockMounter{}
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	// Test case 0: create a new DML processor
	t.Run("CreateNewDMLProcessor", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)
		require.NotNil(t, processor)
		require.NotNil(t, processor.batchDML)
		require.Nil(t, processor.currentDML)
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 1: commitTxn with no current DML, happens when the iter is nil.
	t.Run("CommitTxnWithNoCurrentDML", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)
		err := processor.commitTxn()
		require.NoError(t, err)
		require.Nil(t, processor.currentDML)
		require.Empty(t, processor.insertRowCache)
		require.Equal(t, int32(0), processor.batchDML.Len())
	})

	// Test case 1: start the first transaction
	t.Run("FirstTransactionWithoutCache", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)
		rawEvent := kvEvents[0]

		processor.startTxn(dispatcherID, tableID, tableInfo, rawEvent.StartTs, rawEvent.CRTs)

		// Verify that a new DML event was created
		require.NotNil(t, processor.currentDML)
		require.Equal(t, dispatcherID, processor.currentDML.GetDispatcherID())
		require.Equal(t, tableID, processor.currentDML.GetTableID())
		require.Equal(t, rawEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, rawEvent.CRTs, processor.currentDML.GetCommitTs())

		// Verify that the DML was added to the batch
		require.Len(t, processor.batchDML.DMLEvents, 1)
		require.Equal(t, processor.currentDML, processor.batchDML.DMLEvents[0])

		// Verify insert cache is empty
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 2: Process new transaction when there are cached insert rows
	t.Run("NewTransactionWithInsertCache", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// Setup first transaction
		firstEvent := kvEvents[0]
		processor.startTxn(dispatcherID, tableID, tableInfo, firstEvent.StartTs, firstEvent.CRTs)

		err := processor.appendRow(firstEvent)
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)
		// Verify that insert cache has been processed and cleared
		require.Empty(t, processor.insertRowCache)
		require.Equal(t, 1, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(1), processor.batchDML.Len())

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
		processor.startTxn(dispatcherID, tableID, tableInfo, secondEvent.StartTs, secondEvent.CRTs)

		// Verify new DML event was created for second transaction
		require.NotNil(t, processor.currentDML)
		require.Equal(t, secondEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, secondEvent.CRTs, processor.currentDML.GetCommitTs())

		err = processor.appendRow(secondEvent)
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		// Verify batch now contains two DML events
		require.Equal(t, 2, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(4), processor.batchDML.Len())
	})

	// Test case 3: Multiple consecutive transactions
	t.Run("ConsecutiveTransactions", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// Process multiple transactions
		for i, item := range kvEvents {
			processor.startTxn(dispatcherID, tableID, tableInfo, item.StartTs, item.CRTs)

			// Verify current DML matches the event
			require.NotNil(t, processor.currentDML)
			require.Equal(t, item.StartTs, processor.currentDML.GetStartTs())
			require.Equal(t, item.CRTs, processor.currentDML.GetCommitTs())

			// Verify batch size increases

			err := processor.appendRow(item)
			require.NoError(t, err)

			require.Equal(t, int32(i+1), processor.batchDML.Len())

			err = processor.commitTxn()
			require.NoError(t, err)
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
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// First transaction - no cache
		firstEvent := kvEvents[0]
		processor.startTxn(dispatcherID, tableID, tableInfo, firstEvent.StartTs, firstEvent.CRTs)
		require.Empty(t, processor.insertRowCache)

		err := processor.appendRow(firstEvent)
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		// Second transaction - should process and clear cache
		secondEvent := kvEvents[1]
		processor.startTxn(dispatcherID, tableID, tableInfo, secondEvent.StartTs, secondEvent.CRTs)

		err = processor.appendRow(secondEvent)
		require.NoError(t, err)

		// Add insert rows to cache
		insertRow := &common.RawKVEntry{
			StartTs: firstEvent.StartTs,
			CRTs:    firstEvent.CRTs,
			Key:     []byte("cached_insert"),
			Value:   []byte("cached_value"),
		}
		processor.insertRowCache = append(processor.insertRowCache, insertRow)

		err = processor.commitTxn()
		require.NoError(t, err)

		// Verify cache was cleared
		require.Empty(t, processor.insertRowCache)

		// Verify all events are in the batch
		require.Equal(t, 2, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(3), processor.batchDML.Len())
	})

	// Test 6: First event is update that changes UK
	t.Run("UpdateThatChangesUK", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		helper.Tk().MustExec("use test")
		ddlEvent := helper.DDL2Event("create table t2 (id int primary key, a int(50), b char(50), unique key uk_a(a))")
		tableInfo := ddlEvent.TableInfo
		tableID := ddlEvent.TableInfo.TableName.TableID

		_, updateEvent := helper.DML2UpdateEvent("test", "t2",
			"insert into test.t2(id, a, b) values (0, 1, 'b0')",
			"update test.t2 set a = 2 where id = 0")
		processor.startTxn(dispatcherID, tableID, tableInfo, updateEvent.StartTs, updateEvent.CRTs)

		require.NotNil(t, processor.currentDML)
		require.Equal(t, updateEvent.StartTs, processor.currentDML.GetStartTs())
		require.Equal(t, updateEvent.CRTs, processor.currentDML.GetCommitTs())

		err := processor.appendRow(updateEvent)
		require.NoError(t, err)

		require.Equal(t, 1, len(processor.insertRowCache))
		require.Equal(t, common.OpTypePut, processor.insertRowCache[0].OpType)
		require.False(t, processor.insertRowCache[0].IsUpdate())

		err = processor.commitTxn()
		require.NoError(t, err)

		require.Empty(t, processor.insertRowCache)
		require.Equal(t, 1, len(processor.batchDML.DMLEvents))
		require.Equal(t, int32(2), processor.batchDML.Len())
	})
}

// TestDMLProcessorAppendRow tests the appendRow method of dmlProcessor
func TestDMLProcessorAppendRow(t *testing.T) {
	// Setup helper and table info
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, a char(50), b char(50), unique key uk_a(a))`, []string{
		`insert into test.t(id,a,b) values (0, "a0", "b0")`,
		`insert into test.t(id,a,b) values (1, "a1", "b1")`,
		`insert into test.t(id,a,b) values (2, "a2", "b2")`,
	}...)

	tableInfo := ddlEvent.TableInfo
	tableID := ddlEvent.TableInfo.TableName.TableID
	dispatcherID := common.NewDispatcherID()

	// Create a mock mounter and schema getter
	mockMounter := &mockMounter{}
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	// Test case 1: appendRow before txn started, illegal usage.
	t.Run("NoCurrentDMLEvent", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)
		rawEvent := kvEvents[0]

		require.Panics(t, func() {
			_ = processor.appendRow(rawEvent)
		})
	})

	// Test case 2: appendRow for insert operation (non-update)
	t.Run("AppendInsertRow", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		firstEvent := kvEvents[0]
		processor.startTxn(dispatcherID, tableID, tableInfo, firstEvent.StartTs, firstEvent.CRTs)

		err := processor.appendRow(firstEvent)
		require.NoError(t, err)

		secondEvent := kvEvents[1]
		err = processor.appendRow(secondEvent)
		require.NoError(t, err)

		// Verify insert cache remains empty (since it's not a split update)
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 3: appendRow for delete operation (non-update)
	t.Run("AppendDeleteRow", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)

		processor.startTxn(dispatcherID, tableID, tableInfo, rawEvent.StartTs, rawEvent.CRTs)
		err := processor.appendRow(rawEvent)
		require.NoError(t, err)

		require.NoError(t, err)
		err = processor.appendRow(deleteRow)
		require.NoError(t, err)

		// Verify insert cache remains empty
		require.Empty(t, processor.insertRowCache)
	})

	// Test case 4: appendRow for update operation without unique key change
	t.Run("AppendUpdateRowWithoutUKChange", func(t *testing.T) {
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// Create a current DML event first
		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)

		processor.startTxn(dispatcherID, tableID, tableInfo, rawEvent.StartTs, rawEvent.CRTs)
		err := processor.appendRow(rawEvent)
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
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// Create a current DML event first
		rawEvent := kvEvents[0]
		deleteRow := insertToDeleteRow(rawEvent)

		processor.startTxn(dispatcherID, tableID, tableInfo, rawEvent.StartTs, rawEvent.CRTs)
		err := processor.appendRow(rawEvent)
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
		processor := newDMLProcessor(mockMounter, mockSchemaGetter, nil, false)

		// Create a current DML event first
		rawEvent := kvEvents[0]

		processor.startTxn(dispatcherID, tableID, tableInfo, rawEvent.StartTs, rawEvent.CRTs)
		err := processor.appendRow(rawEvent)
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

	// Test case 7: appendRow for update operation with unique key change and outputRawChangeEvent is true (do not split update)
	t.Run("AppendUpdateRowWithUKChangeAndOutputRawChangeEvent", func(t *testing.T) {
		processor := newDMLProcessor(event.NewMounter(time.UTC, &integrity.Config{}), mockSchemaGetter, nil, true)
		// Generate a real update event that changes unique key using helper
		// This updates the unique key column 'a' from 'a1' to 'a1_new'
		insertSQL, updateSQL := "insert into test.t(id,a,b) values (7, 'a7', 'b7')", "update test.t set a = 'a7_updated' where id = 7"
		_, updateEvent := helper.DML2UpdateEvent("test", "t", insertSQL, updateSQL)

		processor.startTxn(dispatcherID, tableID, tableInfo, updateEvent.StartTs, updateEvent.CRTs)
		err := processor.appendRow(updateEvent)
		require.NoError(t, err)

		// Verify insert cache
		require.Len(t, processor.insertRowCache, 0)
		require.Len(t, processor.batchDML.DMLEvents, 1)
		nextRow, ok := processor.batchDML.DMLEvents[0].GetNextRow()
		require.True(t, ok)
		require.Equal(t, common.RowTypeUpdate, nextRow.RowType)
	})
}

func TestScanSession(t *testing.T) {
	// Test observeRawEntry method
	t.Run("TestObserveRawEntry", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxDMLBytes: 1000}
		sess := newSession(ctx, dispStat, dataRange, limit)

		// Test initial scannedBytes is 0
		require.Equal(t, int64(0), sess.scannedBytes)

		entry := &common.RawKVEntry{
			StartTs: 1,
			CRTs:    2,
			Key:     []byte("insert_key_1"),
			Value:   []byte("insert_value_1"),
		}
		sess.observeRawEntry(entry)
		require.Equal(t, entry.GetSize(), sess.scannedBytes)
		require.Equal(t, 1, sess.scannedEntryCount)

		// Test adding more bytes
		sess.observeRawEntry(entry)
		require.Equal(t, 2*entry.GetSize(), sess.scannedBytes)
		require.Equal(t, 2, sess.scannedEntryCount)
	})

	// Test isContextDone method
	t.Run("TestIsContextDone", func(t *testing.T) {
		// Test with normal context
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxDMLBytes: 1000}

		sess := newSession(ctx, dispStat, dataRange, limit)

		// Context should not be done initially
		require.False(t, sess.isContextDone())

		// Test with cancelled context
		cancelCtx, cancel := context.WithCancel(context.Background())
		sessionWithCancelCtx := newSession(cancelCtx, dispStat, dataRange, limit)

		// Context should not be done before cancellation
		require.False(t, sessionWithCancelCtx.isContextDone())

		// Cancel the context
		cancel()

		// Context should be done after cancellation
		require.True(t, sessionWithCancelCtx.isContextDone())

		// Test with timeout context
		timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer timeoutCancel()
		sessionWithTimeoutCtx := newSession(timeoutCtx, dispStat, dataRange, limit)

		// Context should not be done initially
		require.False(t, sessionWithTimeoutCtx.isContextDone())

		// Wait for timeout
		time.Sleep(10 * time.Millisecond)

		// Context should be done after timeout
		require.True(t, sessionWithTimeoutCtx.isContextDone())
	})

	// Test session initialization
	t.Run("TestSessionInitialization", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{
			Span:          &heartbeatpb.TableSpan{TableID: 123},
			CommitTsStart: 100,
			CommitTsEnd:   200,
		}
		limit := scanLimit{maxDMLBytes: 1000}

		sess := newSession(ctx, dispStat, dataRange, limit)

		// Verify initialization
		require.Equal(t, ctx, sess.ctx)
		require.Equal(t, dispStat, sess.dispatcherStat)
		require.Equal(t, dataRange, sess.dataRange)
		require.Equal(t, limit, sess.limit)
		require.Equal(t, int64(0), sess.scannedBytes)
		require.Equal(t, 0, sess.dmlCount)

		require.NotNil(t, sess.events)
		require.Equal(t, 0, len(sess.events))
		require.Equal(t, int64(0), sess.eventBytes)

		require.True(t, time.Since(sess.startTime) >= 0)
	})

	// Test session state tracking
	t.Run("TestSessionStateTracking", func(t *testing.T) {
		ctx := context.Background()
		dispStat := &dispatcherStat{}
		dataRange := common.DataRange{}
		limit := scanLimit{maxDMLBytes: 1000}

		sess := newSession(ctx, dispStat, dataRange, limit)

		// Test updating state fields
		sess.dmlCount = 5
		require.Equal(t, 5, sess.dmlCount)

		// Test events collection
		require.NotNil(t, sess.events)
		require.Equal(t, 0, len(sess.events))

		// Events slice should be mutable
		sess.events = append(sess.events, nil) // Add a nil event for testing
		require.Equal(t, 1, len(sess.events))
	})

	t.Run("LimitCheck", func(t *testing.T) {
		t.Run("ByteLimitExceeded", func(t *testing.T) {
			ctx := context.Background()
			dispStat := &dispatcherStat{}
			dataRange := common.DataRange{}
			limit := scanLimit{maxDMLBytes: 1000}

			sess := newSession(ctx, dispStat, dataRange, limit)
			sess.eventBytes = 500

			// Test bytes below limit: 500 + 499 = 999 < 1000
			require.False(t, sess.exceedLimit(499))
			// Test bytes at limit: 500 + 500 = 1000 >= 1000
			require.True(t, sess.exceedLimit(500))
			// Test bytes exceeding limit: 500 + 501 = 1001 > 1000
			require.True(t, sess.exceedLimit(501))
		})
	})
}

func TestEventMerger(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	mounter := event.NewMounter(time.UTC, &integrity.Config{})

	t.Run("TestNewEventMerger", func(t *testing.T) {
		// Test creating a new event merger with no DDL events
		merger := newEventMerger(nil)
		require.NotNil(t, merger)
		require.Equal(t, 0, len(merger.ddlEvents))
		require.Equal(t, 0, merger.ddlIndex)
	})

	t.Run("appendDMLEventNoDDL", func(t *testing.T) {
		merger := newEventMerger(nil)

		helper := event.NewEventTestHelper(t)
		defer helper.Close()
		ddlEvent, kvEvents := genEvents(helper,
			`create table test.t(id int primary key, c char(50))`,
			[]string{
				`insert into test.t(id,c) values (0, "c0")`,
			}...)

		tableID := ddlEvent.TableInfo.TableName.TableID
		mockSchemaGetter := NewMockSchemaStore()
		mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

		processor := newDMLProcessor(&mockMounter{}, mockSchemaGetter, nil, false)
		processor.startTxn(dispatcherID, tableID, ddlEvent.TableInfo, kvEvents[0].StartTs, kvEvents[0].CRTs)

		err := processor.appendRow(kvEvents[0])
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

		// Only return DML event
		require.Equal(t, 1, len(events))
		require.Equal(t, event.TypeBatchDMLEvent, events[0].GetType())
		require.Equal(t, kvEvents[0].CRTs, events[0].GetCommitTs())
		require.Equal(t, kvEvents[0].CRTs, merger.lastBatchDMLCommitTs)
	})

	t.Run("appendDMLEventWithDDL", func(t *testing.T) {
		helper := event.NewEventTestHelper(t)
		defer helper.Close()
		ddlEvent, kvEvents := genEvents(helper,
			`create table test.t(id int primary key, c char(50))`,
			[]string{
				`insert into test.t(id,c) values (0, "c0")`,
			}...)

		merger := newEventMerger([]event.Event{&ddlEvent})

		tableID := ddlEvent.TableInfo.TableName.TableID
		mockSchemaGetter := NewMockSchemaStore()
		mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)
		processor := newDMLProcessor(&mockMounter{}, mockSchemaGetter, nil, false)

		processor.startTxn(dispatcherID, tableID, ddlEvent.TableInfo, kvEvents[0].StartTs, kvEvents[0].CRTs)

		err := processor.appendRow(kvEvents[0])
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		events := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

		require.Equal(t, 2, len(events))

		require.Equal(t, event.TypeDDLEvent, events[0].GetType())

		require.Equal(t, event.TypeBatchDMLEvent, events[1].GetType())
		require.Equal(t, kvEvents[0].CRTs, events[1].GetCommitTs())

		require.Equal(t, merger.lastBatchDMLCommitTs, kvEvents[0].CRTs)
	})

	t.Run("MixedDMLAndDDLEvents", func(t *testing.T) {
		helper := event.NewEventTestHelper(t)
		defer helper.Close()

		mockSchemaGetter := NewMockSchemaStore()

		ddlEvent1, kvEvents1 := genEvents(helper,
			`create table test.t1(id int primary key, c char(50))`,
			[]string{
				`insert into test.t1(id,c) values (1, "c1")`,
			}...)
		mockSchemaGetter.AppendDDLEvent(ddlEvent1.TableInfo.TableName.TableID, ddlEvent1)

		ddlEvent2 := event.DDLEvent{
			FinishedTs: kvEvents1[0].CRTs + 10, // DDL2 after DML1
			TableInfo:  ddlEvent1.TableInfo,
		}
		mockSchemaGetter.AppendDDLEvent(ddlEvent2.TableInfo.TableName.TableID, ddlEvent2)

		ddlEvent3 := event.DDLEvent{
			FinishedTs: kvEvents1[0].CRTs + 30, // DDL3 after DML1
			TableInfo:  ddlEvent1.TableInfo,
		}
		mockSchemaGetter.AppendDDLEvent(ddlEvent3.TableInfo.TableName.TableID, ddlEvent3)

		ddlEvents := []event.Event{&ddlEvent1, &ddlEvent2, &ddlEvent3}
		merger := newEventMerger(ddlEvents)

		tableID := ddlEvent1.TableInfo.TableName.TableID
		tableInfo := ddlEvent1.TableInfo
		processor := newDMLProcessor(mounter, mockSchemaGetter, nil, false)

		processor.startTxn(dispatcherID, tableID, tableInfo, kvEvents1[0].StartTs, kvEvents1[0].CRTs)

		// Create first DML event (timestamp after DDL1, before DDL2)
		err := processor.appendRow(kvEvents1[0])
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		events1 := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

		// Should return DDL1 + DML1
		require.Equal(t, 2, len(events1))

		require.Equal(t, event.TypeDDLEvent, events1[0].GetType())
		require.Equal(t, ddlEvent1.FinishedTs, events1[0].GetCommitTs())

		require.Equal(t, event.TypeBatchDMLEvent, events1[1].GetType())
		require.Equal(t, kvEvents1[0].CRTs, events1[1].GetCommitTs())

		// Create second DML event (timestamp after DDL2 and DDL3)
		processor.startTxn(dispatcherID, tableID, tableInfo, kvEvents1[0].StartTs+50, kvEvents1[0].CRTs+50)

		err = processor.appendRow(kvEvents1[0])
		require.NoError(t, err)

		err = processor.commitTxn()
		require.NoError(t, err)

		events2 := merger.mergeWithPrecedingDDLs(processor.getCurrentBatchDML())

		// Should return DDL2 + DDL3 + DML2
		require.Equal(t, 3, len(events2))

		require.Equal(t, event.TypeDDLEvent, events2[0].GetType())
		require.Equal(t, ddlEvent2.FinishedTs, events2[0].GetCommitTs())

		require.Equal(t, event.TypeDDLEvent, events2[1].GetType())
		require.Equal(t, ddlEvent3.FinishedTs, events2[1].GetCommitTs())

		require.Equal(t, event.TypeBatchDMLEvent, events2[2].GetType())
		require.Equal(t, kvEvents1[0].CRTs+50, events2[2].GetCommitTs())
	})

	t.Run("AppendRemainingDDLsBoundary", func(t *testing.T) {
		helper := event.NewEventTestHelper(t)
		defer helper.Close()

		ddlEvent1, _ := genEvents(helper,
			`create table test.t1(id int primary key, c char(50))`,
			[]string{
				`insert into test.t1(id,c) values (1, "c1")`,
			}...)

		ddlEvent2 := event.DDLEvent{
			FinishedTs: 100,
			TableInfo:  ddlEvent1.TableInfo,
		}
		ddlEvent3 := event.DDLEvent{
			FinishedTs: 200,
			TableInfo:  ddlEvent1.TableInfo,
		}
		ddlEvent4 := event.DDLEvent{
			FinishedTs: 300,
			TableInfo:  ddlEvent1.TableInfo,
		}

		ddlEvents := []event.Event{&ddlEvent2, &ddlEvent3, &ddlEvent4}
		merger := newEventMerger(ddlEvents)

		// Test endTs is exactly equal to some DDL's FinishedTs
		events1 := merger.resolveDDLEvents(300)
		require.Equal(t, 3, len(events1)) // DDL2 + DDL3

		require.Equal(t, event.TypeDDLEvent, events1[0].GetType())
		require.Equal(t, uint64(100), events1[0].GetCommitTs())

		require.Equal(t, event.TypeDDLEvent, events1[1].GetType())
		require.Equal(t, uint64(200), events1[1].GetCommitTs())

		require.Equal(t, event.TypeDDLEvent, events1[2].GetType())
		require.Equal(t, uint64(300), events1[2].GetCommitTs())
	})
}

// TestScanAndMergeEventsSingleUKUpdate tests scanAndMergeEvents function with a single event that updates UK
func TestScanAndMergeEventsSingleUKUpdate(t *testing.T) {
	// Setup helper and table info with unique key
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	// Create table with unique key on column 'a'
	helper.Tk().MustExec("use test")
	ddlEvent := helper.DDL2Event("create table t_uk (id int primary key, a int, b char(50), unique key uk_a(a))")
	tableID := ddlEvent.TableInfo.TableName.TableID

	// Generate update event that changes UK
	_, updateEvent := helper.DML2UpdateEvent("test", "t_uk",
		"insert into test.t_uk(id,a,b) values (1, 10, 'old_b')",
		"update test.t_uk set a = 20 where id = 1")

	// Create mock components
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, *ddlEvent)

	// Create a mock iterator that returns only the single update event
	mockIter := &mockEventIterator{
		events: []*common.RawKVEntry{updateEvent},
	}

	// Create event scanner
	scanner := &eventScanner{
		mounter:      event.NewMounter(time.UTC, &integrity.Config{}),
		schemaGetter: mockSchemaGetter,
	}
	dispatcherID := common.NewDispatcherID()

	// Create scan session
	ctx := context.Background()

	disInfo := newMockDispatcherInfoForTest(t)
	stat := &dispatcherStat{
		info:      disInfo,
		id:        dispatcherID,
		isRemoved: atomic.Bool{},
	}

	dataRange := common.DataRange{
		Span: &heartbeatpb.TableSpan{
			TableID: tableID,
		},
		CommitTsStart: updateEvent.StartTs,
		CommitTsEnd:   updateEvent.CRTs + 100,
	}

	limit := scanLimit{
		maxDMLBytes: 1000,
	}

	sess := &session{
		ctx:            ctx,
		dispatcherStat: stat,
		dataRange:      dataRange,
		limit:          limit,
		startTime:      time.Now(),
		events:         make([]event.Event, 0),
	}
	merger := newEventMerger([]event.Event{})

	// Execute scanAndMergeEvents
	isInterrupted, err := scanner.scanAndMergeEvents(sess, merger, mockIter)
	events := sess.events

	// Verify results
	require.NoError(t, err)
	require.False(t, isInterrupted)
	require.Equal(t, 2, len(events)) // BatchDML + ResolvedEvent

	// Verify first event is BatchDMLEvent
	batchDML, ok := events[0].(*event.BatchDMLEvent)
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
	require.Equal(t, common.RowTypeDelete, row1.RowType)
	require.Equal(t, int64(1), row1.PreRow.GetInt64(0))
	require.Equal(t, int64(10), row1.PreRow.GetInt64(1))
	require.Equal(t, "old_b", string(row1.PreRow.GetBytes(2)))
	row2, ok := dmlEvent.GetNextRow()
	require.True(t, ok)
	require.Equal(t, common.RowTypeInsert, row2.RowType)
	require.Equal(t, int64(1), row2.Row.GetInt64(0))
	require.Equal(t, int64(20), row2.Row.GetInt64(1))
	require.Equal(t, "old_b", string(row2.Row.GetBytes(2)))

	// Verify second event is ResolvedEvent
	resolvedEvent, ok := events[1].(event.ResolvedEvent)
	require.True(t, ok)
	require.Equal(t, dispatcherID, resolvedEvent.DispatcherID)
	require.Equal(t, dataRange.CommitTsEnd, resolvedEvent.ResolvedTs)

	// Verify sess state was updated correctly
	require.Equal(t, updateEvent.CRTs, merger.lastBatchDMLCommitTs)
	require.Equal(t, 1, sess.dmlCount)
	require.True(t, sess.scannedBytes > 0) // Some bytes were processed
}

type schemaStoreWithErr struct {
	*mockSchemaStore
	getTableInfoError error
}

func (s *schemaStoreWithErr) GetTableInfo(keyspaceMeta common.KeyspaceMeta, tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	if s.getTableInfoError != nil {
		return nil, s.getTableInfoError
	}
	return s.mockSchemaStore.GetTableInfo(keyspaceMeta, tableID, ts)
}

func TestGetTableInfo4Txn(t *testing.T) {
	// Setup
	broker, _, mockSS, _ := newEventBrokerForTest()
	broker.close()

	disInfo := newMockDispatcherInfoForTest(t)
	disInfo.startTs = uint64(100)
	changefeedStatus := broker.getOrSetChangefeedStatus(disInfo.GetChangefeedID())
	tableID := disInfo.GetTableSpan().TableID

	disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)

	// Prepare a table info for success case
	helper := event.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, _ := genEvents(helper, `create table test.t(id int primary key, c char(50))`)
	mockSS.AppendDDLEvent(tableID, ddlEvent)
	ts := ddlEvent.TableInfo.GetUpdateTS()

	schemaStore := &schemaStoreWithErr{mockSchemaStore: mockSS}
	scanner := newEventScanner(broker.eventStore, schemaStore, &mockMounter{}, 0)

	// Case 1: Success
	t.Run("Success", func(t *testing.T) {
		info, err := scanner.getTableInfo4Txn(disp, tableID, ts)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, ddlEvent.TableInfo, info)
	})

	// Case 2: Dispatcher removed
	t.Run("DispatcherRemoved", func(t *testing.T) {
		schemaStore.getTableInfoError = errors.New("some error")
		disp.isRemoved.Store(true)
		defer disp.isRemoved.Store(false) // reset state

		info, err := scanner.getTableInfo4Txn(disp, tableID, ts)
		require.NoError(t, err)
		require.Nil(t, info)
	})

	// Case 3: Table deleted
	t.Run("TableDeleted", func(t *testing.T) {
		schemaStore.getTableInfoError = &schemastore.TableDeletedError{}

		info, err := scanner.getTableInfo4Txn(disp, tableID, ts)
		require.NoError(t, err)
		require.Nil(t, info)
	})

	// Case 4: Other error
	t.Run("OtherError", func(t *testing.T) {
		otherErr := errors.New("other error")
		schemaStore.getTableInfoError = otherErr

		info, err := scanner.getTableInfo4Txn(disp, tableID, ts)
		require.Error(t, err)
		require.Equal(t, otherErr, err)
		require.Nil(t, info)
	})
}

func TestHasDDLAtLastCommitTs(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	// Create DDL events with different commit timestamps
	ddlEvent1, _ := genEvents(helper, `create table test.t1(id int primary key)`)
	ddlEvent2, _ := genEvents(helper, `create table test.t2(id int primary key)`)
	ddlEvent3, _ := genEvents(helper, `create table test.t3(id int primary key)`)

	// Manually set commit timestamps to control the test scenarios
	ddlEvent1.FinishedTs = 100
	ddlEvent2.FinishedTs = 200
	ddlEvent3.FinishedTs = 300

	ddlEvents := []event.Event{&ddlEvent1, &ddlEvent2, &ddlEvent3}
	merger := newEventMerger(ddlEvents)

	// Test case 1: DDL exists at lastCommitTs - should return true
	merger.ddlIndex = 0
	originalIndex := merger.ddlIndex
	result := merger.hasDDLAtCommitTs(200)
	require.True(t, result, "Should return true when DDL exists at CommitTs")
	require.Equal(t, originalIndex, merger.ddlIndex, "ddlIndex should not be changed")

	// Test case 2: No DDL at lastCommitTs - should return false
	merger.ddlIndex = 0
	originalIndex = merger.ddlIndex
	result = merger.hasDDLAtCommitTs(150)
	require.False(t, result, "Should return false when no DDL exists at CommitTs")
	require.Equal(t, originalIndex, merger.ddlIndex, "ddlIndex should not be changed")

	// Test case 3: DDL exists but ddlIndex is beyond it - should return false
	merger.ddlIndex = 1 // Skip the first DDL
	originalIndex = merger.ddlIndex
	result = merger.hasDDLAtCommitTs(100)
	require.False(t, result, "Should return false when ddlIndex is beyond the target CommitTs")
	require.Equal(t, originalIndex, merger.ddlIndex, "ddlIndex should not be changed")
}

func TestCanInterrupt(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	// Create DML events with specific commit timestamps
	dmlEvent1 := &event.DMLEvent{
		StartTs:         100,
		CommitTs:        101, // x+1
		PhysicalTableID: 1,
		Length:          1,
	}
	dmlEvent2 := &event.DMLEvent{
		StartTs:         100,
		CommitTs:        102, // x+2
		PhysicalTableID: 1,
		Length:          1,
	}

	// Create DDL events
	ddlEvent1, _ := genEvents(helper, `create table test.t1(id int primary key)`)
	ddlEvent2, _ := genEvents(helper, `create table test.t2(id int primary key)`)

	// Set DDL commit timestamps
	ddlEvent1.FinishedTs = 102 // x+2 (same as dmlEvent2 and dmlEvent3)
	ddlEvent2.FinishedTs = 103 // x+3

	t.Run("Case 1 - Different commitTs, can interrupt", func(t *testing.T) {
		// Event sequence:
		//	DML1(x+1) -> DML2(x+2)
		//	          ▲
		//	          └── currentDML.commitTs=x+1, newCommitTs=x+2, can interrupt

		ddlEvents := []event.Event{&ddlEvent2} // DDL at x+3, not at x+1 or x+2
		merger := newEventMerger(ddlEvents)

		// Create BatchDMLEvent with DML1(x+1)
		currentBatchDML := event.NewBatchDMLEvent()
		currentBatchDML.DMLEvents = []*event.DMLEvent{dmlEvent1}

		// Test: currentDML.commitTs=101(x+1), newCommitTs=102(x+2)
		result := merger.canInterrupt(102, currentBatchDML)
		require.True(t, result, "Should be able to interrupt when commitTs are different")
	})

	t.Run("Case 2 - Same commitTs, no DDL, can interrupt", func(t *testing.T) {
		// Event sequence:
		//	DML1(x+1) -> DML2(x+2) -> DML3(x+2)
		//	                       ▲
		//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, no DDL at x+2, can interrupt

		ddlEvents := []event.Event{&ddlEvent2} // DDL at x+3, not at x+2
		merger := newEventMerger(ddlEvents)

		// Create BatchDMLEvent with DML2(x+2)
		currentBatchDML := event.NewBatchDMLEvent()
		currentBatchDML.DMLEvents = []*event.DMLEvent{dmlEvent2}

		// Test: currentDML.commitTs=102(x+2), newCommitTs=102(x+2), no DDL at 102
		result := merger.canInterrupt(102, currentBatchDML)
		require.True(t, result, "Should be able to interrupt when same commitTs but no DDL exists")
	})

	t.Run("Case 3 - Same commitTs, has DDL, cannot interrupt", func(t *testing.T) {
		// Event sequence:
		//	DML1(x+1) -> DML2(x+2) -> DDL(x+2) -> DML3(x+2)
		//	                       ▲
		//	                       └── currentDML.commitTs=x+2, newCommitTs=x+2, DDL exists at x+2, cannot interrupt
		//	                           Must process DML2, DML3, DDL together atomically

		ddlEvents := []event.Event{&ddlEvent1} // DDL at x+2 (102)
		merger := newEventMerger(ddlEvents)

		// Create BatchDMLEvent with DML2(x+2)
		currentBatchDML := event.NewBatchDMLEvent()
		currentBatchDML.DMLEvents = []*event.DMLEvent{dmlEvent2}

		// Test: currentDML.commitTs=102(x+2), newCommitTs=102(x+2), DDL exists at 102
		result := merger.canInterrupt(102, currentBatchDML)
		require.False(t, result, "Should NOT be able to interrupt when same commitTs and DDL exists")
	})

	t.Run("Edge case - Empty BatchDMLEvent", func(t *testing.T) {
		// Test with empty BatchDMLEvent (no DML events)
		ddlEvents := []event.Event{&ddlEvent1}
		merger := newEventMerger(ddlEvents)

		// Create empty BatchDMLEvent
		currentBatchDML := event.NewBatchDMLEvent()

		// Test: currentDML.commitTs=0 (empty), newCommitTs=102
		result := merger.canInterrupt(102, currentBatchDML)
		require.True(t, result, "Should be able to interrupt when currentBatchDML is empty")
	})
}
