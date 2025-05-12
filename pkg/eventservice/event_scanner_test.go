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
	// Close the broker, so we can catch all message in the test.
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

	scanner := NewEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{})

	// case 1:  only has resolvedTs
	// 测试当只有 resolvedTs 事件时，扫描器能正确返回该事件
	// 预期结果:
	// [Resolved(ts=102)]
	disp.eventStoreResolvedTs.Store(102)
	scanLimit := ScanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	needScan, dataRange := broker.checkNeedScan(disp, true)
	require.True(t, needScan)
	events, isBroken, err := scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 1, len(events))
	e := events[0]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, e.GetCommitTs(), uint64(102))

	// case 2: has new dml, ddl, and resolvedTs
	// 测试扫描器能正确处理混合事件（DDL + DML + resolvedTs）
	// 生成的事件序列:
	//   DDL(ts=x) -> DML(ts=x+1) -> DML(ts=x+2) -> DML(ts=x+3) -> DML(ts=x+4) -> Resolved(ts=x+5)
	// 预期结果:
	// [DDL(x), DML(x+1), DML(x+2), DML(x+3), DML(x+4), Resolved(x+5)]
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, t, `create table test.t(id int primary key, c char(50))`, []string{
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

	scanLimit = ScanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 6, len(events))

	// case 3: reach scan limit, only 1 ddl and 1 dml event was scanned
	// 测试当达到 MaxBytes 限制时，扫描器能部分返回事件并标记 isBroken=true
	// 预期结果:
	// [DDL(x), DML(x+1), Resolved(x+1)] (因大小限制只返回部分事件)
	//               ▲
	//               └── 此处中断扫描
	scanLimit = ScanLimit{
		MaxBytes: 1,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 3, len(events))
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())

	// case4: Tests transaction atomicity during scanning
	// 测试相同 commitTs 的事务会被原子性扫描（即使达到限制也不拆分）
	// 修改事件使前3个DML具有相同 commitTs=x:
	//   DDL(x) -> DML(x+1) -> DML(x+1) -> DML(x+1) -> DML(x+4)
	// 预期结果（MaxBytes=1）:
	// [DDL(x), DML(x+1), DML(x+1), DML(x+1), Resolved(x+1)]
	//                               ▲
	//                               └── 此处中断，相同 commitTs 的事件必须一起返回
	firstCommitTs := kvEvents[0].CRTs
	for i := 0; i < 3; i++ {
		kvEvents[i].CRTs = firstCommitTs
	}
	scanLimit = ScanLimit{
		MaxBytes: 1,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	// 1 ddl, 3 dmls and 1 resolvedTs
	require.Equal(t, 5, len(events))

	// ddl
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// dmls
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, kvEvents[0].CRTs, e.GetCommitTs())
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, kvEvents[1].CRTs, e.GetCommitTs())
	e = events[3]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, kvEvents[2].CRTs, e.GetCommitTs())
	// resolvedTs
	e = events[4]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, kvEvents[2].CRTs, e.GetCommitTs())

	// // case 5: Ensure timeout works
	// 测试超时机制，Timeout=0 时应立即返回已扫描的事件
	// 预期结果:
	// [DDL(x), DML(x+1), DML(x+1), DML(x+1), Resolved(x+1)]
	//                               ▲
	//                               └── 因超时中断
	scanLimit = ScanLimit{
		MaxBytes: 1000,
		Timeout:  0 * time.Millisecond,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 5, len(events))

	// case 6: Ensure DDL and DML with the same commitTs are sorted by DML first
	// 测试相同 commitTs 时 DML 优先于 DDL 返回
	// 添加 fakeDDL(ts=x) 后事件序列:
	//   DDL(x) -> DML(x+1) -> DML(x+1) -> DML(x+1) -> fakeDDL(x+1) -> DML(x+4)
	// 预期结果:
	// [DDL(x), DML(x+1), DML(x+1), DML(x+1), fakeDDL(x+1), DML(x+4), Resolved(x+5)]
	//                                ▲
	//                                └── DML 优先于同 ts 的 DDL
	fakeDDL := event.DDLEvent{
		FinishedTs: kvEvents[0].CRTs,
		TableInfo:  ddlEvent.TableInfo,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)
	scanLimit = ScanLimit{
		MaxBytes: 1000,
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 7, len(events))
	// The first DML event should be returned before the fake DDL event
	firstDML := events[1]
	require.Equal(t, firstDML.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, kvEvents[0].CRTs, firstDML.GetCommitTs())
	// The fake DDL event should be returned after the DML event
	ddl := events[2]
	require.Equal(t, ddl.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, ddl.GetCommitTs())
	require.Equal(t, fakeDDL.FinishedTs, firstDML.GetCommitTs())
	e = events[6]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}

// TestEventScanner_InterruptAtDDL tests the case that the scan is interrupted at a DDL event.
// The Scanner will return the DDL event and the DML events that have the same commitTs as the DDL event.
// The Scanner will also return the resolvedTs event with the commitTs of the last DML event.
func TestEventScannerWithDDL(t *testing.T) {
	broker, _, _ := newEventBrokerForTest()
	// Close the broker, so we can catch all message in the test.
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

	scanner := NewEventScanner(broker.eventStore, broker.schemaStore, &mockMounter{})

	// Construct events: dml 2, dml 3 have the same commitTs, fakeDDL  has the same commitTs with dml 2, dml 3
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

	// Create a fake ddl event with the same commitTs as dml 2 and dml 3
	dml1 := kvEvents[0]
	dml2 := kvEvents[1]
	dml3 := kvEvents[2]
	dml3.CRTs = dml2.CRTs
	fakeDDL := event.DDLEvent{
		FinishedTs: dml2.CRTs,
		TableInfo:  ddlEvent.TableInfo,
	}
	mockSchemaStore.AppendDDLEvent(tableID, fakeDDL)
	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange := broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	eSize := len(kvEvents[0].Key) + len(kvEvents[0].Value) + len(kvEvents[0].OldValue)

	// case 1: The scan will be interrupted at dml 1
	// 测试在第一个 DML 处因大小限制中断
	// 事件序列:
	//   DDL(x) -> DML1(x+1) -> DML2(x+2) ->fakeDDL(x+2) -> DML3(x+3)
	// 预期结果（MaxBytes=1*eSize）:
	// [DDL(x), DML1(x+1), Resolved(x+1)]
	//             ▲
	//             └── 在 DML1 处中断
	scanLimit := ScanLimit{
		MaxBytes: int64(1 * eSize),
		Timeout:  10 * time.Second,
	}
	events, isBroken, err := scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 3, len(events))
	// ddl
	e := events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// dml 1
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	// resolvedTs
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	require.Equal(t, dml1.CRTs, e.GetCommitTs())

	// case 2: The scan will be interrupted at dml 2
	// 测试能原子性返回相同 commitTs 的 DML2/DML3/fakeDDL
	// 预期结果（MaxBytes=2*eSize）:
	// [DDL(x), DML1(x+1), DML2(x+2), DML3(x+2), fakeDDL(x+2), Resolved(x+3)]
	//                                               ▲
	//                                               └── 相同 commitTs 的事件必须一起返回
	scanLimit = ScanLimit{
		MaxBytes: int64(2 * eSize),
		Timeout:  10 * time.Second,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 6, len(events))

	// ddl 1
	e = events[0]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, ddlEvent.FinishedTs, e.GetCommitTs())
	// dml 1
	e = events[1]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, dml1.CRTs, e.GetCommitTs())
	// dml 2
	e = events[2]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, dml2.CRTs, e.GetCommitTs())
	// dml 3
	e = events[3]
	require.Equal(t, e.GetType(), pevent.TypeDMLEvent)
	require.Equal(t, dml3.CRTs, e.GetCommitTs())
	// fake ddl
	e = events[4]
	require.Equal(t, e.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL.FinishedTs, e.GetCommitTs())
	require.Equal(t, dml3.CRTs, e.GetCommitTs())
	// resolvedTs
	e = events[5]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, dml3.CRTs, e.GetCommitTs())

	// case 3: Ensure the Scanner will return all remained DDLs
	// 测试能正确处理多个 DDL 事件
	// 添加事件后序列:
	//   ... -> fakeDDL2(x+5) -> fakeDDL3(x+6)
	// 预期结果:
	// [..., fakeDDL2(x+5), fakeDDL3(x+6), Resolved(x+7)]
	scanLimit = ScanLimit{
		MaxBytes: int64(100 * eSize),
		Timeout:  10 * time.Second,
	}

	// add more fake ddl events
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
	disp.eventStoreResolvedTs.Store(resolvedTs)
	needScan, dataRange = broker.checkNeedScan(disp, true)
	require.True(t, needScan)

	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.False(t, isBroken)
	require.Equal(t, 9, len(events))
	e = events[6]
	require.Equal(t, fakeDDL2.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL2.GetCommitTs(), fakeDDL2.FinishedTs)
	e = events[7]
	require.Equal(t, fakeDDL3.GetType(), pevent.TypeDDLEvent)
	require.Equal(t, fakeDDL3.GetCommitTs(), fakeDDL3.FinishedTs)
	e = events[8]
	require.Equal(t, e.GetType(), pevent.TypeResolvedEvent)
	require.Equal(t, resolvedTs, e.GetCommitTs())
}
