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

	// case 1: no new dml, ddl, only has resolvedTs
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

	// case4: Tests transaction atomicity during scanning. When multiple transactions share the same commitTs,
	// they must be scanned together as a single atomic unit, even if the scan limit is reached.
	// The scanner can only break between transactions with different commitTs values.
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

	// case 5: Ensure timeout works
	scanLimit = ScanLimit{
		MaxBytes: 1000,
		Timeout:  0 * time.Millisecond,
	}
	events, isBroken, err = scanner.Scan(context.Background(), disp, dataRange, scanLimit)
	require.NoError(t, err)
	require.True(t, isBroken)
	require.Equal(t, 5, len(events))
}
