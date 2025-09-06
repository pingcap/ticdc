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
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestNewDispatcherStat(t *testing.T) {
	t.Parallel()

	startTs := uint64(50)
	// Mock dispatcher info
	info := newMockDispatcherInfo(
		t,
		startTs,
		common.NewDispatcherID(),
		1,
		eventpb.ActionType_ACTION_TYPE_REGISTER,
	)

	workerIndex := 1
	status := newChangefeedStatus(info.GetChangefeedID())
	stat := newDispatcherStat(info, info.filter, workerIndex, workerIndex, status)

	require.Equal(t, info.GetID(), stat.id)
	require.Equal(t, workerIndex, stat.messageWorkerIndex)
	require.Equal(t, uint64(0), stat.resetTs.Load())
	require.Equal(t, startTs, stat.eventStoreResolvedTs.Load())
	require.Equal(t, startTs, stat.checkpointTs.Load())
	require.Equal(t, startTs, stat.sentResolvedTs.Load())
	require.True(t, stat.isReadyReceivingData.Load())
	require.False(t, stat.enableSyncPoint)
	require.Equal(t, info.GetSyncPointTs(), stat.nextSyncPoint.Load())
	require.Equal(t, info.GetSyncPointInterval(), stat.syncPointInterval)
}

func TestResetSyncpoint(t *testing.T) {
	t.Parallel()

	now := time.Now()
	firstSyncPoint := oracle.GoTimeToTS(now)
	syncPointInterval := time.Second * 10
	secondSyncPoint := oracle.GoTimeToTS(oracle.GetTimeFromTS(firstSyncPoint).Add(syncPointInterval))
	thirdSyncPoint := oracle.GoTimeToTS(oracle.GetTimeFromTS(firstSyncPoint).Add(2 * syncPointInterval))
	startTs := oracle.GoTimeToTS(now.Add(-2 * time.Second))

	info := newMockDispatcherInfo(t, startTs, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	info.enableSyncPoint = true
	info.nextSyncPoint = firstSyncPoint
	info.syncPointInterval = syncPointInterval
	status := newChangefeedStatus(info.GetChangefeedID())
	stat := newDispatcherStat(info, info.filter, 1, 1, status)

	stat.nextSyncPoint.Store(thirdSyncPoint)
	stat.resetState(secondSyncPoint)
	require.Equal(t, thirdSyncPoint, stat.nextSyncPoint.Load())
	stat.resetState(secondSyncPoint - 1)
	require.Equal(t, secondSyncPoint, stat.nextSyncPoint.Load())
	stat.resetState(startTs)
	require.Equal(t, firstSyncPoint, stat.nextSyncPoint.Load())
}

func TestDispatcherStatResolvedTs(t *testing.T) {
	t.Parallel()

	info := newMockDispatcherInfo(t, 100, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	status := newChangefeedStatus(info.GetChangefeedID())
	stat := newDispatcherStat(info, info.filter, 1, 1, status)

	// Test normal update
	updated := stat.onResolvedTs(150)
	require.True(t, updated)
	require.Equal(t, uint64(150), stat.eventStoreResolvedTs.Load())

	// Test same ts update
	updated = stat.onResolvedTs(150)
	require.False(t, updated)
}

func TestDispatcherStatGetDataRange(t *testing.T) {
	t.Parallel()

	info := newMockDispatcherInfo(t, 100, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	status := newChangefeedStatus(info.GetChangefeedID())
	stat := newDispatcherStat(info, info.filter, 1, 1, status)
	stat.eventStoreResolvedTs.Store(200)

	// Normal case
	r, ok := stat.getDataRange()
	require.True(t, ok)
	require.Equal(t, uint64(100), r.CommitTsStart)
	require.Equal(t, uint64(200), r.CommitTsEnd)
	require.Equal(t, info.GetTableSpan(), r.Span)

	// When watermark equals resolvedTs
	stat.isHandshaked.Store(true)
	stat.updateSentResolvedTs(200)
	r, ok = stat.getDataRange()
	require.False(t, ok)

	// When reset, the data range should be start from the reset ts.
	stat.resetState(150)
	r, ok = stat.getDataRange()
	require.True(t, ok)
	require.Equal(t, uint64(150), r.CommitTsStart)
}

func TestDispatcherStatUpdateWatermark(t *testing.T) {
	startTs := uint64(100)
	info := newMockDispatcherInfo(t, startTs, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	status := newChangefeedStatus(info.GetChangefeedID())
	stat := newDispatcherStat(info, info.filter, 1, 1, status)

	// Case 1: no new events, only watermark change
	stat.onResolvedTs(200)
	require.Equal(t, uint64(200), stat.eventStoreResolvedTs.Load())

	// Case 2: new events, and watermark increase
	stat.onLatestCommitTs(300)
	stat.onResolvedTs(400)
	require.Equal(t, uint64(300), stat.eventStoreCommitTs.Load())
	require.Equal(t, uint64(400), stat.eventStoreResolvedTs.Load())

	// Case 3: new events, and watermark decrease
	// watermark should not decrease
	stat.onLatestCommitTs(500)
	stat.onResolvedTs(300)
	require.Equal(t, uint64(500), stat.eventStoreCommitTs.Load())
	require.Equal(t, uint64(400), stat.eventStoreResolvedTs.Load())
}

func TestResolvedTsCache(t *testing.T) {
	rc := newResolvedTsCache(10)
	require.Equal(t, 0, rc.len)
	require.Equal(t, 10, len(rc.cache))
	require.Equal(t, 10, rc.limit)

	// Case 1: insert a new resolved ts
	rc.add(pevent.ResolvedEvent{
		DispatcherID: common.NewDispatcherID(),
		ResolvedTs:   100,
	})
	require.Equal(t, 1, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.False(t, rc.isFull())

	// Case 2: add more resolved ts until full
	i := 1
	for !rc.isFull() {
		rc.add(pevent.ResolvedEvent{
			DispatcherID: common.NewDispatcherID(),
			ResolvedTs:   uint64(100 + i),
		})
		i++
	}
	require.Equal(t, 10, rc.len)
	require.Equal(t, uint64(100), rc.cache[0].ResolvedTs)
	require.Equal(t, uint64(109), rc.cache[9].ResolvedTs)
	require.True(t, rc.isFull())

	// Case 3: get all resolved ts
	res := rc.getAll()
	require.Equal(t, 10, len(res))
	require.Equal(t, 0, rc.len)
	require.Equal(t, uint64(100), res[0].ResolvedTs)
	require.Equal(t, uint64(109), res[9].ResolvedTs)
	require.False(t, rc.isFull())
}
