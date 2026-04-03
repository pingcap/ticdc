// Copyright 2026 PingCAP, Inc.
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

package logpuller

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

func TestScheduleRegionRequestAfter(t *testing.T) {
	client := &subscriptionClient{
		rangeTaskCh:     make(chan rangeTask, 8),
		regionTaskQueue: NewPriorityQueue(),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()
	client.pdClock = pdutil.NewClock4Test()
	client.retryScheduler = newRetryScheduler(client)
	client.retryBackoff = newRetryBackoffManager()
	client.storeBackoff = newStoreBackoffManager()

	done := make(chan error, 1)
	go func() {
		done <- client.retryScheduler.run(client.ctx)
	}()

	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	span := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	rt := client.newSubscribedSpan(SubscriptionID(1), span, 100, consumeKVEvents, advanceResolvedTs, 0, false)
	region := newRegionInfo(tikv.NewRegionVerID(11, 1, 1), heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("b"), EndKey: []byte("c")}, nil, rt, false)

	client.scheduleRegionRequestAfter(client.ctx, region, TaskHighPrior, 50*time.Millisecond)

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, client.regionTaskQueue.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := client.regionTaskQueue.Pop(ctx)
	require.NoError(t, err)
	regionInfo := task.GetRegionInfo()
	require.Equal(t, uint64(11), regionInfo.verID.GetID())

	client.cancel()
	select {
	case err := <-done:
		require.Equal(t, context.Canceled, err)
	case <-time.After(time.Second):
		t.Fatal("retry scheduler should exit after cancel")
	}
}

func TestRangeReloadAggregatorMergesSpans(t *testing.T) {
	client := &subscriptionClient{
		rangeTaskCh: make(chan rangeTask, 8),
	}
	client.ctx, client.cancel = context.WithCancel(context.Background())
	defer client.cancel()
	client.retryScheduler = newRetryScheduler(client)
	client.rangeReloadAggregator = newRangeReloadAggregator(client, 10*time.Millisecond, 16)

	done := make(chan error, 1)
	go func() {
		done <- client.retryScheduler.run(client.ctx)
	}()

	consumeKVEvents := func(_ []common.RawKVEntry, _ func()) bool { return false }
	advanceResolvedTs := func(ts uint64) {}
	rawSpan := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("z")}
	rt := client.newSubscribedSpan(SubscriptionID(1), rawSpan, 100, consumeKVEvents, advanceResolvedTs, 0, false)

	client.addReloadRangeTask(heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}, rt, false, TaskHighPrior)
	client.addReloadRangeTask(heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("b"), EndKey: []byte("c")}, rt, false, TaskLowPrior)
	client.addReloadRangeTask(heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("d"), EndKey: []byte("e")}, rt, false, TaskHighPrior)

	var tasks []rangeTask
	deadline := time.After(time.Second)
	for len(tasks) < 2 {
		select {
		case task := <-client.rangeTaskCh:
			tasks = append(tasks, task)
		case <-deadline:
			t.Fatal("timed out waiting for merged range reload tasks")
		}
	}
	require.Len(t, tasks, 2)
	require.Equal(t, []byte("a"), tasks[0].span.StartKey)
	require.Equal(t, []byte("c"), tasks[0].span.EndKey)
	require.Equal(t, TaskHighPrior, tasks[0].priority)
	require.Equal(t, []byte("d"), tasks[1].span.StartKey)
	require.Equal(t, []byte("e"), tasks[1].span.EndKey)

	select {
	case extra := <-client.rangeTaskCh:
		t.Fatalf("unexpected extra range task: %+v", extra)
	case <-time.After(30 * time.Millisecond):
	}

	client.cancel()
	select {
	case err := <-done:
		require.Equal(t, context.Canceled, err)
	case <-time.After(time.Second):
		t.Fatal("retry scheduler should exit after cancel")
	}
}

func TestMergeTableSpans(t *testing.T) {
	spans := []heartbeatpb.TableSpan{
		{StartKey: []byte("d"), EndKey: []byte("e")},
		{StartKey: []byte("a"), EndKey: []byte("b")},
		{StartKey: []byte("b"), EndKey: []byte("c")},
		{StartKey: []byte("c"), EndKey: []byte("d")},
	}
	merged := mergeTableSpans(spans)
	require.Len(t, merged, 1)
	require.Equal(t, []byte("a"), merged[0].StartKey)
	require.Equal(t, []byte("e"), merged[0].EndKey)
}

func TestStoreBackoffManager(t *testing.T) {
	m := newStoreBackoffManager()
	d1 := m.markFailure("store-1")
	d2 := m.markFailure("store-1")
	require.Greater(t, d2, d1)
	require.Greater(t, m.cooldownRemaining("store-1"), time.Duration(0))
	m.markSuccess("store-1")
	require.Equal(t, time.Duration(0), m.cooldownRemaining("store-1"))
}
