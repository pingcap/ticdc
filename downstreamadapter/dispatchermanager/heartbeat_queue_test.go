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

package dispatchermanager

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatRequestQueueEnqueueCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := &HeartbeatRequestQueue{queue: make(chan *HeartBeatRequestWithTargetID, 1)}
		first, second := &HeartBeatRequestWithTargetID{}, &HeartBeatRequestWithTargetID{}
		require.True(t, queue.Enqueue(t.Context(), first))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan bool, 1)
		go func() { result <- queue.Enqueue(ctx, second) }()
		synctest.Wait()
		require.Empty(t, result, "a full queue must preserve backpressure")
		cancel()
		synctest.Wait()
		require.False(t, <-result)
		require.Same(t, first, queue.Dequeue(t.Context()))
		require.False(t, queue.Enqueue(ctx, second), "an already canceled producer must not enqueue")
		require.Empty(t, queue.queue)
		require.True(t, queue.Enqueue(t.Context(), second), "other producers can still use the queue")
		require.Same(t, second, queue.Dequeue(t.Context()))
	})
}

func TestBlockStatusRequestQueueEnqueueCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := NewBlockStatusRequestQueue()
		queue.queue = make(chan *BlockStatusRequestWithTargetID, 1)
		targetID, dispatcherID := node.NewID(), common.NewDispatcherID()
		first := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
		second := newDoneBlockStatusRequest(targetID, dispatcherID, 101)
		require.True(t, queue.Enqueue(t.Context(), first))
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan bool, 1)
		go func() { result <- queue.Enqueue(ctx, second) }()
		synctest.Wait()
		require.Empty(t, result, "a full queue must preserve backpressure")
		require.Len(t, queue.requestStatusKeys, 2)
		cancel()
		synctest.Wait()
		require.False(t, <-result)
		require.Len(t, queue.requestStatusKeys, 1)
		require.Len(t, queue.queuedStatuses, 1)
		require.Empty(t, queue.inFlightStatuses)
		require.Same(t, first, queue.Dequeue(t.Context()))
		queue.OnSendComplete(first)
		require.False(t, queue.Enqueue(ctx, second))
		require.Empty(t, queue.requestStatusKeys)
		require.Empty(t, queue.queuedStatuses)
		require.True(t, queue.Enqueue(t.Context(), second), "canceled statuses must remain retryable")
		require.Same(t, second, queue.Dequeue(t.Context()))
		queue.OnSendComplete(second)
		require.Empty(t, queue.requestStatusKeys)
		require.Empty(t, queue.inFlightStatuses)
	})
}

func TestBlockStatusRequestQueueDeduplicatesQueuedAndInFlightDone(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	first := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	second := newDoneBlockStatusRequest(targetID, dispatcherID, 100)

	queue.Enqueue(t.Context(), first)
	queue.Enqueue(t.Context(), second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dequeued := queue.Dequeue(ctx)
	require.Same(t, first, dequeued)

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	require.Nil(t, queue.Dequeue(shortCtx))

	third := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	queue.Enqueue(t.Context(), third)

	shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel2()
	require.Nil(t, queue.Dequeue(shortCtx2))

	queue.OnSendComplete(dequeued)

	fourth := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	queue.Enqueue(t.Context(), fourth)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, fourth, queue.Dequeue(ctx2))
}

func TestBlockStatusRequestQueueDeduplicatesQueuedAndInFlightWaiting(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	first := &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   101,
						Stage:     heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}
	second := &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   101,
						Stage:     heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}

	queue.Enqueue(t.Context(), first)
	queue.Enqueue(t.Context(), second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dequeued := queue.Dequeue(ctx)
	require.Same(t, first, dequeued)

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	require.Nil(t, queue.Dequeue(shortCtx))

	third := &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   101,
						Stage:     heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}
	queue.Enqueue(t.Context(), third)

	shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel2()
	require.Nil(t, queue.Dequeue(shortCtx2))

	queue.OnSendComplete(dequeued)

	fourth := &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   101,
						Stage:     heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}
	queue.Enqueue(t.Context(), fourth)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, fourth, queue.Dequeue(ctx2))
}

func TestBlockStatusRequestQueueKeepsDistinctDoneKeys(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	defaultDone := newDoneBlockStatusRequest(targetID, dispatcherID, 200)
	syncPointDone := newDoneBlockStatusRequest(targetID, dispatcherID, 200)
	syncPointDone.Request.BlockStatuses[0].State.IsSyncPoint = true
	redoDone := newDoneBlockStatusRequest(targetID, dispatcherID, 200)
	redoDone.Request.BlockStatuses[0].Mode = common.RedoMode
	redoDone.Request.Mode = common.RedoMode

	queue.Enqueue(t.Context(), defaultDone)
	queue.Enqueue(t.Context(), syncPointDone)
	queue.Enqueue(t.Context(), redoDone)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Same(t, defaultDone, queue.Dequeue(ctx))
	queue.OnSendComplete(defaultDone)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, syncPointDone, queue.Dequeue(ctx2))
	queue.OnSendComplete(syncPointDone)

	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()
	require.Same(t, redoDone, queue.Dequeue(ctx3))
}

func TestBlockStatusRequestQueueKeepsWaitingAndDoneDistinct(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	waiting := &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   300,
						Stage:     heartbeatpb.BlockStage_WAITING,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}
	done := newDoneBlockStatusRequest(targetID, dispatcherID, 300)

	queue.Enqueue(t.Context(), waiting)
	queue.Enqueue(t.Context(), done)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Same(t, waiting, queue.Dequeue(ctx))

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, done, queue.Dequeue(ctx2))
}

func newDoneBlockStatusRequest(targetID node.ID, dispatcherID common.DispatcherID, blockTs uint64) *BlockStatusRequestWithTargetID {
	return &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked: true,
						BlockTs:   blockTs,
						Stage:     heartbeatpb.BlockStage_DONE,
					},
					Mode: common.DefaultMode,
				},
			},
			Mode: common.DefaultMode,
		},
	}
}
