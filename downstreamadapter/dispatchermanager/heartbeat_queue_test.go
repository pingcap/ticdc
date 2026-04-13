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
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestBlockStatusRequestQueueDeduplicatesQueuedAndInFlightDone(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	first := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	second := newDoneBlockStatusRequest(targetID, dispatcherID, 100)

	queue.Enqueue(first)
	queue.Enqueue(second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dequeued := queue.Dequeue(ctx)
	require.Same(t, first, dequeued)

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel()
	require.Nil(t, queue.Dequeue(shortCtx))

	third := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	queue.Enqueue(third)

	shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer shortCancel2()
	require.Nil(t, queue.Dequeue(shortCtx2))

	queue.OnSendComplete(dequeued)

	fourth := newDoneBlockStatusRequest(targetID, dispatcherID, 100)
	queue.Enqueue(fourth)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, fourth, queue.Dequeue(ctx2))
}

func TestBlockStatusRequestQueueDoesNotDeduplicateWaiting(t *testing.T) {
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

	queue.Enqueue(first)
	queue.Enqueue(second)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.Same(t, first, queue.Dequeue(ctx))

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	require.Same(t, second, queue.Dequeue(ctx2))
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

	queue.Enqueue(defaultDone)
	queue.Enqueue(syncPointDone)
	queue.Enqueue(redoDone)

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
