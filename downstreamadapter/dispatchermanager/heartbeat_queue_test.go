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

func TestBlockStatusRequestQueueKeepsStatusReservedUntilSendAttemptFinishes(t *testing.T) {
	for _, stage := range []heartbeatpb.BlockStage{
		heartbeatpb.BlockStage_WAITING,
		heartbeatpb.BlockStage_DONE,
	} {
		t.Run(stage.String(), func(t *testing.T) {
			queue := NewBlockStatusRequestQueue()
			targetID := node.NewID()
			dispatcherID := common.NewDispatcherID()

			first := newBlockStatusRequest(targetID, dispatcherID, 100, stage, false, common.DefaultMode)
			second := newBlockStatusRequest(targetID, dispatcherID, 100, stage, false, common.DefaultMode)

			queue.Enqueue(first)
			queue.Enqueue(second)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			dequeued := queue.Dequeue(ctx)
			assertSingleBlockStatusRequest(t, dequeued, targetID, dispatcherID, 100, stage, false, common.DefaultMode)

			shortCtx, shortCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer shortCancel()
			require.Nil(t, queue.Dequeue(shortCtx))

			third := newBlockStatusRequest(targetID, dispatcherID, 100, stage, false, common.DefaultMode)
			queue.Enqueue(third)

			shortCtx2, shortCancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer shortCancel2()
			require.Nil(t, queue.Dequeue(shortCtx2))

			queue.OnSendAttemptFinished(dequeued)

			fourth := newBlockStatusRequest(targetID, dispatcherID, 100, stage, false, common.DefaultMode)
			queue.Enqueue(fourth)

			ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
			defer cancel2()
			assertSingleBlockStatusRequest(t, queue.Dequeue(ctx2), targetID, dispatcherID, 100, stage, false, common.DefaultMode)
		})
	}
}

func TestBlockStatusRequestQueueKeepsWaitingAndDoneDistinct(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	waiting := newBlockStatusRequest(targetID, dispatcherID, 200, heartbeatpb.BlockStage_WAITING, false, common.DefaultMode)
	done := newBlockStatusRequest(targetID, dispatcherID, 200, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)

	queue.Enqueue(waiting)
	queue.Enqueue(done)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dequeuedWaiting := queue.Dequeue(ctx)
	assertSingleBlockStatusRequest(t, dequeuedWaiting, targetID, dispatcherID, 200, heartbeatpb.BlockStage_WAITING, false, common.DefaultMode)
	queue.OnSendAttemptFinished(dequeuedWaiting)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	assertSingleBlockStatusRequest(t, queue.Dequeue(ctx2), targetID, dispatcherID, 200, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)
}

func TestBlockStatusRequestQueueKeepsDistinctKeys(t *testing.T) {
	queue := NewBlockStatusRequestQueue()
	targetID := node.NewID()
	dispatcherID := common.NewDispatcherID()

	defaultDone := newBlockStatusRequest(targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)
	syncPointDone := newBlockStatusRequest(targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, true, common.DefaultMode)
	redoDone := newBlockStatusRequest(targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.RedoMode)

	queue.Enqueue(defaultDone)
	queue.Enqueue(syncPointDone)
	queue.Enqueue(redoDone)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := queue.Dequeue(ctx)
	assertSingleBlockStatusRequest(t, first, targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)
	queue.OnSendAttemptFinished(first)

	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	second := queue.Dequeue(ctx2)
	assertSingleBlockStatusRequest(t, second, targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, true, common.DefaultMode)
	queue.OnSendAttemptFinished(second)

	ctx3, cancel3 := context.WithTimeout(context.Background(), time.Second)
	defer cancel3()
	assertSingleBlockStatusRequest(t, queue.Dequeue(ctx3), targetID, dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.RedoMode)
}

func newBlockStatusRequest(
	targetID node.ID,
	dispatcherID common.DispatcherID,
	blockTs uint64,
	stage heartbeatpb.BlockStage,
	isSyncPoint bool,
	mode int64,
) *BlockStatusRequestWithTargetID {
	return &BlockStatusRequestWithTargetID{
		TargetID: targetID,
		Request: &heartbeatpb.BlockStatusRequest{
			BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
				{
					ID: dispatcherID.ToPB(),
					State: &heartbeatpb.State{
						IsBlocked:   true,
						BlockTs:     blockTs,
						IsSyncPoint: isSyncPoint,
						Stage:       stage,
					},
					Mode: mode,
				},
			},
			Mode: mode,
		},
	}
}

func assertSingleBlockStatusRequest(
	t *testing.T,
	request *BlockStatusRequestWithTargetID,
	targetID node.ID,
	dispatcherID common.DispatcherID,
	blockTs uint64,
	stage heartbeatpb.BlockStage,
	isSyncPoint bool,
	mode int64,
) {
	t.Helper()
	require.NotNil(t, request)
	require.Equal(t, targetID, request.TargetID)
	require.NotNil(t, request.Request)
	require.Equal(t, mode, request.Request.Mode)
	require.Len(t, request.Request.BlockStatuses, 1)

	status := request.Request.BlockStatuses[0]
	require.Equal(t, dispatcherID, common.NewDispatcherIDFromPB(status.ID))
	require.Equal(t, blockTs, status.State.BlockTs)
	require.Equal(t, stage, status.State.Stage)
	require.Equal(t, isSyncPoint, status.State.IsSyncPoint)
	require.Equal(t, mode, status.Mode)
}
