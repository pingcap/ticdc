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

package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestBlockStatusBufferDeduplicatesPendingDone(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()
	firstDone := newBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)
	secondDone := newBlockStatus(dispatcherID, 100, heartbeatpb.BlockStage_DONE, false, common.DefaultMode)

	buffer.Offer(firstDone)
	buffer.Offer(secondDone)

	require.Equal(t, 1, buffer.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := buffer.Take(ctx)
	require.NotNil(t, msg)
	require.Equal(t, dispatcherID, common.NewDispatcherIDFromPB(msg.ID))
	require.Equal(t, uint64(100), msg.State.BlockTs)
	require.Equal(t, heartbeatpb.BlockStage_DONE, msg.State.Stage)
	require.False(t, msg.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, msg.Mode)

	_, ok := buffer.TryTake()
	require.False(t, ok)
}

func TestBlockStatusBufferDeduplicatesPendingWaiting(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	waiting := newBlockStatus(common.NewDispatcherID(), 150, heartbeatpb.BlockStage_WAITING, false, common.DefaultMode)

	buffer.Offer(waiting)
	buffer.Offer(waiting)

	require.Equal(t, 1, buffer.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := buffer.Take(ctx)
	require.NotNil(t, msg)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, msg.State.Stage)

	_, ok := buffer.TryTake()
	require.False(t, ok)
}

func TestBlockStatusBufferAllowsStatusAgainAfterTake(t *testing.T) {
	for _, stage := range []heartbeatpb.BlockStage{
		heartbeatpb.BlockStage_WAITING,
		heartbeatpb.BlockStage_DONE,
	} {
		t.Run(stage.String(), func(t *testing.T) {
			buffer := NewBlockStatusBuffer(4)
			status := newBlockStatus(common.NewDispatcherID(), 180, stage, false, common.DefaultMode)

			buffer.Offer(status)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			first := buffer.Take(ctx)
			require.NotNil(t, first)
			require.Equal(t, stage, first.State.Stage)

			buffer.Offer(status)

			second := buffer.Take(ctx)
			require.NotNil(t, second)
			require.Equal(t, stage, second.State.Stage)

			_, ok := buffer.TryTake()
			require.False(t, ok)
		})
	}
}

func TestBlockStatusBufferKeepsStageOrderForSameBlockEvent(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(newBlockStatus(dispatcherID, 200, heartbeatpb.BlockStage_WAITING, false, common.DefaultMode))
	buffer.Offer(newBlockStatus(dispatcherID, 200, heartbeatpb.BlockStage_DONE, false, common.DefaultMode))
	buffer.Offer(newBlockStatus(dispatcherID, 200, heartbeatpb.BlockStage_DONE, false, common.DefaultMode))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.State.Stage)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.State.Stage)

	_, ok := buffer.TryTake()
	require.False(t, ok)
}

func TestBlockStatusBufferKeepsDistinctKeys(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(newBlockStatus(dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.DefaultMode))
	buffer.Offer(newBlockStatus(dispatcherID, 300, heartbeatpb.BlockStage_DONE, true, common.DefaultMode))
	buffer.Offer(newBlockStatus(dispatcherID, 300, heartbeatpb.BlockStage_DONE, false, common.RedoMode))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.False(t, first.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, first.Mode)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.True(t, second.State.IsSyncPoint)
	require.Equal(t, common.DefaultMode, second.Mode)

	third := buffer.Take(ctx)
	require.NotNil(t, third)
	require.False(t, third.State.IsSyncPoint)
	require.Equal(t, common.RedoMode, third.Mode)

	_, ok := buffer.TryTake()
	require.False(t, ok)
}

func newBlockStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	stage heartbeatpb.BlockStage,
	isSyncPoint bool,
	mode int64,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked:   true,
			BlockTs:     blockTs,
			IsSyncPoint: isSyncPoint,
			Stage:       stage,
		},
		Mode: mode,
	}
}
