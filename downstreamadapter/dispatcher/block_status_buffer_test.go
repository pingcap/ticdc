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
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestBlockStatusBufferDeduplicatesPendingDone(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 100, false, common.DefaultMode))
	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 100, false, common.DefaultMode))

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

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferDeduplicatesPendingWaiting(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	offerWaiting := func() {
		buffer.Offer(NewWaitingBlockStatusEntry(
			dispatcherID,
			150,
			nil,
			nil,
			nil,
			nil,
			false,
			common.DefaultMode,
		))
	}

	offerWaiting()
	offerWaiting()

	require.Equal(t, 1, buffer.Len())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	msg := buffer.Take(ctx)
	require.NotNil(t, msg)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, msg.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferAllowsWaitingAgainAfterTake(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	offerWaiting := func() {
		buffer.Offer(NewWaitingBlockStatusEntry(
			dispatcherID,
			180,
			nil,
			nil,
			nil,
			nil,
			false,
			common.DefaultMode,
		))
	}

	offerWaiting()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.State.Stage)

	offerWaiting()

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferAllowsDoneAgainAfterTake(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 190, false, common.DefaultMode))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_DONE, first.State.Stage)

	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 190, false, common.DefaultMode))

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferKeepsWaitingBeforeDone(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(NewWaitingBlockStatusEntry(
		dispatcherID,
		200,
		nil,
		nil,
		nil,
		nil,
		false,
		common.DefaultMode,
	))
	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 200, false, common.DefaultMode))
	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 200, false, common.DefaultMode))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := buffer.Take(ctx)
	require.NotNil(t, first)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.State.Stage)

	second := buffer.Take(ctx)
	require.NotNil(t, second)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.State.Stage)

	requireNoBlockStatus(t, buffer)
}

func TestBlockStatusBufferKeepsDistinctDoneKeys(t *testing.T) {
	buffer := NewBlockStatusBuffer(4)
	dispatcherID := common.NewDispatcherID()

	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 300, false, common.DefaultMode))
	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 300, true, common.DefaultMode))
	buffer.Offer(NewDoneBlockStatusEntry(dispatcherID, 300, false, common.RedoMode))

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

	requireNoBlockStatus(t, buffer)
}

func TestWaitingBlockStatusEntryClonesMutableMetadata(t *testing.T) {
	dispatcherID := common.NewDispatcherID()
	blockTables := &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeNormal,
		TableIDs:      []int64{11, 12},
	}
	needDroppedTables := &commonEvent.InfluencedTables{
		InfluenceType: commonEvent.InfluenceTypeDB,
		SchemaID:      99,
		TableIDs:      []int64{21, 22},
	}
	needAddedTables := []commonEvent.Table{
		{SchemaID: 31, TableID: 32, Splitable: true},
	}
	updatedSchemas := []commonEvent.SchemaIDChange{
		{TableID: 41, OldSchemaID: 42, NewSchemaID: 43},
	}

	entry := NewWaitingBlockStatusEntry(
		dispatcherID,
		500,
		blockTables,
		needDroppedTables,
		needAddedTables,
		updatedSchemas,
		false,
		common.DefaultMode,
	)

	blockTables.TableIDs[0] = 101
	needDroppedTables.TableIDs[0] = 202
	needAddedTables[0].TableID = 303
	updatedSchemas[0].NewSchemaID = 404

	msg := entry.toPB()
	require.Equal(t, []int64{11, 12}, msg.State.BlockTables.TableIDs)
	require.Equal(t, []int64{21, 22}, msg.State.NeedDroppedTables.TableIDs)
	require.Len(t, msg.State.NeedAddedTables, 1)
	require.Equal(t, int64(32), msg.State.NeedAddedTables[0].TableID)
	require.Len(t, msg.State.UpdatedSchemas, 1)
	require.Equal(t, int64(43), msg.State.UpdatedSchemas[0].NewSchemaID)
}

func requireNoBlockStatus(t *testing.T, buffer *BlockStatusBuffer) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.Nil(t, buffer.Take(ctx))
}
