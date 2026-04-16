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

package maintainer

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestBlockStatusMailboxDrainBlockedThenDoneInOrder(t *testing.T) {
	mailbox := newBlockStatusMailbox(8, 8)
	from := node.NewID()
	dispatcherID := common.NewDispatcherID()

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxBlockedStatus(dispatcherID, 100, heartbeatpb.BlockStage_WAITING),
			newMailboxBlockedStatus(dispatcherID, 100, heartbeatpb.BlockStage_WAITING),
			newMailboxBlockedStatus(dispatcherID, 100, heartbeatpb.BlockStage_DONE),
		},
	})

	batch, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, batch.statuses, 2)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, batch.statuses[0].State.Stage)
	require.Equal(t, heartbeatpb.BlockStage_DONE, batch.statuses[1].State.Stage)
	require.Equal(t, 0, mailbox.Len())
}

func TestBlockStatusMailboxAcceptsRepeatedBlockedAfterDrain(t *testing.T) {
	mailbox := newBlockStatusMailbox(8, 8)
	from := node.NewID()
	dispatcherID := common.NewDispatcherID()

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxBlockedStatus(dispatcherID, 101, heartbeatpb.BlockStage_WAITING),
		},
	})

	batch, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, batch.statuses, 1)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, batch.statuses[0].State.Stage)
	require.Equal(t, 1, mailbox.Len())

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxBlockedStatus(dispatcherID, 101, heartbeatpb.BlockStage_WAITING),
		},
	})

	batch, ok = mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, batch.statuses, 1)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, batch.statuses[0].State.Stage)
	require.Equal(t, 1, mailbox.Len())

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxBlockedStatus(dispatcherID, 101, heartbeatpb.BlockStage_DONE),
		},
	})

	batch, ok = mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, batch.statuses, 1)
	require.Equal(t, heartbeatpb.BlockStage_DONE, batch.statuses[0].State.Stage)
	require.Equal(t, 0, mailbox.Len())
}

func TestBlockStatusMailboxRespectsDrainBatchLimit(t *testing.T) {
	mailbox := newBlockStatusMailbox(8, 1)
	from := node.NewID()
	dispatcherID := common.NewDispatcherID()

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxBlockedStatus(dispatcherID, 102, heartbeatpb.BlockStage_WAITING),
			newMailboxBlockedStatus(dispatcherID, 102, heartbeatpb.BlockStage_DONE),
		},
	})

	first, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, first.statuses, 1)
	require.Equal(t, heartbeatpb.BlockStage_WAITING, first.statuses[0].State.Stage)
	require.Equal(t, 1, mailbox.Len())

	second, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, second.statuses, 1)
	require.Equal(t, heartbeatpb.BlockStage_DONE, second.statuses[0].State.Stage)
	require.Equal(t, 0, mailbox.Len())
}

func TestBlockStatusMailboxSignalsRemainingReadySource(t *testing.T) {
	mailbox := newBlockStatusMailbox(8, 1)
	firstFrom := node.NewID()
	secondFrom := node.NewID()

	mailbox.enqueueRequest(firstFrom, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxNonBlockedStatus(common.NewDispatcherID(), 200),
		},
	})
	mailbox.enqueueRequest(secondFrom, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxNonBlockedStatus(common.NewDispatcherID(), 201),
		},
	})

	select {
	case <-mailbox.Notify():
	case <-time.After(time.Second):
		t.Fatal("expected initial mailbox notification")
	}

	firstBatch, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, firstBatch.statuses, 1)

	select {
	case <-mailbox.Notify():
	case <-time.After(time.Second):
		t.Fatal("expected mailbox notification for remaining ready source")
	}

	secondBatch, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, secondBatch.statuses, 1)
	require.NotEqual(t, firstBatch.from, secondBatch.from)
	require.Equal(t, 0, mailbox.Len())
}

func TestBlockStatusMailboxDropsNewUniqueWhenFull(t *testing.T) {
	mailbox := newBlockStatusMailbox(1, 8)
	from := node.NewID()
	firstDispatcherID := common.NewDispatcherID()
	secondDispatcherID := common.NewDispatcherID()

	mailbox.enqueueRequest(from, &heartbeatpb.BlockStatusRequest{
		Mode: common.DefaultMode,
		BlockStatuses: []*heartbeatpb.TableSpanBlockStatus{
			newMailboxNonBlockedStatus(firstDispatcherID, 103),
			newMailboxNonBlockedStatus(firstDispatcherID, 103),
			newMailboxNonBlockedStatus(secondDispatcherID, 104),
		},
	})

	batch, ok := mailbox.drainBatch()
	require.True(t, ok)
	require.Len(t, batch.statuses, 1)
	require.True(t, common.NewDispatcherIDFromPB(batch.statuses[0].ID).Equal(firstDispatcherID))
	require.Equal(t, 0, mailbox.Len())
}

func newMailboxBlockedStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
	stage heartbeatpb.BlockStage,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked: true,
			BlockTs:   blockTs,
			Stage:     stage,
		},
		Mode: common.DefaultMode,
	}
}

func newMailboxNonBlockedStatus(
	dispatcherID common.DispatcherID,
	blockTs uint64,
) *heartbeatpb.TableSpanBlockStatus {
	return &heartbeatpb.TableSpanBlockStatus{
		ID: dispatcherID.ToPB(),
		State: &heartbeatpb.State{
			IsBlocked: false,
			BlockTs:   blockTs,
			Stage:     heartbeatpb.BlockStage_NONE,
		},
		Mode: common.DefaultMode,
	}
}
