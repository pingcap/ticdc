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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
)

func newOwnerElectionClientForTest(t *testing.T, owner node.ID, revision int64) *etcd.MockOwnerCaptureInfoClient {
	t.Helper()
	client := etcd.NewMockOwnerCaptureInfoClient(gomock.NewController(t))
	client.EXPECT().GetOwnerRevision(gomock.Any(), owner.String()).Return(revision, nil).AnyTimes()
	return client
}

func newCoordinatorAuthManagerForTest(t *testing.T) (*Manager, *etcd.MockOwnerCaptureInfoClient, chan *messaging.TargetMessage) {
	t.Helper()
	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.CaptureWriteGate, writelease.NewGate())
	election := etcd.NewMockOwnerCaptureInfoClient(gomock.NewController(t))
	m := NewMaintainerManager(&node.Info{ID: "capture"}, config.NewDefaultSchedulerConfig(), new(liveness.Liveness), election)
	t.Cleanup(m.maintainers.taskScheduler.Stop)
	t.Cleanup(m.maintainers.closeAll)
	return m, election, mc.GetMessageChannel()
}

func coordinatorBootstrapForTest(from node.ID, revision int64, protocol uint32) *messaging.TargetMessage {
	message := messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic, &heartbeatpb.CoordinatorBootstrapRequest{
		Version: revision, WriteLeaseProtocolVersion: protocol,
	})
	message.From = from
	return message
}

func bootstrapCoordinatorForTest(t *testing.T, m *Manager, election *etcd.MockOwnerCaptureInfoClient,
	messages chan *messaging.TargetMessage, from node.ID, revision int64, protocol uint32,
) *heartbeatpb.NodeHeartbeat {
	t.Helper()
	election.EXPECT().GetOwnerRevision(gomock.Any(), from.String()).Return(revision, nil)
	m.handleMessage(t.Context(), coordinatorBootstrapForTest(from, revision, protocol))
	require.Len(t, messages, 2)
	response := <-messages
	require.Equal(t, from, response.To)
	require.Equal(t, messaging.TypeCoordinatorBootstrapResponse, response.Type)
	heartbeat := <-messages
	require.Equal(t, from, heartbeat.To)
	return heartbeat.Message[0].(*heartbeatpb.NodeHeartbeat)
}

func TestCoordinatorBootstrapRejectsUnelectedSender(t *testing.T) {
	m, election, messages := newCoordinatorAuthManagerForTest(t)
	election.EXPECT().GetOwnerRevision(gomock.Any(), "impostor").Return(int64(0), errors.ErrNotOwner.GenWithStackByArgs())
	m.coordinatorID = "owner"
	m.coordinatorVersion = 10
	m.writeGate.SetP2PRequired(true)
	require.True(t, m.writeGate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))
	m.onCoordinatorBootstrapRequest(t.Context(), coordinatorBootstrapForTest("impostor", math.MaxInt64, heartbeatpb.LegacyWriteLeaseProtocolVersion))
	require.Equal(t, node.ID("owner"), m.coordinatorID)
	require.Equal(t, int64(10), m.coordinatorVersion)
	require.True(t, m.writeGate.Status().P2PRequired)
	require.False(t, m.writeGate.IsWritable())
	require.Empty(t, messages)
	cfID := common.NewChangefeedID4Test("default", "forged")
	for _, payload := range []messaging.IOTypeT{
		newAddMaintainerRequestForEpoch(t, cfID, 1, 1),
		&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ToPB(), Cascade: true},
	} {
		command := messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic, payload)
		command.From = "impostor"
		m.handleMessage(t.Context(), command)
	}
	require.Empty(t, m.ListMaintainers())
	require.Empty(t, messages)
}

func TestCoordinatorBootstrapRejectsInvalidClaimsWithoutStateChanges(t *testing.T) {
	for _, name := range []string{"forged revision", "stale revision", "election unavailable", "owner absent", "unknown protocol", "missing validator", "empty sender", "nil payload"} {
		t.Run(name, func(t *testing.T) {
			m, election, messages := newCoordinatorAuthManagerForTest(t)
			m.coordinatorID, m.coordinatorVersion = "owner", 10
			m.writeGate.SetP2PRequired(true)
			now := time.Now()
			require.True(t, m.writeGate.RenewEtcd(now, writelease.EtcdProofDuration))
			require.True(t, m.writeGate.RenewP2P(now, writelease.P2PLeaseDuration))
			m.node.writeLeaseRequestSeq = 4
			m.node.lastAppliedLeaseSeq = 3
			m.node.writeLeaseRequestSentAt[4] = now
			ack := &heartbeatpb.WriteLeaseWitnessAck{CoordinatorVersion: 10}
			m.node.pendingWitnessAck = ack
			message := coordinatorBootstrapForTest("owner", 10, heartbeatpb.LegacyWriteLeaseProtocolVersion)
			req := message.Message[0].(*heartbeatpb.CoordinatorBootstrapRequest)
			switch name {
			case "forged revision", "stale revision":
				req.Version = math.MaxInt64
				if name == "stale revision" {
					req.Version = 9
				}
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(10), nil)
			case "election unavailable":
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), context.DeadlineExceeded)
			case "owner absent":
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), errors.ErrOwnerNotFound.GenWithStackByArgs())
			case "unknown protocol":
				req.WriteLeaseProtocolVersion = heartbeatpb.CurrentWriteLeaseProtocolVersion + 1
			case "missing validator":
				m.electionClient = nil
			case "empty sender":
				message.From = ""
			case "nil payload":
				message.Message[0] = (*heartbeatpb.CoordinatorBootstrapRequest)(nil)
			}
			m.handleMessage(t.Context(), message)
			require.Equal(t, node.ID("owner"), m.coordinatorID)
			require.Equal(t, int64(10), m.coordinatorVersion)
			require.True(t, m.writeGate.Status().P2PRequired)
			require.True(t, m.writeGate.IsWritable())
			require.Equal(t, uint64(4), m.node.writeLeaseRequestSeq)
			require.Equal(t, uint64(3), m.node.lastAppliedLeaseSeq)
			require.Equal(t, map[uint64]time.Time{4: now}, m.node.writeLeaseRequestSentAt)
			require.Same(t, ack, m.node.pendingWitnessAck)
			require.Empty(t, messages)
		})
	}
}

func TestCoordinatorBootstrapRecoversAndChangesOwner(t *testing.T) {
	m, election, messages := newCoordinatorAuthManagerForTest(t)
	m.coordinatorID, m.coordinatorVersion = "impostor", math.MaxInt64
	m.node.lastAppliedLeaseSeq = 100
	m.node.writeLeaseRequestSentAt[100] = time.Now()
	require.True(t, m.writeGate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))
	hb := bootstrapCoordinatorForTest(t, m, election, messages, "owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)
	require.Equal(t, node.ID("owner"), m.coordinatorID)
	require.Equal(t, int64(10), m.coordinatorVersion)
	require.Zero(t, m.node.lastAppliedLeaseSeq)
	require.NotContains(t, m.node.writeLeaseRequestSentAt, uint64(100))
	require.True(t, m.writeGate.Status().P2PRequired)
	require.False(t, m.writeGate.IsWritable())
	grant := messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic, &heartbeatpb.NodeHeartbeatResponse{
		CoordinatorVersion: 10, TargetNodeEpoch: m.node.nodeEpoch, RequestSeq: hb.WriteLeaseRequestSeq,
		LeaseDurationMs: uint64(writelease.P2PLeaseDuration.Milliseconds()),
	})
	grant.From = "owner"
	m.handleMessage(t.Context(), grant)
	require.True(t, m.writeGate.IsWritable())
	// An identical, verified bootstrap preserves this generation's proof and sequence history.
	bootstrapCoordinatorForTest(t, m, election, messages, "owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)
	require.True(t, m.writeGate.IsWritable())
	require.Equal(t, hb.WriteLeaseRequestSeq, m.node.lastAppliedLeaseSeq)

	bootstrapCoordinatorForTest(t, m, election, messages, "new-owner", 11, heartbeatpb.LegacyWriteLeaseProtocolVersion)
	require.False(t, m.writeGate.Status().P2PRequired)
	require.True(t, m.writeGate.IsWritable())
	require.Zero(t, m.node.lastAppliedLeaseSeq)
	require.Zero(t, m.writeGate.Status().P2PRemaining)
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), errors.ErrNotOwner.GenWithStackByArgs())
	m.handleMessage(t.Context(), coordinatorBootstrapForTest("owner", 10, heartbeatpb.LegacyWriteLeaseProtocolVersion))
	require.Equal(t, node.ID("new-owner"), m.coordinatorID)
	require.Equal(t, int64(11), m.coordinatorVersion)
	require.Empty(t, messages)

	// Re-election of the same capture is a new generation too.
	bootstrapCoordinatorForTest(t, m, election, messages, "new-owner", 12, heartbeatpb.CurrentWriteLeaseProtocolVersion)
	require.Equal(t, int64(12), m.coordinatorVersion)
	require.True(t, m.writeGate.Status().P2PRequired)
	require.False(t, m.writeGate.IsWritable())
	m.writeGate.Fence()
	bootstrapCoordinatorForTest(t, m, election, messages, "new-owner", 12, heartbeatpb.LegacyWriteLeaseProtocolVersion)
	require.False(t, m.writeGate.IsWritable(), "protocol negotiation cannot undo fencing")
}

func TestOwnerChangeRejectsOldCoordinatorCommands(t *testing.T) {
	cfID := common.NewChangefeedID4Test("default", "untrusted")
	for _, payload := range []messaging.IOTypeT{
		newAddMaintainerRequestForEpoch(t, cfID, 1, 1),
		&heartbeatpb.RemoveMaintainerRequest{Id: cfID.ToPB(), Cascade: true},
		&heartbeatpb.SetNodeLivenessRequest{Target: heartbeatpb.NodeLiveness_STOPPING},
		&heartbeatpb.SetDispatcherDrainTargetRequest{TargetNodeId: "other", TargetEpoch: 1},
		&heartbeatpb.NodeHeartbeatResponse{CoordinatorVersion: 10, RequestSeq: 1},
	} {
		command := messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic, payload)
		t.Run(command.Type.String(), func(t *testing.T) {
			m, election, messages := newCoordinatorAuthManagerForTest(t)
			bootstrapCoordinatorForTest(t, m, election, messages, "old-owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)
			require.True(t, m.writeGate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))
			require.True(t, m.writeGate.RenewP2P(time.Now(), writelease.P2PLeaseDuration))
			if request, ok := payload.(*heartbeatpb.SetNodeLivenessRequest); ok {
				request.NodeEpoch = m.node.nodeEpoch
			}
			if response, ok := payload.(*heartbeatpb.NodeHeartbeatResponse); ok {
				response.TargetNodeEpoch = m.node.nodeEpoch
			}
			command.From = "old-owner"
			m.OnCoordinatorChange("new-owner")
			election.EXPECT().GetOwnerRevision(gomock.Any(), "old-owner").Return(int64(0), errors.ErrNotOwner.GenWithStackByArgs())
			m.handleMessage(t.Context(), command)
			require.False(t, m.isBootstrap())
			require.Empty(t, m.coordinatorID)
			require.Empty(t, m.ListMaintainers())
			require.Equal(t, liveness.CaptureAlive, m.node.liveness.Load())
			target, epoch := m.getDispatcherDrainTarget()
			require.Empty(t, target)
			require.Zero(t, epoch)
			require.Empty(t, m.node.writeLeaseRequestSentAt)
			require.True(t, m.writeGate.Status().P2PRequired)
			require.False(t, m.writeGate.IsWritable())
			require.Empty(t, messages)
			// Subsequent commands stay rejected until the next verified bootstrap.
			m.handleMessage(t.Context(), command)
			require.Empty(t, messages)
		})
	}
}

func TestOwnerRevalidationHandlesDelayedNotificationsAndOutages(t *testing.T) {
	m, election, messages := newCoordinatorAuthManagerForTest(t)
	bootstrapCoordinatorForTest(t, m, election, messages, "owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)
	m.OnCoordinatorChange("outdated-watcher-owner")
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(10), nil)
	require.True(t, m.validateCoordinator(t.Context()))
	require.Equal(t, int64(10), m.coordinatorVersion)

	command := messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic,
		&heartbeatpb.SetDispatcherDrainTargetRequest{TargetNodeId: "other", TargetEpoch: 1})
	command.From = "owner"
	m.OnCoordinatorChange("other-owner")
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), context.DeadlineExceeded)
	m.handleMessage(t.Context(), command)
	require.Equal(t, node.ID("owner"), m.coordinatorID)
	require.True(t, m.coordinatorNeedsValidation)
	target, _ := m.getDispatcherDrainTarget()
	require.Empty(t, target)
	require.Empty(t, messages)
	// Retry after etcd recovers can accept a still-valid owner command.
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(10), nil)
	m.handleMessage(t.Context(), command)
	target, epoch := m.getDispatcherDrainTarget()
	require.Equal(t, node.ID("other"), target)
	require.Equal(t, uint64(1), epoch)
	require.False(t, m.coordinatorNeedsValidation)
	require.Len(t, messages, 1)
	<-messages
	// A changed revision invalidates the previous generation even for the same ID.
	m.OnCoordinatorChange("owner")
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(11), nil)
	require.False(t, m.validateCoordinator(t.Context()))
	require.False(t, m.isBootstrap())
	bootstrapCoordinatorForTest(t, m, election, messages, "owner", 11, heartbeatpb.CurrentWriteLeaseProtocolVersion)
	require.True(t, m.isBootstrap())
}

func TestCoordinatorElectionReadRespectsManagerCancellation(t *testing.T) {
	m, election, messages := newCoordinatorAuthManagerForTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, exited := make(chan struct{}), make(chan error, 1)
	election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").DoAndReturn(func(queryCtx context.Context, _ string) (int64, error) {
		_, hasDeadline := queryCtx.Deadline()
		require.True(t, hasDeadline)
		close(entered)
		<-queryCtx.Done()
		return 0, context.Cause(queryCtx)
	})
	go func() { exited <- m.Run(ctx) }()
	require.NoError(t, m.recvMessages(ctx, coordinatorBootstrapForTest("owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("election read did not start")
	}
	// Watcher notifications and router callbacks stay independent of the read.
	for range 10 {
		m.OnCoordinatorChange("owner")
	}
	require.NoError(t, m.recvMessages(ctx, messaging.NewSingleTargetMessage("capture", messaging.MaintainerManagerTopic,
		&heartbeatpb.SetDispatcherDrainTargetRequest{TargetEpoch: 1})))
	cancel()
	select {
	case err := <-exited:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after cancellation")
	}
	require.False(t, m.isBootstrap())
	require.Empty(t, messages)
}

func TestOwnerChangeRevokesIdleManager(t *testing.T) {
	for _, name := range []string{"different owner", "no owner", "new revision"} {
		t.Run(name, func(t *testing.T) {
			m, election, messages := newCoordinatorAuthManagerForTest(t)
			bootstrapCoordinatorForTest(t, m, election, messages, "owner", 10, heartbeatpb.CurrentWriteLeaseProtocolVersion)
			require.True(t, m.writeGate.RenewP2P(time.Now(), writelease.P2PLeaseDuration))
			switch name {
			case "different owner":
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), errors.ErrNotOwner.GenWithStackByArgs())
			case "no owner":
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(0), errors.ErrOwnerNotFound.GenWithStackByArgs())
			case "new revision":
				election.EXPECT().GetOwnerRevision(gomock.Any(), "owner").Return(int64(11), nil)
			}
			ctx, cancel := context.WithCancel(t.Context())
			exited := make(chan error, 1)
			go func() { exited <- m.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				require.ErrorIs(t, <-exited, context.Canceled)
				require.False(t, m.isBootstrap())
				require.Empty(t, m.node.writeLeaseRequestSentAt)
			})
			m.OnCoordinatorChange("owner")
			require.Eventually(t, func() bool {
				return m.writeGate.Status().P2PRemaining == 0
			}, time.Second, time.Millisecond)
		})
	}
}
