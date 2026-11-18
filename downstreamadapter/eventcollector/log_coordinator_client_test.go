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

package eventcollector

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/mock"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

func newLogCoordinatorClientForTest(t *testing.T) (*LogCoordinatorClient, *mock.MockMessageCenter, *etcd.MockOwnerCaptureInfoClient, *dispatcherStat) {
	t.Helper()
	ctrl := gomock.NewController(t)
	mc := mock.NewMockMessageCenter(ctrl)
	election := etcd.NewMockOwnerCaptureInfoClient(ctrl)
	ec := &EventCollector{
		serverId:              "local",
		mc:                    mc,
		dispatcherMessageChan: chann.NewAutoDrainChann[DispatcherMessage](chann.Cap(16)),
	}
	mc.EXPECT().RegisterHandler(logCoordinatorClientTopic, gomock.Any())
	l := newLogCoordinatorClient(ec, election)
	l.enableRemoteEventService = true
	stat := newDispatcherStat(newMockDispatcher(common.NewDispatcherID(), 100), ec, nil)
	ec.dispatcherMap.Store(stat.getDispatcherID(), stat)
	t.Cleanup(l.logCoordinatorRequestChan.CloseAndDrain)
	t.Cleanup(ec.dispatcherMessageChan.CloseAndDrain)
	return l, mc, election, stat
}

func TestLogCoordinatorClientValidatesAuthorityAndCandidates(t *testing.T) {
	l, mc, election, stat := newLogCoordinatorClientForTest(t)
	ctx := t.Context()
	req := &logservicepb.ReusableEventServiceRequest{ID: stat.getDispatcherID().ToPB(), Span: stat.target.GetTableSpan(), StartTs: 100}
	l.addReusableRequest(req)
	require.Len(t, l.pendingRequests, 1)
	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "impostor").Return(int64(0), errors.ErrNotOwner.GenWithStackByArgs())
	l.handleCoordinatorMessage(ctx, &messaging.TargetMessage{From: "impostor", Message: []messaging.IOTypeT{&common.LogCoordinatorBroadcastRequest{}}})
	require.Empty(t, l.coordinatorInfo.id)

	var sent *logservicepb.ReusableEventServiceRequest
	mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
		require.Equal(t, node.ID("owner"), msg.To)
		sent = msg.Message[0].(*logservicepb.ReusableEventServiceRequest)
		return nil
	})
	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "owner").Return(int64(10), nil)
	l.handleCoordinatorMessage(ctx, &messaging.TargetMessage{From: "owner", Message: []messaging.IOTypeT{&common.LogCoordinatorBroadcastRequest{}}})
	require.NotZero(t, sent.RequestID)
	require.Zero(t, req.RequestID, "sending must not mutate an already queued request")
	response := &logservicepb.ReusableEventServiceResponse{ID: req.ID, RequestID: sent.RequestID, Nodes: []string{"", "local", "unknown", "remote", "remote"}}

	// None of these replies is eligible for an election read or remote registration.
	l.handleReusableResponse(ctx, "impostor", response)
	legacy := *response
	legacy.RequestID = 0
	l.handleReusableResponse(ctx, "owner", &legacy)
	stale := *response
	stale.RequestID++
	l.handleReusableResponse(ctx, "owner", &stale)
	unsolicited := *response
	unsolicited.ID = common.NewDispatcherID().ToPB()
	l.handleReusableResponse(ctx, "owner", &unsolicited)
	_, _, pending := sessionState(stat.session)
	require.Empty(t, pending)

	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "owner").Return(int64(10), nil)
	mc.EXPECT().IsReadyToSend(node.ID("unknown")).Return(false)
	mc.EXPECT().IsReadyToSend(node.ID("remote")).Return(true)
	l.handleReusableResponse(ctx, "owner", response)
	require.Empty(t, l.pendingRequests)
	_, _, pending = sessionState(stat.session)
	require.Equal(t, node.ID("remote"), pending)
	require.Empty(t, stat.session.connState.remoteCandidates)
	requireDispatcherRequests(t, readDispatcherRequests(t, l.eventCollector, 1), dispatcherRequestRecord{to: "remote", action: eventpb.ActionType_ACTION_TYPE_REGISTER})
	l.handleReusableResponse(ctx, "owner", response) // Consumed replies cannot restart probing.
	requireNoDispatcherRequest(t, l.eventCollector)
}

func TestLogCoordinatorClientRejectsOldElectionReplies(t *testing.T) {
	l, mc, election, stat := newLogCoordinatorClientForTest(t)
	ctx := t.Context()
	var sent []*logservicepb.ReusableEventServiceRequest
	mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
		sent = append(sent, msg.Message[0].(*logservicepb.ReusableEventServiceRequest))
		return nil
	}).Times(3)
	broadcast := func(id node.ID, revision int64) {
		t.Helper()
		election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), id.String()).Return(revision, nil)
		l.handleCoordinatorMessage(ctx, &messaging.TargetMessage{From: id, Message: []messaging.IOTypeT{&common.LogCoordinatorBroadcastRequest{}}})
	}
	broadcast("owner", 10)
	l.addReusableRequest(&logservicepb.ReusableEventServiceRequest{ID: stat.getDispatcherID().ToPB()})
	old := &logservicepb.ReusableEventServiceResponse{ID: sent[0].ID, RequestID: sent[0].RequestID, Nodes: []string{"remote"}}
	// The same capture has been re-elected since sending this query.
	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "owner").Return(int64(11), nil)
	l.handleReusableResponse(ctx, "owner", old)
	require.Len(t, sent, 2)
	require.NotEqual(t, sent[0].RequestID, sent[1].RequestID)
	require.Equal(t, old.RequestID, sent[0].RequestID)
	l.handleReusableResponse(ctx, "owner", old)

	// Losing election authority fails closed, including during an etcd outage.
	current := &logservicepb.ReusableEventServiceResponse{ID: sent[1].ID, RequestID: sent[1].RequestID, Nodes: []string{"remote"}}
	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "owner").Return(int64(0), context.DeadlineExceeded)
	l.handleReusableResponse(ctx, "owner", current)
	require.Empty(t, l.coordinatorInfo.id)
	broadcast("new-owner", 12)
	require.Len(t, sent, 3)
	l.handleReusableResponse(ctx, "owner", current)
	_, _, pending := sessionState(stat.session)
	require.Empty(t, pending)

	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "new-owner").Return(int64(12), nil)
	mc.EXPECT().IsReadyToSend(node.ID("remote")).Return(true)
	l.handleReusableResponse(ctx, "new-owner", &logservicepb.ReusableEventServiceResponse{ID: sent[2].ID, RequestID: sent[2].RequestID, Nodes: []string{"remote"}})
	_, _, pending = sessionState(stat.session)
	require.Equal(t, node.ID("remote"), pending)
}

func TestLogCoordinatorClientRequestLifetime(t *testing.T) {
	for _, state := range []string{"legacy coordinator", "removed", "replaced", "local ready", "disabled"} {
		t.Run(state, func(t *testing.T) {
			l, mc, _, stat := newLogCoordinatorClientForTest(t)
			l.setCoordinatorInfo(logCoordinatorInfo{id: "owner", revision: 10})
			var sent *logservicepb.ReusableEventServiceRequest
			mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
				sent = msg.Message[0].(*logservicepb.ReusableEventServiceRequest)
				return nil
			})
			l.addReusableRequest(&logservicepb.ReusableEventServiceRequest{ID: stat.getDispatcherID().ToPB()})
			request := l.pendingRequests[stat.getDispatcherID()]
			switch state {
			case "legacy coordinator":
				mc.EXPECT().SendCommand(gomock.Any()).Return(nil).Times(commonMsgRetryQuota - 1)
				l.handleReusableResponse(t.Context(), "owner", &logservicepb.ReusableEventServiceResponse{ID: sent.ID, Nodes: []string{"remote"}})
				for range commonMsgRetryQuota {
					l.sendReusableRequest(request)
				}
				// Local registration is independent of legacy remote-reuse replies.
				stat.run()
				requireDispatcherRequests(t, readDispatcherRequests(t, l.eventCollector, 1), dispatcherRequestRecord{to: "local", action: eventpb.ActionType_ACTION_TYPE_REGISTER})
			case "removed":
				l.eventCollector.dispatcherMap.Delete(stat.getDispatcherID())
			case "replaced":
				l.eventCollector.dispatcherMap.Store(stat.getDispatcherID(), newDispatcherStat(stat.target, l.eventCollector, nil))
			case "local ready":
				markSessionReceiving(stat.session, "local")
			case "disabled":
				l.enableRemoteEventService = false
			}
			response := &logservicepb.ReusableEventServiceResponse{ID: sent.ID, RequestID: sent.RequestID, Nodes: []string{"remote"}}
			l.handleReusableResponse(t.Context(), "owner", response)
			if state != "disabled" {
				require.Empty(t, l.pendingRequests)
			}
			_, _, pending := sessionState(stat.session)
			require.Empty(t, pending)
		})
	}
}

func TestLogCoordinatorClientElectionReadDoesNotBlockMessageRouting(t *testing.T) {
	l, _, election, _ := newLogCoordinatorClientForTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	entered, exited := make(chan struct{}), make(chan error, 1)
	election.EXPECT().GetLogCoordinatorRevision(gomock.Any(), "owner").DoAndReturn(func(ctx context.Context, _ string) (int64, error) {
		close(entered)
		<-ctx.Done()
		return 0, context.Cause(ctx)
	})
	go func() { exited <- l.run(ctx) }()
	t.Cleanup(func() {
		cancel()
		require.ErrorIs(t, <-exited, context.Canceled)
	})
	require.NoError(t, l.MessageCenterHandler(ctx, &messaging.TargetMessage{From: "owner", Message: []messaging.IOTypeT{&common.LogCoordinatorBroadcastRequest{}}}))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("election read was not started")
	}
	// The election read stays blocked while further router callbacks complete.
	for range cap(l.coordinatorMessages) + 1 {
		require.NoError(t, l.MessageCenterHandler(ctx, &messaging.TargetMessage{From: "other"}))
	}
}
