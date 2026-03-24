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

package messaging

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockRecvStream struct {
	message *proto.Message
}

func (s *mockRecvStream) Send(*proto.Message) error {
	return nil
}

func (s *mockRecvStream) Recv() (*proto.Message, error) {
	return s.message, nil
}

func newRemoteMessageTargetForTest() *remoteMessageTarget {
	localId := node.NewID()
	remoteId := node.NewID()
	ctx := context.Background()
	cfg := config.NewDefaultMessageCenterConfig("")
	receivedMsgCh := make(chan *TargetMessage, 1)
	rt := newRemoteMessageTarget(ctx, localId, remoteId, "", "", receivedMsgCh, receivedMsgCh, cfg, nil)
	return rt
}

func TestRemoteTargetNewMessage(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	msg := &TargetMessage{
		Type: TypeMessageHandShake,
	}
	msg1 := rt.newMessage(msg)
	require.Equal(t, TypeMessageHandShake, IOType(msg1.Type))

	msg2 := rt.newMessage(msg)
	log.Info("msg2", zap.Any("msg2", msg2))
	require.Equal(t, TypeMessageHandShake, IOType(msg2.Type))
}

func TestRemoteTargetReadinessByStream(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	session := &streamSession{cancel: func() {}}
	rt.streams.Store(EventStreamType, session)
	require.True(t, rt.isReadyToSendByStream(EventStreamType))
	require.False(t, rt.isReadyToSendByStream(CommandStreamType))

	msg := &TargetMessage{
		Topic:   "test-topic",
		Type:    TypeMessageHandShake,
		Message: newMockIOTypeT([]byte("test")),
	}

	require.NoError(t, rt.sendEvent(msg))
	select {
	case protoMsg := <-rt.sendEventCh:
		require.Equal(t, int32(TypeMessageHandShake), protoMsg.Type)
	case <-time.After(time.Second):
		t.Fatal("event message not sent")
	}

	err := rt.sendCommand(msg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Stream not ready")

	rt.streams.Store(CommandStreamType, session)
	require.True(t, rt.isReadyToSendByStream(CommandStreamType))
	require.NoError(t, rt.sendCommand(msg))
}

func TestRemoteTargetHandleIncomingMessageExitOnContextCancel(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	recvCh := make(chan *TargetMessage, 1)
	recvCh <- &TargetMessage{}

	ctx, cancel := context.WithCancel(context.Background())
	stream := &mockRecvStream{
		message: &proto.Message{
			From: string(node.NewID()),
			To:   string(node.NewID()),
			Type: int32(TypeMessageHandShake),
		},
	}

	done := make(chan error, 1)
	go func() {
		done <- rt.handleIncomingMessage(
			ctx,
			stream,
			recvCh,
			metrics.MessagingReceiveMsgCounter.WithLabelValues("event"),
			EventStreamType,
		)
	}()

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("handleIncomingMessage does not exit after context cancel")
	}
}

func TestRemoteTargetStreamGauge(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	session := &streamSession{cancel: func() {}}

	require.Equal(t, float64(0), testutil.ToFloat64(metrics.MessagingStreamGauge.WithLabelValues(rt.targetId.String())))

	rt.streams.Store(EventStreamType, session)
	rt.updateStreamGauge()
	require.Equal(t, float64(1), testutil.ToFloat64(metrics.MessagingStreamGauge.WithLabelValues(rt.targetId.String())))

	rt.streams.Store(CommandStreamType, session)
	rt.updateStreamGauge()
	require.Equal(t, float64(2), testutil.ToFloat64(metrics.MessagingStreamGauge.WithLabelValues(rt.targetId.String())))

	rt.closeConn()
	require.Equal(t, float64(0), testutil.ToFloat64(metrics.MessagingStreamGauge.WithLabelValues(rt.targetId.String())))
}
