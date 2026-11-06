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
	"io"
	"net"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/peer"
)

type testGrpcStream struct {
	messages []*proto.Message
}

func (s *testGrpcStream) Send(*proto.Message) error { return nil }

func (s *testGrpcStream) Recv() (*proto.Message, error) {
	if len(s.messages) == 0 {
		return nil, io.EOF
	}
	msg := s.messages[0]
	s.messages = s.messages[1:]
	return msg, nil
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

func TestDecodeHandshakeMessageRejectsMalformedInput(t *testing.T) {
	valid := &HandshakeMessage{Version: 1, Timestamp: 2, StreamType: streamTypeCommand}
	payload, err := valid.Marshal()
	require.NoError(t, err)

	tests := []struct {
		name string
		msg  *proto.Message
	}{
		{name: "nil message"},
		{name: "wrong type", msg: &proto.Message{Type: int32(TypeBatchDMLEvent)}},
		{name: "missing payload", msg: &proto.Message{Type: int32(TypeMessageHandShake)}},
		{name: "multiple payloads", msg: &proto.Message{Type: int32(TypeMessageHandShake), Payload: [][]byte{payload, payload}}},
		{name: "invalid payload", msg: &proto.Message{Type: int32(TypeMessageHandShake), Payload: [][]byte{[]byte("{")}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeHandshakeMessage(test.msg)
			require.Error(t, err)
		})
	}

	decoded, err := decodeHandshakeMessage(&proto.Message{
		Type:    int32(TypeMessageHandShake),
		Payload: [][]byte{payload},
	})
	require.NoError(t, err)
	require.Equal(t, valid, decoded)
}

func TestValidateIncomingPeer(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 40000},
	})
	require.NoError(t, validateIncomingPeer(ctx, "192.0.2.10:8300"))
	require.Error(t, validateIncomingPeer(ctx, "192.0.2.11:8300"))
	require.Error(t, validateIncomingPeer(context.Background(), "192.0.2.10:8300"))
	require.Error(t, validateIncomingPeer(ctx, "invalid-address"))

	ipv6Ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::10"), Port: 40000, Zone: "eth0"},
	})
	require.NoError(t, validateIncomingPeer(ipv6Ctx, "[2001:db8::10]:8300"))
}

func TestHandleIncomingMessageDropsEmptyPayload(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	received := make(chan *TargetMessage, 1)
	err := rt.handleIncomingMessage(context.Background(), &testGrpcStream{
		messages: []*proto.Message{{
			From:  rt.targetId.String(),
			To:    rt.messageCenterID.String(),
			Topic: EventServiceTopic,
			Type:  int32(TypeDispatcherRequest),
		}},
	}, received)
	require.Error(t, err)
	require.Empty(t, received)
}
