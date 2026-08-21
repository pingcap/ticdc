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

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newRemoteMessageTargetForTest() *remoteMessageTarget {
	localId := node.NewID()
	remoteId := node.NewID()
	ctx := context.Background()
	cfg := config.NewDefaultMessageCenterConfig("")
	receivedMsgCh := make(chan *TargetMessage, 1)
	rt := newRemoteMessageTarget(ctx, localId, remoteId, "", "", receivedMsgCh, receivedMsgCh, cfg, nil)
	return rt
}

func TestDecodeMessageDropsEmptyDecodedPayload(t *testing.T) {
	rt := newRemoteMessageTargetForTest()
	defer rt.close()

	received := rt.decodeMessage(&proto.Message{
		Type:    int32(TypeDispatcherHeartbeat),
		Payload: [][]byte{{0}},
	})
	require.Nil(t, received)

	heartbeat := commonEvent.NewDispatcherHeartbeat()
	payload, err := heartbeat.Marshal()
	require.NoError(t, err)
	received = rt.decodeMessage(&proto.Message{
		Type:    int32(TypeDispatcherHeartbeat),
		Payload: [][]byte{payload},
	})
	require.NotNil(t, received)
	require.Len(t, received.Message, 1)
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
