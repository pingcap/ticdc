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

package coordinator

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
)

func TestCaptureWriteLeaseGrantsRemoteNode(t *testing.T) {
	controller := newCaptureWriteLeaseController(10, node.ID("coordinator"))
	heartbeat := newWriteLeaseHeartbeat(11, 1)

	messages := controller.handleHeartbeat(node.ID("capture-1"), heartbeat, []node.ID{"capture-1"})
	require.Len(t, messages, 1)
	grant := requireWriteLeaseResponse(t, messages[0])
	require.Equal(t, int64(10), grant.CoordinatorVersion)
	require.Equal(t, uint64(11), grant.TargetNodeEpoch)
	require.Equal(t, uint64(1), grant.RequestSeq)
	require.Equal(t, uint64(writelease.P2PLeaseDuration.Milliseconds()), grant.LeaseDurationMs)

	require.Empty(t, controller.handleHeartbeat(node.ID("capture-1"), heartbeat, []node.ID{"capture-1"}))

	// A different process epoch cannot replace the epoch already associated
	// with a tracked capture ID.
	messages = controller.handleHeartbeat(node.ID("capture-1"), newWriteLeaseHeartbeat(12, 1), []node.ID{"capture-1"})
	require.Empty(t, messages)

	// Removing and re-adding the capture resets its fencing state.
	controller.removeNode(node.ID("capture-1"))
	messages = controller.handleHeartbeat(node.ID("capture-1"), newWriteLeaseHeartbeat(12, 1), []node.ID{"capture-1"})
	require.Len(t, messages, 1)
	require.Equal(t, uint64(12), requireWriteLeaseResponse(t, messages[0]).TargetNodeEpoch)
}

func TestCaptureWriteLeaseRequiresRemoteWitnessForCoordinatorNode(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newCaptureWriteLeaseController(10, node.ID("coordinator"))
	controller.now = func() time.Time { return now }
	controller.nonce = func(nonce []byte) (int, error) {
		for i := range nonce {
			nonce[i] = byte(i + 1)
		}
		return len(nonce), nil
	}

	// Observe the remote node epoch first so it can serve as a witness.
	remoteMessages := controller.handleHeartbeat(
		node.ID("capture-1"),
		newWriteLeaseHeartbeat(21, 1),
		[]node.ID{"coordinator", "capture-1"},
	)
	require.Len(t, remoteMessages, 1)

	messages := controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 1),
		[]node.ID{"coordinator", "capture-1"},
	)
	require.Len(t, messages, 1)
	require.Equal(t, node.ID("capture-1"), messages[0].To)
	challengeResponse := requireWriteLeaseResponse(t, messages[0])
	require.Zero(t, challengeResponse.RequestSeq)
	challenge := challengeResponse.GetWitnessChallenge()
	require.NotNil(t, challenge)
	require.Equal(t, uint64(11), challenge.CoordinatorNodeEpoch)
	require.Equal(t, uint64(21), challenge.WitnessNodeEpoch)

	ackHeartbeat := newWriteLeaseHeartbeat(21, 2)
	ackHeartbeat.WriteLeaseWitnessAck = &heartbeatpb.WriteLeaseWitnessAck{
		CoordinatorVersion:   challenge.CoordinatorVersion,
		CoordinatorNodeEpoch: challenge.CoordinatorNodeEpoch,
		SelfRequestSeq:       challenge.SelfRequestSeq,
		WitnessNodeEpoch:     challenge.WitnessNodeEpoch,
		Nonce:                append([]byte(nil), challenge.Nonce...),
	}
	messages = controller.handleHeartbeat(
		node.ID("capture-1"),
		ackHeartbeat,
		[]node.ID{"coordinator", "capture-1"},
	)
	require.Len(t, messages, 2)
	selfGrant := requireWriteLeaseResponse(t, messages[0])
	require.Equal(t, node.ID("coordinator"), messages[0].To)
	require.Equal(t, uint64(1), selfGrant.RequestSeq)
	require.Equal(t, uint64(11), selfGrant.TargetNodeEpoch)

	// The ack is one-shot. Replaying it only receives the witness's own grant.
	ackHeartbeat.WriteLeaseRequestSeq = 3
	messages = controller.handleHeartbeat(
		node.ID("capture-1"),
		ackHeartbeat,
		[]node.ID{"coordinator", "capture-1"},
	)
	require.Len(t, messages, 1)
	require.Equal(t, node.ID("capture-1"), messages[0].To)
}

func TestCaptureWriteLeaseSingleNodeFallback(t *testing.T) {
	controller := newCaptureWriteLeaseController(10, node.ID("coordinator"))
	messages := controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 1),
		[]node.ID{"coordinator"},
	)

	require.Len(t, messages, 1)
	require.Equal(t, node.ID("coordinator"), messages[0].To)
	require.Equal(t, uint64(1), requireWriteLeaseResponse(t, messages[0]).RequestSeq)
}

func TestCaptureWriteLeaseRetriesAnotherWitnessBeforeLeaseExpires(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newCaptureWriteLeaseController(10, node.ID("coordinator"))
	controller.now = func() time.Time { return now }
	initializedNodes := []node.ID{"coordinator", "capture-1", "capture-2"}

	controller.handleHeartbeat(node.ID("capture-1"), newWriteLeaseHeartbeat(21, 1), initializedNodes)
	controller.handleHeartbeat(node.ID("capture-2"), newWriteLeaseHeartbeat(31, 1), initializedNodes)

	messages := controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 1),
		initializedNodes,
	)
	require.Len(t, messages, 1)
	require.Equal(t, node.ID("capture-1"), messages[0].To)

	now = now.Add(writelease.NodeHeartbeatInterval)
	require.Empty(t, controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 2),
		initializedNodes,
	))

	now = now.Add(writelease.NodeHeartbeatInterval)
	messages = controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 3),
		initializedNodes,
	)
	require.Len(t, messages, 1)
	require.Equal(t, node.ID("capture-2"), messages[0].To)
	require.Less(t, witnessChallengeTimeout, writelease.P2PLeaseDuration)
}

func TestCaptureWriteLeaseRejectsInvalidHeartbeatAndLateWitness(t *testing.T) {
	now := time.Unix(100, 0)
	controller := newCaptureWriteLeaseController(10, node.ID("coordinator"))
	controller.now = func() time.Time { return now }

	legacy := newWriteLeaseHeartbeat(21, 1)
	legacy.WriteLeaseProtocolVersion = heartbeatpb.LegacyWriteLeaseProtocolVersion
	require.Empty(t, controller.handleHeartbeat(node.ID("capture-1"), legacy, []node.ID{"capture-1"}))

	stopping := newWriteLeaseHeartbeat(21, 2)
	stopping.Liveness = heartbeatpb.NodeLiveness_STOPPING
	require.Empty(t, controller.handleHeartbeat(node.ID("capture-1"), stopping, []node.ID{"capture-1"}))

	controller.handleHeartbeat(node.ID("capture-1"), newWriteLeaseHeartbeat(21, 3), []node.ID{"coordinator", "capture-1"})
	challengeMessages := controller.handleHeartbeat(
		node.ID("coordinator"),
		newWriteLeaseHeartbeat(11, 1),
		[]node.ID{"coordinator", "capture-1"},
	)
	challenge := requireWriteLeaseResponse(t, challengeMessages[0]).GetWitnessChallenge()
	now = now.Add(witnessChallengeTimeout)

	ackHeartbeat := newWriteLeaseHeartbeat(21, 4)
	ackHeartbeat.WriteLeaseWitnessAck = &heartbeatpb.WriteLeaseWitnessAck{
		CoordinatorVersion:   challenge.CoordinatorVersion,
		CoordinatorNodeEpoch: challenge.CoordinatorNodeEpoch,
		SelfRequestSeq:       challenge.SelfRequestSeq,
		WitnessNodeEpoch:     challenge.WitnessNodeEpoch,
		Nonce:                challenge.Nonce,
	}
	messages := controller.handleHeartbeat(
		node.ID("capture-1"),
		ackHeartbeat,
		[]node.ID{"coordinator", "capture-1"},
	)
	require.Len(t, messages, 1)
	require.Equal(t, node.ID("capture-1"), messages[0].To)
}

func newWriteLeaseHeartbeat(nodeEpoch, requestSeq uint64) *heartbeatpb.NodeHeartbeat {
	return &heartbeatpb.NodeHeartbeat{
		Liveness:                  heartbeatpb.NodeLiveness_ALIVE,
		NodeEpoch:                 nodeEpoch,
		WriteLeaseRequestSeq:      requestSeq,
		WriteLeaseProtocolVersion: heartbeatpb.CurrentWriteLeaseProtocolVersion,
	}
}

func requireWriteLeaseResponse(t *testing.T, message *messaging.TargetMessage) *heartbeatpb.NodeHeartbeatResponse {
	t.Helper()
	require.Equal(t, messaging.TypeNodeHeartbeatResponse, message.Type)
	return message.Message[0].(*heartbeatpb.NodeHeartbeatResponse)
}
