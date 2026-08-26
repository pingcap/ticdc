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

package messaging

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestNodeHeartbeatResponseIOTypeRoundTrip(t *testing.T) {
	response := &heartbeatpb.NodeHeartbeatResponse{
		CoordinatorVersion: 10,
		TargetNodeEpoch:    11,
		RequestSeq:         12,
		LeaseDurationMs:    5000,
		WitnessChallenge: &heartbeatpb.WriteLeaseWitnessChallenge{
			CoordinatorVersion:   10,
			CoordinatorNodeEpoch: 11,
			SelfRequestSeq:       12,
			WitnessNodeEpoch:     13,
			Nonce:                []byte("nonce"),
		},
	}
	message := NewSingleTargetMessage(node.ID("capture"), MaintainerManagerTopic, response)
	require.Equal(t, TypeNodeHeartbeatResponse, message.Type)

	data, err := response.Marshal()
	require.NoError(t, err)
	decoded, err := decodeIOType(TypeNodeHeartbeatResponse, data)
	require.NoError(t, err)
	require.Equal(t, response, decoded)
}
