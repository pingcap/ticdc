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

func TestSetDispatcherDrainTargetMessageTypeMapping(t *testing.T) {
	req := &heartbeatpb.SetDispatcherDrainTargetRequest{
		TargetNodeId: "n2",
		TargetEpoch:  5,
	}

	msg := NewSingleTargetMessage(node.ID("n1"), MaintainerManagerTopic, req)
	require.Equal(t, TypeSetDispatcherDrainTargetRequest, msg.Type)
	require.Equal(t, "SetDispatcherDrainTargetRequest", msg.Type.String())

	payload, err := req.Marshal()
	require.NoError(t, err)
	decoded, err := decodeIOType(TypeSetDispatcherDrainTargetRequest, payload)
	require.NoError(t, err)

	got := decoded.(*heartbeatpb.SetDispatcherDrainTargetRequest)
	require.Equal(t, req.TargetNodeId, got.TargetNodeId)
	require.Equal(t, req.TargetEpoch, got.TargetEpoch)
}
