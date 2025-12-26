// Copyright 2024 PingCAP, Inc.
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

package bootstrap

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestHandleNewNodes(t *testing.T) {
	b := NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse]("test", func(id node.ID, addr string) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})

	nodes := make(map[node.ID]*node.Info)

	added, removed, requests, responses := b.HandleNodesChange(nodes)
	require.Len(t, added, 0)
	require.Len(t, removed, 0)
	require.Len(t, requests, 0)
	require.False(t, b.AllNodesReady())
	require.Nil(t, responses, 0)

	node1 := node.NewInfo("", "")
	node2 := node.NewInfo("", "")
	nodes[node1.ID] = node1
	nodes[node2.ID] = node2

	added, removed, requests, responses = b.HandleNodesChange(nodes)
	require.Len(t, added, 2)
	require.Len(t, removed, 0)
	require.Len(t, requests, 2)
	require.Len(t, b.GetAllNodeIDs(), 2)
	require.Nil(t, responses)
	require.False(t, b.AllNodesReady())

	changefeedIDPB := common.NewChangefeedID4Test("ns", "cf").ToPB()

	// not found, this should not happen in the real world,
	// since response should be sent after the bootstrapper send request.
	responses = b.HandleBootstrapResponse(
		"ef",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.False(t, b.AllNodesReady())
	require.Nil(t, responses)

	// receive one response, not bootstrapped yet
	responses = b.HandleBootstrapResponse(
		node1.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.False(t, b.AllNodesReady())
	require.Nil(t, responses)

	// all nodes responses received, bootstrapped
	responses = b.HandleBootstrapResponse(
		node2.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}, {}},
		})
	require.True(t, b.AllNodesReady())
	require.Len(t, responses, 2)
	require.Equal(t, 1, len(responses[node1.ID].Spans))
	require.Equal(t, 2, len(responses[node2.ID].Spans))

	// add one new node
	node3 := node.NewInfo("", "")
	nodes[node3.ID] = node3

	added, removed, requests, responses = b.HandleNodesChange(nodes)
	require.Len(t, added, 1)
	require.Len(t, removed, 0)
	require.Len(t, requests, 1)
	require.Nil(t, responses)
	require.False(t, b.AllNodesReady())
	responses = b.HandleBootstrapResponse(
		node3.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}, {}, {}},
		})
	require.True(t, b.AllNodesReady())
	require.Len(t, responses, 1)
	require.Equal(t, 3, len(responses[node3.ID].Spans))

	// remove a node
	delete(nodes, node1.ID)
	added, removed, requests, responses = b.HandleNodesChange(nodes)
	require.Len(t, added, 0)
	require.Len(t, removed, 1)
	require.Len(t, requests, 0)
	require.Nil(t, responses)
	require.True(t, b.AllNodesReady())

	// add a new node, and remove one node
	nodes[node1.ID] = node1
	delete(nodes, node2.ID)
	added, removed, requests, responses = b.HandleNodesChange(nodes)
	require.Len(t, added, 1)
	require.Len(t, removed, 1)
	require.Len(t, requests, 1)
	require.Nil(t, responses)
	require.False(t, b.AllNodesReady())

	responses = b.HandleBootstrapResponse(
		node1.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.True(t, b.AllNodesReady())
	require.Len(t, responses, 1)
	require.Equal(t, 1, len(responses[node1.ID].Spans))
}

func TestResendBootstrapMessage(t *testing.T) {
	b := NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse]("test", func(id node.ID, addr string) *messaging.TargetMessage {
		return &messaging.TargetMessage{
			To: id,
		}
	})
	b.resendInterval = time.Second * 2
	b.currentTime = func() time.Time { return time.Unix(0, 0) }

	nodes := make(map[node.ID]*node.Info)
	node1 := node.NewInfo("", "")
	nodes[node1.ID] = node1

	_, _, msgs, _ := b.HandleNodesChange(nodes)
	require.Len(t, msgs, 1)
	b.currentTime = func() time.Time {
		return time.Unix(1, 0)
	}

	node2 := node.NewInfo("", "")
	nodes[node2.ID] = node2

	_, _, msgs, _ = b.HandleNodesChange(nodes)
	require.Len(t, msgs, 1)
	b.currentTime = func() time.Time {
		return time.Unix(2, 0)
	}

	msgs = b.ResendBootstrapMessage()
	require.Len(t, msgs, 1)
	require.Equal(t, msgs[0].To, node1.ID)
}
