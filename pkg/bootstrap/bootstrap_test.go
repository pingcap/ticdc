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
	node1 := node.NewInfo("", "")
	node2 := node.NewInfo("", "")
	nodes[node1.ID] = node1
	nodes[node2.ID] = node2

	added, removed, msgs, responses := b.HandleNewNodes(nodes)
	require.Len(t, added, 2)
	require.Len(t, removed, 0)
	require.Len(t, msgs, 2)
	require.Len(t, b.GetAllNodeIDs(), 2)
	require.Nil(t, responses)
	require.False(t, b.Bootstrapped())

	cfId := common.NewChangefeedID4Test("ns", "cf")
	changefeedIDPB := cfId.ToPB()

	// not found, this should not happen in the real world,
	// since response should be sent after the bootstrapper send request.
	cached := b.HandleBootstrapResponse(
		"ef",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.False(t, b.Bootstrapped())
	require.Nil(t, cached)

	// receive one response, not bootstrapped yet
	cached = b.HandleBootstrapResponse(
		node1.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.False(t, b.Bootstrapped())
	require.Nil(t, cached)

	// all nodes responses received, bootstrapped
	cached = b.HandleBootstrapResponse(
		node2.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}, {}},
		})
	require.True(t, b.Bootstrapped())
	require.Len(t, cached, 2)
	require.Equal(t, 1, len(cached[node1.ID].Spans))
	require.Equal(t, 2, len(cached[node2.ID].Spans))

	// add one new node
	node3 := node.NewInfo("", "")
	nodes[node3.ID] = node3

	added, removed, msgs, responses = b.HandleNewNodes(nodes)
	require.Len(t, added, 1)
	require.Len(t, removed, 0)
	require.Len(t, msgs, 1)
	require.Nil(t, responses)
	require.False(t, b.Bootstrapped())
	cached = b.HandleBootstrapResponse(
		node3.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}, {}, {}},
		})
	require.True(t, b.Bootstrapped())
	require.Len(t, cached, 0)

	// remove a node
	delete(nodes, node1.ID)
	added, removed, msgs, responses = b.HandleNewNodes(nodes)
	require.Len(t, added, 0)
	require.Len(t, removed, 1)
	require.Len(t, msgs, 0)
	require.Nil(t, responses)
	require.True(t, b.Bootstrapped())

	// add a new node, and remove one node
	nodes[node1.ID] = node1
	delete(nodes, node2.ID)
	added, removed, msgs, responses = b.HandleNewNodes(nodes)
	require.Len(t, added, 1)
	require.Len(t, removed, 1)
	require.Len(t, msgs, 1)
	require.Nil(t, responses)
	require.False(t, b.Bootstrapped())

	cached = b.HandleBootstrapResponse(
		node1.ID,
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: changefeedIDPB,
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.True(t, b.Bootstrapped())
	require.Len(t, cached, 0)
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
	nodes["ab"] = node.NewInfo("", "")

	_, _, msgs, _ := b.HandleNewNodes(nodes)
	require.Len(t, msgs, 1)
	b.currentTime = func() time.Time {
		return time.Unix(1, 0)
	}

	nodes["bc"] = node.NewInfo("", "")
	_, _, msgs, _ = b.HandleNewNodes(nodes)
	require.Len(t, msgs, 1)
	b.currentTime = func() time.Time {
		return time.Unix(2, 0)
	}
	msgs = b.ResendBootstrapMessage()
	require.Len(t, msgs, 1)
	require.Equal(t, msgs[0].To, node.ID("ab"))
}

func TestCheckAllNodeInitialized(t *testing.T) {
	b := NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse]("test", func(id node.ID, addr string) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})

	nodes := make(map[node.ID]*node.Info)
	nodes["ab"] = node.NewInfo("", "")

	_, _, msgs, _ := b.HandleNewNodes(nodes)
	require.Len(t, msgs, 1)
	require.False(t, b.Bootstrapped())
	cfId := common.NewChangefeedID4Test("ns", "cf")
	b.HandleBootstrapResponse(
		"ab",
		&heartbeatpb.MaintainerBootstrapResponse{
			ChangefeedID: cfId.ToPB(),
			Spans:        []*heartbeatpb.BootstrapTableSpan{{}},
		})
	require.True(t, b.Bootstrapped())
}

func TestGetAllNodes(t *testing.T) {
	b := NewBootstrapper[heartbeatpb.MaintainerBootstrapResponse]("test", func(id node.ID, addr string) *messaging.TargetMessage {
		return &messaging.TargetMessage{}
	})

	nodes := make(map[node.ID]*node.Info)
	nodes["ab"] = node.NewInfo("", "")
	nodes["cd"] = node.NewInfo("", "")

	b.HandleNewNodes(nodes)
	all := b.GetAllNodeIDs()
	require.Equal(t, 2, len(all))
}
