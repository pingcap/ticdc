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
	coscheduler "github.com/pingcap/ticdc/coordinator/scheduler"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/server/watcher"
)

var _ coscheduler.DestNodeSelector = (*destNodeSelector)(nil)

type destNodeSelector struct {
	nodeManager  *watcher.NodeManager
	livenessView *NodeLivenessView
}

func (s *destNodeSelector) GetSchedulableDestNodes() map[node.ID]*node.Info {
	nodes := s.nodeManager.GetAliveNodes()
	if len(nodes) == 0 {
		return nil
	}

	out := make(map[node.ID]*node.Info, len(nodes))
	for id, info := range nodes {
		if s.livenessView.IsSchedulableDestination(id) {
			out[id] = info
		}
	}
	return out
}

func (s *destNodeSelector) GetSchedulableDestNodeIDs() []node.ID {
	nodes := s.nodeManager.GetAliveNodes()
	if len(nodes) == 0 {
		return nil
	}

	ids := make([]node.ID, 0, len(nodes))
	for id := range nodes {
		if s.livenessView.IsSchedulableDestination(id) {
			ids = append(ids, id)
		}
	}
	return ids
}
