// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package scheduler

import (
	"github.com/pingcap/ticdc/pkg/node"
)

type livenessReader interface {
	IsSchedulableDest(node.ID) bool
	GetDrainingOrStoppingNodes() []node.ID
}

func filterSchedulableNodeIDs(nodeIDs []node.ID, liveness livenessReader) []node.ID {
	if liveness == nil {
		return nodeIDs
	}
	filtered := nodeIDs[:0]
	for _, nodeID := range nodeIDs {
		if liveness.IsSchedulableDest(nodeID) {
			filtered = append(filtered, nodeID)
		}
	}
	return filtered
}

func hasDrainingOrStoppingNode(liveness livenessReader) bool {
	return liveness != nil && len(liveness.GetDrainingOrStoppingNodes()) > 0
}

func filterSchedulableAliveNodes(
	nodes map[node.ID]*node.Info,
	liveness livenessReader,
) map[node.ID]*node.Info {
	if liveness == nil {
		return nodes
	}
	filtered := make(map[node.ID]*node.Info, len(nodes))
	for id, info := range nodes {
		if liveness.IsSchedulableDest(id) {
			filtered[id] = info
		}
	}
	return filtered
}
