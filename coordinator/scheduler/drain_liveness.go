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
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/pkg/node"
)

// filterSchedulableNodeIDs removes nodes that are draining, stopping, or stale
// so drain scheduling never moves work onto a node that should no longer accept
// new maintainers.
func filterSchedulableNodeIDs(nodeIDs []node.ID, liveness *drain.Controller) []node.ID {
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

// hasDrainingOrStoppingNode reports whether drain-aware schedulers should keep
// considering node evacuation in the current tick.
func hasDrainingOrStoppingNode(liveness *drain.Controller) bool {
	return liveness != nil && len(liveness.GetDrainingOrStoppingNodes()) > 0
}

// filterSchedulableAliveNodes applies the same destination constraint to the
// alive-node map used by balance schedulers.
func filterSchedulableAliveNodes(
	nodes map[node.ID]*node.Info,
	liveness *drain.Controller,
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
