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

package scheduler

import (
	"time"

	"github.com/pingcap/ticdc/pkg/node"
)

const balanceDrainCooldown = 120 * time.Second

type drainTargetGetter func() (node.ID, uint64)

func snapshotDrainTarget(getTarget drainTargetGetter) (node.ID, uint64, bool) {
	if getTarget == nil {
		return "", 0, false
	}
	target, epoch := getTarget()
	if target.IsEmpty() || epoch == 0 {
		return "", 0, false
	}
	return target, epoch, true
}

func shouldPauseBalanceForDrain(getTarget drainTargetGetter, now time.Time, blockedUntil *time.Time) bool {
	if _, _, drainActive := snapshotDrainTarget(getTarget); drainActive {
		*blockedUntil = now.Add(balanceDrainCooldown)
		return true
	}
	return now.Before(*blockedUntil)
}

func filterNodeIDsByDrainTarget(nodeIDs []node.ID, getTarget drainTargetGetter) []node.ID {
	target, _, active := snapshotDrainTarget(getTarget)
	if !active {
		return nodeIDs
	}
	filtered := nodeIDs[:0]
	for _, id := range nodeIDs {
		if id == target {
			continue
		}
		filtered = append(filtered, id)
	}
	return filtered
}

func filterAliveNodesByDrainTarget(
	nodes map[node.ID]*node.Info,
	getTarget drainTargetGetter,
) map[node.ID]*node.Info {
	target, _, active := snapshotDrainTarget(getTarget)
	if !active {
		return nodes
	}
	filtered := make(map[node.ID]*node.Info, len(nodes))
	for id, info := range nodes {
		if id == target {
			continue
		}
		filtered[id] = info
	}
	return filtered
}
