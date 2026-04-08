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
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/node"
)

// balanceDrainCooldown keeps regular balance schedulers paused for a short
// window after drain target clearing so dispatchers are not moved back
// immediately and cause scheduling churn.
const balanceDrainCooldown = 120 * time.Second

// DrainState stores the drain-related scheduler state shared by one
// changefeed. All schedulers read it through snapshots so a single tick sees a
// consistent view of the maintainer host and the active drain target.
type DrainState struct {
	mu sync.RWMutex

	selfNodeID   node.ID
	targetNodeID node.ID
	targetEpoch  uint64
}

type drainStateSnapshot struct {
	selfNodeID   node.ID
	targetNodeID node.ID
	targetEpoch  uint64
}

// NewDrainState creates an empty drain state with no active drain target.
func NewDrainState() *DrainState {
	return &DrainState{}
}

// SetSelfNodeID records the node currently hosting the changefeed maintainer.
func (s *DrainState) SetSelfNodeID(id node.ID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.selfNodeID = id
}

// SetDispatcherDrainTarget applies the newest drain target visible to the
// changefeed. Older epochs are ignored so scheduler state does not regress, and
// same-epoch updates follow the manager-level monotonic rule: only clearing a
// non-empty target is allowed.
func (s *DrainState) SetDispatcherDrainTarget(target node.ID, epoch uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if epoch < s.targetEpoch {
		return
	}
	if epoch == s.targetEpoch {
		if target == s.targetNodeID {
			return
		}
		if target.IsEmpty() && !s.targetNodeID.IsEmpty() {
			s.targetNodeID = target
		}
		return
	}
	s.targetNodeID = target
	s.targetEpoch = epoch
}

// DispatcherDrainTarget returns the current drain target and epoch snapshot
// used by maintainer status reporting.
func (s *DrainState) DispatcherDrainTarget() (node.ID, uint64) {
	if s == nil {
		return "", 0
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.targetNodeID, s.targetEpoch
}

// snapshot returns a consistent view of all drain-related scheduler state.
func (s *DrainState) snapshot() drainStateSnapshot {
	if s == nil {
		return drainStateSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return drainStateSnapshot{
		selfNodeID:   s.selfNodeID,
		targetNodeID: s.targetNodeID,
		targetEpoch:  s.targetEpoch,
	}
}

// activeDrainTarget returns the current active drain target from a scheduler
// snapshot. Empty targets and zero epochs mean drain is not active.
func activeDrainTarget(state drainStateSnapshot) (node.ID, uint64, bool) {
	if state.targetNodeID.IsEmpty() || state.targetEpoch == 0 {
		return "", 0, false
	}
	return state.targetNodeID, state.targetEpoch, true
}

// shouldPauseBalanceForDrain reports whether balance scheduling should stay
// paused. Active drain pauses balance immediately, and clearing the target keeps
// balance paused until the cooldown expires.
func shouldPauseBalanceForDrain(
	state drainStateSnapshot,
	now time.Time,
	blockedUntil *time.Time,
) bool {
	if _, _, drainActive := activeDrainTarget(state); drainActive {
		*blockedUntil = now.Add(balanceDrainCooldown)
		return true
	}
	return now.Before(*blockedUntil)
}

// filterNodeIDsByDrainTarget removes the active drain target from a slice of
// candidate node IDs. It reuses the input slice storage to avoid an extra
// allocation on scheduler hot paths.
func filterNodeIDsByDrainTarget(
	nodeIDs []node.ID,
	state drainStateSnapshot,
) []node.ID {
	target, _, active := activeDrainTarget(state)
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

// filterAliveNodesByDrainTarget removes the active drain target from the alive
// node map used by balance schedulers, ensuring no new movement targets the
// node being evacuated.
func filterAliveNodesByDrainTarget(
	nodes map[node.ID]*node.Info,
	state drainStateSnapshot,
) map[node.ID]*node.Info {
	target, _, active := activeDrainTarget(state)
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
