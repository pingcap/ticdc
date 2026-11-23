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

package replica

import (
	"math"
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

const NodeLoadStaleThreshold = 5 * time.Second

// NodeLoadTracker shares an immutable coordinator snapshot among all maintainers
// on a capture. It contains node totals only, never dispatcher placement.
type NodeLoadTracker struct {
	mu        sync.RWMutex
	rates     map[node.ID]float64
	status    heartbeatpb.NodeLoadStatus
	updatedAt time.Time
}

func NewNodeLoadTracker() *NodeLoadTracker {
	return &NodeLoadTracker{status: heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE}
}

func (t *NodeLoadTracker) Replace(loads []*heartbeatpb.NodeLoad, status heartbeatpb.NodeLoadStatus, now time.Time) {
	if t == nil {
		return
	}
	rates := make(map[node.ID]float64, len(loads))
	if status == heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE {
		for _, load := range loads {
			if load == nil || load.NodeId == "" || load.BytesPerSecond < 0 ||
				math.IsNaN(load.BytesPerSecond) || math.IsInf(load.BytesPerSecond, 0) {
				status = heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
				break
			}
			id := node.ID(load.NodeId)
			if _, exists := rates[id]; exists {
				status = heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
				break
			}
			rates[id] = load.BytesPerSecond
		}
	} else if status != heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED {
		status = heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rates, t.status, t.updatedAt = rates, status, now
}

// Snapshot returns immutable rates. Callers must copy them before projecting
// local moves; those projections must not influence other maintainers.
func (t *NodeLoadTracker) Snapshot(nodes map[node.ID]*node.Info, now time.Time) (map[node.ID]float64, heartbeatpb.NodeLoadStatus) {
	if t == nil {
		return nil, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.status == heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED {
		return nil, t.status
	}
	if now.Sub(t.updatedAt) > NodeLoadStaleThreshold {
		return nil, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
	}
	if t.status != heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE {
		return nil, t.status
	}
	for id := range nodes {
		if _, ok := t.rates[id]; !ok {
			return nil, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
		}
	}
	return t.rates, t.status
}
