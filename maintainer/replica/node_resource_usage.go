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
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
)

// Resource usage is normally reported by the node heartbeat every 500ms.
// Allow several missed reports before treating current-version telemetry as
// incomplete and suppressing traffic-driven moves.
const nodeResourceUsageStaleThreshold = 5 * time.Second

// NodeResourceUsageTracker stores one immutable rate snapshot shared by all
// local changefeed group checkers.
type NodeResourceUsageTracker struct {
	mu                            sync.RWMutex
	eventStoreWriteBytesPerSecond map[node.ID]uint64
	status                        heartbeatpb.NodeResourceUsageStatus
	updatedAt                     time.Time
	now                           func() time.Time
}

func NewNodeResourceUsageTracker() *NodeResourceUsageTracker {
	return &NodeResourceUsageTracker{
		status: heartbeatpb.NodeResourceUsageStatus_INCOMPLETE,
		now:    time.Now,
	}
}

// ReplaceEventStoreWriteBytesPerSecond atomically replaces the cluster-wide
// rate snapshot computed by the coordinator.
func (t *NodeResourceUsageTracker) ReplaceEventStoreWriteBytesPerSecond(
	usages []*heartbeatpb.NodeResourceUsage,
	status heartbeatpb.NodeResourceUsageStatus,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	if status != heartbeatpb.NodeResourceUsageStatus_AVAILABLE {
		if status != heartbeatpb.NodeResourceUsageStatus_UNSUPPORTED {
			status = heartbeatpb.NodeResourceUsageStatus_INCOMPLETE
		}
		t.eventStoreWriteBytesPerSecond = nil
		t.status = status
		t.updatedAt = now
		return
	}

	rates := make(map[node.ID]uint64, len(usages))
	for _, usage := range usages {
		if usage == nil || usage.NodeId == "" {
			t.eventStoreWriteBytesPerSecond = nil
			t.status = heartbeatpb.NodeResourceUsageStatus_INCOMPLETE
			t.updatedAt = now
			return
		}
		nodeID := node.ID(usage.NodeId)
		rates[nodeID] = usage.EventStoreWriteBytesPerSecond
	}
	t.updatedAt = now
	t.eventStoreWriteBytesPerSecond = rates
	t.status = heartbeatpb.NodeResourceUsageStatus_AVAILABLE
}

// EventStoreWriteBytesPerSecond returns a shared immutable rate snapshot and its
// availability. Unsupported means callers may use the legacy policy during a
// rolling upgrade. Incomplete means resource-aware moves must be suppressed.
func (t *NodeResourceUsageTracker) EventStoreWriteBytesPerSecond(
	nodeIDs []node.ID,
) (map[node.ID]uint64, heartbeatpb.NodeResourceUsageStatus) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.status == heartbeatpb.NodeResourceUsageStatus_UNSUPPORTED {
		return nil, t.status
	}
	if t.status != heartbeatpb.NodeResourceUsageStatus_AVAILABLE ||
		t.now().Sub(t.updatedAt) > nodeResourceUsageStaleThreshold {
		return nil, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE
	}
	for _, nodeID := range nodeIDs {
		if _, ok := t.eventStoreWriteBytesPerSecond[nodeID]; !ok {
			return nil, heartbeatpb.NodeResourceUsageStatus_INCOMPLETE
		}
	}
	return t.eventStoreWriteBytesPerSecond, heartbeatpb.NodeResourceUsageStatus_AVAILABLE
}
