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

// NodeResourceUsageTracker converts cluster-wide cumulative counters into one
// immutable delta snapshot shared by all local changefeed group checkers.
type NodeResourceUsageTracker struct {
	mu                   sync.RWMutex
	previousWriteBytes   map[node.ID]uint64
	eventStoreWriteDelta map[node.ID]uint64
	status               heartbeatpb.NodeResourceUsageStatus
	updatedAt            time.Time
	now                  func() time.Time
}

func NewNodeResourceUsageTracker() *NodeResourceUsageTracker {
	return &NodeResourceUsageTracker{
		status: heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_UNSUPPORTED,
		now:    time.Now,
	}
}

// ReplaceEventStoreWriteBytes atomically replaces the cluster snapshot and
// computes one delta map for all group checkers. Incomplete telemetry clears
// the baseline; unsupported telemetry preserves the rolling-upgrade fallback.
// The tracker takes ownership of writeBytes and never mutates it.
func (t *NodeResourceUsageTracker) ReplaceEventStoreWriteBytes(
	writeBytes map[node.ID]uint64,
	status heartbeatpb.NodeResourceUsageStatus,
) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	if status != heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE {
		if status != heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_UNSUPPORTED {
			status = heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE
		}
		t.previousWriteBytes = nil
		t.eventStoreWriteDelta = nil
		t.status = status
		t.updatedAt = now
		return
	}

	current := writeBytes
	previous := t.previousWriteBytes
	previousIsFresh := !t.updatedAt.IsZero() && now.Sub(t.updatedAt) <= nodeResourceUsageStaleThreshold
	t.previousWriteBytes = current
	t.eventStoreWriteDelta = nil
	t.status = heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE
	t.updatedAt = now
	if !previousIsFresh || len(previous) != len(current) {
		return
	}

	delta := make(map[node.ID]uint64, len(current))
	for nodeID, currentValue := range current {
		previousValue, ok := previous[nodeID]
		if !ok || currentValue < previousValue {
			return
		}
		delta[nodeID] = currentValue - previousValue
	}
	t.eventStoreWriteDelta = delta
	t.status = heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE
}

// EventStoreWriteBytesDelta returns a shared immutable delta snapshot and its
// availability. Unsupported means callers may use the legacy policy during a
// rolling upgrade. Incomplete means resource-aware moves must be suppressed.
func (t *NodeResourceUsageTracker) EventStoreWriteBytesDelta(
	nodeIDs []node.ID,
) (map[node.ID]uint64, heartbeatpb.NodeResourceUsageStatus) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.status == heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_UNSUPPORTED {
		return nil, t.status
	}
	if t.status != heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE ||
		t.now().Sub(t.updatedAt) > nodeResourceUsageStaleThreshold {
		return nil, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE
	}
	for _, nodeID := range nodeIDs {
		if _, ok := t.eventStoreWriteDelta[nodeID]; !ok {
			return nil, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_INCOMPLETE
		}
	}
	return t.eventStoreWriteDelta, heartbeatpb.NodeResourceUsageStatus_NODE_RESOURCE_USAGE_AVAILABLE
}
