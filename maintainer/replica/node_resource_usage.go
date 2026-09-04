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

	"github.com/pingcap/ticdc/pkg/node"
)

// Resource usage is normally reported every 200ms (50ms in low-latency mode).
// Allow several missed reports before falling back to group-local traffic.
const nodeResourceUsageStaleThreshold = 5 * time.Second

type eventStoreWriteBytesSample struct {
	writeBytes uint64
	updatedAt  time.Time
}

// NodeResourceUsageTracker stores the latest node-wide cumulative counters
// reported to one changefeed maintainer.
type NodeResourceUsageTracker struct {
	mu                   sync.RWMutex
	eventStoreWriteBytes map[node.ID]eventStoreWriteBytesSample
	now                  func() time.Time
}

func NewNodeResourceUsageTracker() *NodeResourceUsageTracker {
	return &NodeResourceUsageTracker{
		eventStoreWriteBytes: make(map[node.ID]eventStoreWriteBytesSample),
		now:                  time.Now,
	}
}

func (t *NodeResourceUsageTracker) UpdateEventStoreWriteBytes(nodeID node.ID, writeBytes uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.eventStoreWriteBytes[nodeID] = eventStoreWriteBytesSample{
		writeBytes: writeBytes,
		updatedAt:  t.now(),
	}
}

// EventStoreWriteBytes returns a complete snapshot for the requested nodes.
// The second return value is false when any node has not reported the counter or
// its last report is stale. This lets callers retain the old scheduling behavior
// during rolling upgrades and heartbeat interruptions.
func (t *NodeResourceUsageTracker) EventStoreWriteBytes(nodeIDs []node.ID) (map[node.ID]uint64, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	now := t.now()
	result := make(map[node.ID]uint64, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		sample, ok := t.eventStoreWriteBytes[nodeID]
		if !ok || now.Sub(sample.updatedAt) > nodeResourceUsageStaleThreshold {
			return nil, false
		}
		result[nodeID] = sample.writeBytes
	}
	return result, true
}
