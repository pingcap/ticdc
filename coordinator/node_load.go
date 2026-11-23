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
	"slices"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
)

const nodeLoadStaleThreshold = 5 * time.Second

type nodeLoadState struct {
	supported bool
	bytes     uint64
	rate      float64
	available bool
	updatedAt time.Time
}

func (s *nodeLoadState) observe(load *heartbeatpb.NodeLoad, now time.Time) {
	if load == nil {
		*s = nodeLoadState{}
		return
	}
	s.supported = true
	if !s.updatedAt.IsZero() && !now.After(s.updatedAt) {
		return
	}
	elapsed := now.Sub(s.updatedAt)
	if !s.updatedAt.IsZero() && elapsed < time.Second && load.FlushedEventBytes >= s.bytes {
		// Witness acknowledgements and drain updates force extra heartbeats.
		// Keep one sampling window instead of replacing rates with tiny deltas.
		return
	}
	s.available = !s.updatedAt.IsZero() && elapsed <= nodeLoadStaleThreshold && load.FlushedEventBytes >= s.bytes
	if s.available {
		s.rate = float64(load.FlushedEventBytes-s.bytes) / elapsed.Seconds()
	}
	s.bytes = load.FlushedEventBytes
	s.updatedAt = now
}

// nodeLoadSnapshot only distributes observations. Maintainers independently
// decide which of their dispatchers to move, without coordinator approval.
func (c *captureWriteLeaseController) nodeLoadSnapshot() ([]*heartbeatpb.NodeLoad, heartbeatpb.NodeLoadStatus) {
	if len(c.activeNodes) == 0 {
		return nil, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED
	}
	for _, id := range c.activeNodes {
		if state := c.nodes[id]; state == nil || !state.load.supported {
			return nil, heartbeatpb.NodeLoadStatus_LOAD_UNSUPPORTED
		}
	}
	now := c.now()
	ids := slices.Clone(c.activeNodes)
	slices.Sort(ids)
	loads := make([]*heartbeatpb.NodeLoad, 0, len(ids))
	for _, id := range ids {
		load := &c.nodes[id].load
		if !load.available || now.Sub(load.updatedAt) > nodeLoadStaleThreshold {
			return nil, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
		}
		loads = append(loads, &heartbeatpb.NodeLoad{NodeId: id.String(), BytesPerSecond: load.rate})
	}
	return loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE
}
