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
	"math"
	"slices"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
)

const nodeLoadStaleThreshold = 5 * time.Second

type nodeLoadState struct {
	supported bool
	bytes     map[resource.Type]uint64
	rates     resource.Load
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
	counters, valid := resource.Counters(load)
	continuous := valid && len(counters) == len(s.bytes)
	for kind, value := range counters {
		previous, exists := s.bytes[kind]
		if !exists || value < previous {
			continuous = false
		}
	}
	elapsed := now.Sub(s.updatedAt)
	if !s.updatedAt.IsZero() && elapsed < time.Second && continuous {
		// Witness acknowledgements and drain updates force extra heartbeats.
		// Keep one sampling window instead of replacing rates with tiny deltas.
		return
	}
	available := !s.updatedAt.IsZero() && elapsed <= nodeLoadStaleThreshold && continuous
	if available {
		// Match the dispatcher reporting timescale independently for each
		// resource; asynchronous flushes must not look like a new hotspot.
		weight := -math.Expm1(-elapsed.Seconds() / heartbeatpb.DispatcherTrafficSampleInterval.Seconds())
		for kind, value := range counters {
			rate := float64(value-s.bytes[kind]) / elapsed.Seconds()
			if s.available {
				s.rates[kind] += weight * (rate - s.rates[kind])
			} else {
				s.rates[kind] = rate
			}
		}
	} else {
		s.rates = make(resource.Load, len(counters))
	}
	s.available = available
	s.bytes = counters
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
	var commonResources resource.Load
	for _, id := range ids {
		load := &c.nodes[id].load
		if !load.available || now.Sub(load.updatedAt) > nodeLoadStaleThreshold {
			return nil, heartbeatpb.NodeLoadStatus_LOAD_INCOMPLETE
		}
		if commonResources == nil {
			commonResources = make(resource.Load, len(load.rates))
			for kind := range load.rates {
				commonResources[kind] = 0
			}
		}
		for kind := range commonResources {
			if _, supported := load.rates[kind]; !supported {
				delete(commonResources, kind)
			}
		}
	}
	// Only compare a resource when every active node reports it. Missing
	// metrics during an upgrade must not make old captures appear idle.
	for _, id := range ids {
		rates := make(resource.Load, len(commonResources))
		for kind := range commonResources {
			rates[kind] = c.nodes[id].load.rates[kind]
		}
		loads = append(loads, rates.ToNodeLoad(id.String()))
	}
	return loads, heartbeatpb.NodeLoadStatus_LOAD_AVAILABLE
}
