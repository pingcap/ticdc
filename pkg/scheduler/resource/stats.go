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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package resource

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
)

const (
	// SampleInterval bounds full-node aggregation, including forced heartbeats.
	SampleInterval = time.Second
	counterShards  = 16
)

// NodeStats belongs to one capture. Components own counters; heartbeats reuse
// a cached aggregate instead of scanning dispatchers or recalculating per send.
type NodeStats struct {
	mu        sync.Mutex
	counters  map[*Counter]struct{}
	totals    map[Type]uint64
	kinds     []Type
	cached    []uint64
	sampledAt time.Time
}

// NewNodeStats creates isolated resource accounting for one capture.
func NewNodeStats() *NodeStats {
	return &NodeStats{
		counters: make(map[*Counter]struct{}),
		totals:   map[Type]uint64{Sink: 0, EventStoreWrite: 0, EventStoreRead: 0},
		kinds:    []Type{Sink, EventStoreWrite, EventStoreRead},
	}
}

// Counter is one component's resource counter, with a fixed number of shards.
// Register components, not individual dispatchers or short-lived iterators.
type Counter struct {
	node   *NodeStats
	kind   Type
	next   atomic.Uint64
	closed atomic.Bool
	shards [counterShards]Recorder
}

// Recorder is selected once when a writer is created. Padding keeps independent
// writers from contending on one cache line on common 64-byte-line platforms.
type Recorder struct {
	value    atomic.Uint64
	previous uint64 // only accessed under NodeStats.mu
	_        [48]byte
}

// NewCounter registers a component's counter for one resource dimension.
func (s *NodeStats) NewCounter(kind Type) *Counter {
	if s == nil {
		return nil
	}
	counter := &Counter{node: s, kind: kind}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.totals[kind]; !exists {
		s.totals[kind] = 0
		s.kinds = append(s.kinds, kind)
		slices.Sort(s.kinds)
		// A newly added dimension needs a new complete sample.
		s.sampledAt = time.Time{}
	}
	s.counters[counter] = struct{}{}
	return counter
}

// Recorder assigns a writer to a shard without registering the writer itself.
func (c *Counter) Recorder() *Recorder {
	if c == nil || c.closed.Load() {
		return nil
	}
	return &c.shards[(c.next.Add(1)-1)%counterShards]
}

// Add records bytes in the writer's component shard.
func (r *Recorder) Add(bytes uint64) {
	if r != nil {
		r.value.Add(bytes)
	}
}

// Close preserves the component's final contribution and unregisters its shards.
// The owner must stop its writers before closing, as with its other resources.
func (c *Counter) Close() {
	if c == nil || !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.node.mu.Lock()
	defer c.node.mu.Unlock()
	c.node.collect(c)
	delete(c.node.counters, c)
}

func (s *NodeStats) collect(c *Counter) {
	for i := range c.shards {
		shard := &c.shards[i]
		current := shard.value.Load()
		// Subtract integers before conversion to floating-point rates, preserving
		// small deltas above 2^53.
		s.totals[c.kind] += current - shard.previous
		shard.previous = current
	}
}

// Snapshot returns independent wire data, reusing totals sampled within a second.
func (s *NodeStats) Snapshot(now time.Time) *heartbeatpb.NodeLoad {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sampledAt.IsZero() || now.Sub(s.sampledAt) >= SampleInterval {
		for counter := range s.counters {
			s.collect(counter)
		}
		s.cached = s.cached[:0]
		for _, kind := range s.kinds {
			s.cached = append(s.cached, s.totals[kind])
		}
		s.sampledAt = now
	}
	load := &heartbeatpb.NodeLoad{Resources: make([]*heartbeatpb.ResourceUsage, 0, len(s.kinds))}
	for i, kind := range s.kinds {
		value := s.cached[i]
		load.Resources = append(load.Resources, &heartbeatpb.ResourceUsage{Resource: kind, TotalBytes: value})
		if kind == Sink {
			load.FlushedEventBytes = value
		}
	}
	return load
}
