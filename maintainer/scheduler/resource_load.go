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

package scheduler

import (
	"maps"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
)

const resourceOverloadRatio = 1.2

// resourceLoadView owns this round's projections. Moving a dispatcher relocates
// its sink work. EventStore subscriptions can be shared or remote, so their
// traffic screens destinations without assuming any immediate relief.
type resourceLoadView struct {
	loads   map[node.ID]resource.Load
	average resource.Load
}

func newResourceLoadView(loads map[node.ID]resource.Load, nodes map[node.ID]*node.Info) *resourceLoadView {
	v := &resourceLoadView{loads: make(map[node.ID]resource.Load, len(nodes)), average: make(resource.Load)}
	for id := range nodes {
		v.loads[id] = maps.Clone(loads[id])
		for kind, rate := range loads[id] {
			v.average[kind] += rate / float64(len(nodes))
		}
	}
	return v
}

func (v *resourceLoadView) sinkOverloaded(id node.ID) bool {
	return v.loads[id][resource.Sink] > v.average[resource.Sink]*resourceOverloadRatio
}

func (v *resourceLoadView) sinkUnderloaded(id node.ID) bool {
	return v.loads[id][resource.Sink] < v.average[resource.Sink]
}

func (v *resourceLoadView) canMove(source, target node.ID, sinkRate float64) bool {
	if sinkRate > 0 && !improvesNodeLoad(v.loads[source][resource.Sink], v.loads[target][resource.Sink], sinkRate) {
		return false
	}
	for kind, average := range v.average {
		if kind == resource.Sink {
			continue
		}
		// A sink-idle node can still be busy serving EventStore reads/writes
		// for other captures. Do not choose it when this would move work toward
		// a node with greater, already overloaded pressure on another resource.
		if v.loads[target][kind] > average*resourceOverloadRatio && v.loads[target][kind] > v.loads[source][kind] {
			return false
		}
	}
	return true
}

func (v *resourceLoadView) project(source, target node.ID, sinkRate float64) {
	v.loads[source][resource.Sink] = max(0, v.loads[source][resource.Sink]-sinkRate)
	v.loads[target][resource.Sink] += sinkRate
}

// improvesNodeLoad requires a meaningful reduction in the pair's imbalance.
// A single indivisible hot table must not bounce between otherwise idle nodes.
func improvesNodeLoad(source, target, rate float64) bool {
	return rate > 0 && rate < (source-target)*0.9
}
