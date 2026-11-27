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
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/resource"
	"github.com/stretchr/testify/require"
)

func TestResourceLoadMoveProjection(t *testing.T) {
	loads := map[node.ID]resource.Load{
		"source": {resource.Sink: 300, resource.EventStoreWrite: 100, resource.EventStoreRead: 200},
		"target": {resource.Sink: 0, resource.EventStoreWrite: 100, resource.EventStoreRead: 200},
	}
	nodes := map[node.ID]*node.Info{"source": {ID: "source"}, "target": {ID: "target"}}
	v := newResourceLoadView(loads, nodes)
	require.True(t, v.sinkOverloaded("source"))
	require.True(t, v.canMove("source", "target", 100))
	v.project("source", "target", 100)
	require.Equal(t, resource.Load{resource.Sink: 300, resource.EventStoreWrite: 100, resource.EventStoreRead: 200}, loads["source"], "projection is private to this maintainer")
	require.Equal(t, resource.Load{resource.Sink: 200, resource.EventStoreWrite: 100, resource.EventStoreRead: 200}, v.loads["source"], "shared or remote EventStore work must not be subtracted")
	require.False(t, v.canMove("source", "target", 100), "another hot move would not improve balance")

	// Count balancing must apply the same target guards even to idle tables.
	v.loads["target"][resource.EventStoreRead] = 1000
	require.False(t, v.canMove("source", "target", 0))
}
