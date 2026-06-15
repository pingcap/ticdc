// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

type testReplicationID string

func (id testReplicationID) String() string {
	return string(id)
}

type testReplication struct {
	id             testReplicationID
	groupID        GroupID
	nodeID         node.ID
	shouldRun      bool
	shouldRunCalls *atomic.Int64
}

func (r *testReplication) GetID() testReplicationID {
	return r.id
}

func (r *testReplication) GetGroupID() GroupID {
	return r.groupID
}

func (r *testReplication) GetNodeID() node.ID {
	return r.nodeID
}

func (r *testReplication) SetNodeID(nodeID node.ID) {
	r.nodeID = nodeID
}

func (r *testReplication) ShouldRun() bool {
	if r.shouldRunCalls != nil {
		r.shouldRunCalls.Add(1)
	}
	return r.shouldRun
}

func TestIMapLenTracksOverwriteAndDelete(t *testing.T) {
	t.Parallel()

	replicaMap := newIMap[testReplicationID, *testReplication]()
	id := testReplicationID("a")

	replicaMap.Set(id, &testReplication{id: id})
	replicaMap.Set(id, &testReplication{id: id})
	require.Equal(t, 1, replicaMap.Len())

	replicaMap.Delete(testReplicationID("missing"))
	require.Equal(t, 1, replicaMap.Len())

	replicaMap.Delete(id)
	require.Equal(t, 0, replicaMap.Len())
}

func TestGetAbsentByGroupStopsAtBatch(t *testing.T) {
	t.Parallel()

	var shouldRunCalls atomic.Int64
	db := NewReplicationDB[testReplicationID, *testReplication](
		"test",
		func(action func()) { action() },
		NewEmptyChecker[testReplicationID, *testReplication],
	)
	for i := 0; i < 100; i++ {
		id := testReplicationID(fmt.Sprintf("r%d", i))
		db.AddAbsentWithoutLock(&testReplication{
			id:             id,
			groupID:        DefaultGroupID,
			shouldRun:      true,
			shouldRunCalls: &shouldRunCalls,
		})
	}

	absent := db.GetAbsentByGroup(DefaultGroupID, 3)
	require.Len(t, absent, 3)
	require.Equal(t, int64(3), shouldRunCalls.Load())
}

func TestGetAbsentByGroupSkipsNotRunnableTasks(t *testing.T) {
	t.Parallel()

	var shouldRunCalls atomic.Int64
	db := NewReplicationDB[testReplicationID, *testReplication](
		"test",
		func(action func()) { action() },
		NewEmptyChecker[testReplicationID, *testReplication],
	)
	for i := 0; i < 100; i++ {
		id := testReplicationID(fmt.Sprintf("r%d", i))
		db.AddAbsentWithoutLock(&testReplication{
			id:             id,
			groupID:        DefaultGroupID,
			shouldRunCalls: &shouldRunCalls,
		})
	}

	absent := db.GetAbsentByGroup(DefaultGroupID, 3)
	require.Len(t, absent, 0)
	require.Equal(t, int64(100), shouldRunCalls.Load())
}
