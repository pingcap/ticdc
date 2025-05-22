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

package logcoordinator

import (
	"testing"

	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/assert"
)

func newLogCoordinatorForTest() *logCoordinator {
	c := &logCoordinator{}
	c.eventStoreStates.m = make(map[node.ID]*logservicepb.EventStoreState)
	return c
}

func TestGetCandidateNodes(t *testing.T) {
	coordinator := newLogCoordinatorForTest()

	nodeID1 := node.ID("node-1")
	nodeID2 := node.ID("node-2")
	nodeID3 := node.ID("node-3")

	// initialize table spans
	tableID1 := int64(100)
	tableID2 := int64(101)
	span1 := common.TableIDToComparableSpan(tableID1)
	span2 := common.TableIDToComparableSpan(tableID2)

	// initialize event store states
	coordinator.updateEventStoreState(nodeID1, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   200,
					},
				},
			},
		},
	})
	coordinator.updateEventStoreState(nodeID2, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 90,
						ResolvedTs:   180,
					},
					{
						SubID:        2,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   220,
					},
					{
						SubID:        3,
						Span:         &span1,
						CheckpointTs: 80,
						ResolvedTs:   160,
					},
				},
			},
			tableID2: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        4,
						Span:         &span2,
						CheckpointTs: 90,
						ResolvedTs:   190,
					},
					{
						SubID:        5,
						Span:         &span2,
						CheckpointTs: 90,
						ResolvedTs:   240,
					},
				},
			},
		},
	})
	coordinator.updateEventStoreState(nodeID3, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID2: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span2,
						CheckpointTs: 100,
						ResolvedTs:   290,
					},
					{
						SubID:        2,
						Span:         &span2,
						CheckpointTs: 100,
						ResolvedTs:   230,
					},
				},
			},
		},
	})

	// check get candidates
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID1, &span2, uint64(100))
		assert.Equal(t, []string{nodeID3.String(), nodeID2.String()}, nodes)
	}
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span2, uint64(100))
		assert.Equal(t, []string{nodeID2.String()}, nodes)
	}

	// update event store state for node1 and check again
	coordinator.updateEventStoreState(nodeID1, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   300,
					},
				},
			},
		},
	})
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID1.String(), nodeID2.String()}, nodes)
	}

	// update event store state for node2 and check again
	coordinator.updateEventStoreState(nodeID2, &logservicepb.EventStoreState{
		TableStates: map[int64]*logservicepb.TableState{
			tableID1: {
				Subscriptions: []*logservicepb.SubscriptionState{
					{
						SubID:        1,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   230,
					},
					{
						SubID:        2,
						Span:         &span1,
						CheckpointTs: 100,
						ResolvedTs:   310,
					},
				},
			},
		},
	})
	{
		nodes := coordinator.getCandidateNodes(nodeID3, &span1, uint64(100))
		assert.Equal(t, []string{nodeID2.String(), nodeID1.String()}, nodes)
	}
}
