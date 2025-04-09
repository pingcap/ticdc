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

package scheduler

import (
	"math/rand"

	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/heap"
)

// TODO: abstract
const (
	randomPartBitSize = 8
	randomPartMask    = (1 << randomPartBitSize) - 1
)

// randomizeWorkload injects small randomness into the workload, so that
// when two captures tied in competing for the minimum workload, the result
// will not always be the same.
// The bitwise layout of the return value is:
// 63                8                0
// |----- input -----|-- random val --|
func randomizeWorkload(random *rand.Rand, input int) int {
	if random == nil {
		return input
	}
	randomPart := int(random.Uint32() & randomPartMask)
	// randomPart is a small random value that only affects the
	// result of comparison of workloads when two workloads are equal.
	return (input << randomPartBitSize) | randomPart
}

type priorityQueue struct {
	h    *heap.Heap[*item]
	less func(a, b int) bool

	rand *rand.Rand
}

func (q *priorityQueue) InitItem(node node.ID, load int, tasks []*replica.SpanReplication) {
	q.AddOrUpdate(&item{
		Node:  node,
		Tasks: tasks,
		Load:  load,
		less:  q.less,
	})
}

func (q *priorityQueue) AddOrUpdate(item *item) {
	item.randomizeWorkload = randomizeWorkload(q.rand, item.Load)
	q.h.AddOrUpdate(item)
}

func (q *priorityQueue) PeekTop() (*item, bool) {
	return q.h.PeekTop()
}

// item is an item in the priority queue, use the Load field as the priority
type item struct {
	// for internal usage
	Node  node.ID
	Tasks []*replica.SpanReplication
	Load  int

	// for heap adjustment usage
	index             int
	randomizeWorkload int
	less              func(randomizeWorkloadA, randomizeWorkloadB int) bool
}

func (i *item) SetHeapIndex(idx int) {
	i.index = idx
}

func (i *item) GetHeapIndex() int {
	return i.index
}

func (i *item) LessThan(t *item) bool {
	return i.less(i.randomizeWorkload, t.randomizeWorkload)
}
