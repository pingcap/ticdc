// Copyright 2025 PingCAP, Inc.
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

package operator

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/scheduler/replica"
)

// OperatorWithTime is a wrapper for Operator, it contains the time the operator enqueued and created.
type OperatorWithTime[T replica.ReplicationID, S replica.ReplicationStatus] struct {
	// OP is the underlying operator to be executed
	OP Operator[T, S]
	// NotifyAt records when this operator should be notified
	NotifyAt time.Time
	// CreatedAt records when this operator was created
	CreatedAt time.Time
	// IsRemoved indicates whether this operator has been marked for removal
	IsRemoved atomic.Bool
}

func NewOperatorWithTime[T replica.ReplicationID, S replica.ReplicationStatus](op Operator[T, S], time time.Time) *OperatorWithTime[T, S] {
	return &OperatorWithTime[T, S]{OP: op, NotifyAt: time, CreatedAt: time}
}

// OperatorQueue is a priority queue of operators that implements heap.Interface.
// It orders operators based on their enqueue time (FIFO order).
// Type parameters:
//   - T: represents the ReplicationID type for identifying replications
//   - S: represents the ReplicationStatus type for tracking replication state
type OperatorQueue[T replica.ReplicationID, S replica.ReplicationStatus] []*OperatorWithTime[T, S]

func (o OperatorQueue[T, S]) Len() int { return len(o) }

func (o OperatorQueue[T, S]) Less(i, j int) bool {
	return o[i].NotifyAt.Before(o[j].NotifyAt)
}

func (o OperatorQueue[T, S]) Swap(i, j int) {
	o[i], o[j] = o[j], o[i]
}

func (o *OperatorQueue[T, S]) Push(x interface{}) {
	item := x.(*OperatorWithTime[T, S])
	*o = append(*o, item)
}

func (o *OperatorQueue[T, S]) Pop() interface{} {
	old := *o
	n := len(old)
	if n == 0 {
		return nil
	}
	item := old[n-1]
	*o = old[0 : n-1]
	return item
}
