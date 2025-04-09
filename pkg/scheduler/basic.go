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
	"time"

	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/pkg/scheduler/replica"
	"github.com/pingcap/ticdc/server/watcher"
)

// basicScheduler generates operators for the spans, and push them to the operator controller
// it generates add operator for the absent spans, and move operator for the unbalanced replicating spans
// currently, it only supports balance the spans by size
type basicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]] struct {
	id        string
	batchSize int

	operatorController operator.Controller[T, S]
	db                 replica.ScheduleGroup[T, R]
	nodeManager        *watcher.NodeManager

	absent []R // buffer for the absent spans
	// newAddOperator  func(r R, target node.ID) operator.Operator[T, S] // scheduler r to target node
	doBasicSchedule func(replica.ScheduleGroup[T, R], *watcher.NodeManager, operator.Controller[T, S], int)
}

func NewBasicScheduler[T replica.ReplicationID, S replica.ReplicationStatus, R replica.Replication[T]](
	id string, batchSize int,
	oc operator.Controller[T, S],
	db replica.ScheduleGroup[T, R],
	nodeManager *watcher.NodeManager,
	doBasicSchedule func(replica.ScheduleGroup[T, R], *watcher.NodeManager, operator.Controller[T, S], int),
) *basicScheduler[T, S, R] {
	return &basicScheduler[T, S, R]{
		id:                 id,
		batchSize:          batchSize,
		operatorController: oc,
		db:                 db,
		nodeManager:        nodeManager,
		absent:             make([]R, 0, batchSize),
		// newAddOperator:     newAddOperator,
		doBasicSchedule: doBasicSchedule,
	}
}

// Execute periodically execute the operator
func (s *basicScheduler[T, S, R]) Execute() time.Time {
	availableSize := s.batchSize - s.operatorController.OperatorSize()
	if s.db.GetAbsentSize() <= 0 || availableSize <= 0 {
		// can not schedule more operators, skip
		return time.Now().Add(time.Millisecond * 500)
	}
	if availableSize < s.batchSize/2 {
		// too many running operators, skip
		return time.Now().Add(time.Millisecond * 100)
	}

	s.doBasicSchedule(s.db, s.nodeManager, s.operatorController, availableSize)

	return time.Now().Add(time.Millisecond * 500)
}
func (s *basicScheduler[T, S, R]) Name() string {
	return BasicScheduler
}
