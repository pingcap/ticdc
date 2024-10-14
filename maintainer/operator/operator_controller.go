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

package operator

import (
	"container/heap"
	"sync"
	"time"

	"github.com/flowbehappy/tigate/heartbeatpb"
	"github.com/flowbehappy/tigate/maintainer/replica"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/flowbehappy/tigate/pkg/node"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type Controller struct {
	lock         sync.RWMutex
	db           *replica.ReplicationDB
	operators    map[common.DispatcherID]Operator
	runningQueue operatorQueue
	batchSize    int
	mc           messaging.MessageCenter
	changefeedID string
}

func NewOperatorController(changefeedID string,
	mc messaging.MessageCenter,
	db *replica.ReplicationDB,
	batchSize int) *Controller {
	oc := &Controller{
		changefeedID: changefeedID,
		operators:    make(map[common.DispatcherID]Operator),
		runningQueue: make(operatorQueue, 0),
		mc:           mc,
		batchSize:    batchSize,
		db:           db,
	}
	return oc
}

// Execute periodically execute the operator
// todo: use a better way to control the execution frequency
func (oc *Controller) Execute() time.Time {
	executedItem := 0
	for {
		r, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(time.Millisecond * 200)
		}
		if r == nil {
			continue
		}

		oc.lock.RLock()
		msg := r.Schedule()
		oc.lock.RUnlock()

		if msg != nil {
			_ = oc.mc.SendCommand(msg)
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

func (oc *Controller) ReplicaSetRemoved(op *RemoveDispatcherOperator) {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if old, ok := oc.operators[op.ID()]; ok {
		log.Info("replica set is removed , replace the old one",
			zap.String("changefeed", oc.changefeedID),
			zap.String("replicaset", op.ID().String()),
			zap.String("operator", op.String()))
		old.OnTaskRemoved()
		delete(oc.operators, op.ID())
	}
	oc.operators[op.ID()] = op
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op Operator) bool {
	oc.lock.Lock()
	defer oc.lock.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("changefeed", oc.changefeedID),
			zap.String("operator", op.String()))
		return false
	}
	log.Info("add operator to running queue",
		zap.String("changefeed", oc.changefeedID),
		zap.String("operator", op.String()))
	oc.operators[op.ID()] = op
	op.Start()
	heap.Push(&oc.runningQueue, &operatorWithTime{op: op, time: time.Now()})
	return true
}

func (oc *Controller) GetOperator(id common.DispatcherID) Operator {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return oc.operators[id]
}

func (oc *Controller) UpdateOperatorStatus(id common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.Check(from, status)
	}
}

func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.lock.RLock()
	defer oc.lock.RUnlock()

	for _, span := range oc.db.GetTaskByNodeID(n) {
		_, ok := oc.operators[span.ID]
		if !ok {
			oc.db.MarkSpanAbsent(span)
		}
	}
	for _, op := range oc.operators {
		op.OnNodeRemove(n)
	}
}

func (oc *Controller) OperatorSize() int {
	oc.lock.RLock()
	defer oc.lock.RUnlock()
	return len(oc.operators)
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (Operator, bool) {
	oc.lock.Lock()
	defer oc.lock.Unlock()
	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operatorWithTime)
	op := item.op
	opID := item.op.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		delete(oc.operators, opID)
		log.Info("operator finished",
			zap.String("changefeed", oc.changefeedID),
			zap.String("operator", opID.String()),
			zap.String("operator", op.String()))
		return nil, true
	}
	now := time.Now()
	if now.Before(item.time) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.time = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}