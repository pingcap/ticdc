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
	"math/rand"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/replica"
	"github.com/pingcap/ticdc/maintainer/span"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	"go.uber.org/zap"
)

const (
	// emptyPollInterval is the interval to poll the operator from the queue when the queue is empty.
	emptyPollInterval = time.Millisecond * 200
	// nextPollInterval is the interval to poll the operator from the queue when the queue is not empty.
	nextPollInterval = time.Millisecond * 50
	// minSendMessageInterval is the minimum interval to send same message to the dispatcher.
	minSendMessageInterval = 5 * time.Second
)

var _ operator.Controller[common.DispatcherID, *heartbeatpb.TableSpanStatus] = &Controller{}

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	role           string
	changefeedID   common.ChangeFeedID
	batchSize      int
	messageCenter  messaging.MessageCenter
	spanController *span.Controller
	nodeManager    *watcher.NodeManager
	splitter       *split.Splitter

	mu           sync.RWMutex // protect the following fields
	operators    map[common.DispatcherID]*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus]
	runningQueue operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus]
}

// NewOperatorController creates a new operator controller
func NewOperatorController(
	changefeedID common.ChangeFeedID,
	spanController *span.Controller,
	batchSize int,
) *Controller {
	return &Controller{
		changefeedID:   changefeedID,
		batchSize:      batchSize,
		operators:      make(map[common.DispatcherID]*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus]),
		runningQueue:   make(operator.OperatorQueue[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0),
		role:           "maintainer",
		spanController: spanController,
		nodeManager:    appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
		messageCenter:  appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
	}
}

// Execute poll the operator from the queue and execute it
// It will be called in the thread pool.
func (oc *Controller) Execute() time.Time {
	executedCounter := 0
	for {
		op, next := oc.pollQueueingOperator()
		if !next {
			return time.Now().Add(emptyPollInterval)
		}
		if op == nil {
			continue
		}

		msg := op.Schedule()

		if msg != nil {
			_ = oc.messageCenter.SendCommand(msg)
			log.Info("send command to dispatcher",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("operator", op.String()))
		}
		executedCounter++
		if executedCounter >= oc.batchSize {
			return time.Now().Add(nextPollInterval)
		}
	}
}

// RemoveAllTasks remove all tasks, and notify all operators to stop.
// it is only called by the barrier when the changefeed is stopped.
func (oc *Controller) RemoveAllTasks() {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	for _, replicaSet := range oc.spanController.RemoveAll() {
		oc.removeReplicaSet(newRemoveDispatcherOperator(oc.spanController, replicaSet))
	}
}

// RemoveTasksBySchemaID remove all tasks by schema id.
// it is only by the barrier when the schema is dropped by ddl
func (oc *Controller) RemoveTasksBySchemaID(schemaID int64) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	for _, replicaSet := range oc.spanController.RemoveBySchemaID(schemaID) {
		oc.removeReplicaSet(newRemoveDispatcherOperator(oc.spanController, replicaSet))
	}
}

// RemoveTasksByTableIDs remove all tasks by table ids.
// it is only called by the barrier when the table is dropped by ddl
func (oc *Controller) RemoveTasksByTableIDs(tables ...int64) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	for _, replicaSet := range oc.spanController.RemoveByTableIDs(tables...) {
		oc.removeReplicaSet(newRemoveDispatcherOperator(oc.spanController, replicaSet))
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	if _, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", op.String()))
		return false
	}
	span := oc.spanController.GetTaskByID(op.ID())
	if span == nil {
		log.Warn("add operator failed, span not found",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

func (oc *Controller) UpdateOperatorStatus(id common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.OP.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all spans on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	for _, span := range oc.spanController.GetTaskByNodeID(n) {
		_, ok := oc.operators[span.ID]
		if !ok {
			oc.spanController.MarkSpanAbsent(span)
		}
	}
	for _, op := range oc.operators {
		op.OP.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id common.DispatcherID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	if op, ok := oc.operators[id]; !ok {
		return nil
	} else {
		return op.OP
	}
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSize() int {
	oc.mu.RLock()
	defer oc.mu.RUnlock()
	return len(oc.operators)
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSizeWithLock() int {
	return len(oc.operators)
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (
	operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus],
	bool,
) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	if item.IsRemoved {
		return nil, true
	}
	op := item.OP
	opID := op.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		item.IsRemoved = true
		delete(oc.operators, opID)
		metrics.FinishedOperatorCount.WithLabelValues(common.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Inc()
		metrics.OperatorDuration.WithLabelValues(common.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Observe(time.Since(item.CreatedAt).Seconds())
		log.Info("operator finished",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", opID.String()),
			zap.String("operator", op.String()))
		return nil, true
	}
	now := time.Now()
	if now.Before(item.NotifyAt) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.NotifyAt = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}

func (oc *Controller) removeReplicaSet(op *removeDispatcherOperator) {
	if old, ok := oc.operators[op.ID()]; ok {
		log.Info("replica set is removed , replace the old one",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("replicaSet", old.OP.ID().String()),
			zap.String("operator", old.OP.String()))
		old.OP.OnTaskRemoved()
		old.OP.PostFinish()
		old.IsRemoved = true
		delete(oc.operators, op.ID())
	}
	oc.pushOperator(op)
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	oc.checkAffectedNodes(op)
	log.Info("add operator to running queue",
		zap.String("role", oc.role),
		zap.String("changefeed", oc.changefeedID.Name()),
		zap.String("operator", op.String()))
	withTime := operator.NewOperatorWithTime(op, time.Now())
	oc.operators[op.ID()] = withTime
	op.Start()
	heap.Push(&oc.runningQueue, withTime)
	metrics.CreatedOperatorCount.WithLabelValues(common.DefaultNamespace, oc.changefeedID.Name(), op.Type()).Inc()
}

func (oc *Controller) checkAffectedNodes(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	aliveNodes := oc.nodeManager.GetAliveNodes()
	for _, nodeID := range op.AffectedNodes() {
		if _, ok := aliveNodes[nodeID]; !ok {
			op.OnNodeRemove(nodeID)
		}
	}
}

func (oc *Controller) NewAddOperator(replicaSet *replica.SpanReplication, id node.ID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &AddDispatcherOperator{
		replicaSet:     replicaSet,
		dest:           id,
		spanController: oc.spanController,
	}
}

func (oc *Controller) NewMoveOperator(replicaSet *replica.SpanReplication, origin, dest node.ID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &MoveDispatcherOperator{
		replicaSet:     replicaSet,
		origin:         origin,
		dest:           dest,
		spanController: oc.spanController,
	}
}

func (oc *Controller) NewRemoveOperator(replicaSet *replica.SpanReplication) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return &removeDispatcherOperator{
		replicaSet:     replicaSet,
		spanController: oc.spanController,
	}
}

func (oc *Controller) NewSplitOperator(
	replicaSet *replica.SpanReplication, originNode node.ID, splitSpans []*heartbeatpb.TableSpan,
) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return NewSplitDispatcherOperator(oc.spanController, replicaSet, originNode, splitSpans)
}

// AddMergeOperator creates a merge operator, which merge consecutive replica sets.
// We need create a mergeOperator for the new replicaset, and create len(affectedReplicaSets) empty operator
// to occupy these replica set not evolve other scheduling among merging.
func (oc *Controller) AddMergeOperator(
	affectedReplicaSets []*replica.SpanReplication,
) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	operators := make([]operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(affectedReplicaSets))
	for _, replicaSet := range affectedReplicaSets {
		operator := NewOccupyDispatcherOperator(oc.spanController, replicaSet)
		operators = append(operators, operator)

		if _, ok := oc.operators[operator.ID()]; ok {
			log.Info("add operator failed, operator already exists",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("operator", operator.String()))
			return nil
		}
		oc.pushOperator(operator)
	}

	mergeOperator := NewMergeDispatcherOperator(oc.spanController, affectedReplicaSets, operators)
	if mergeOperator == nil {
		log.Error("failed to create merge operator",
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.Int("affectedReplicaSets", len(affectedReplicaSets)),
		)
		return nil
	}
	if _, ok := oc.operators[mergeOperator.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("role", oc.role),
			zap.String("changefeed", oc.changefeedID.Name()),
			zap.String("operator", mergeOperator.String()))
		return nil
	}
	oc.pushOperator(mergeOperator)

	log.Info("add merge operator",
		zap.String("role", oc.role),
		zap.String("changefeed", oc.changefeedID.Name()),
		zap.Int("affectedReplicaSets", len(affectedReplicaSets)),
	)
	return mergeOperator
}

// AddMergeSplitOperator adds a merge split operator to the controller.
//  1. Merge Operator: len(affectedReplicaSets) > 1, len(splitSpans) == 1
//  2. Split Operator: len(affectedReplicaSets) == 1, len(splitSpans) > 1
//  3. MergeAndSplit Operator: len(affectedReplicaSets) > 1, len(splitSpans) > 1
func (oc *Controller) AddMergeSplitOperator(
	affectedReplicaSets []*replica.SpanReplication,
	splitSpans []*heartbeatpb.TableSpan,
) bool {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	// TODO: check if there are some intersection between `ret.Replications` and `spans`.
	// Ignore the intersection spans to prevent meaningless split operation.
	for _, replicaSet := range affectedReplicaSets {
		if _, ok := oc.operators[replicaSet.ID]; ok {
			log.Info("add operator failed, operator already exists",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("dispatcherID", replicaSet.ID.String()),
			)
			return false
		}
		span := oc.spanController.GetTaskByID(replicaSet.ID)
		if span == nil {
			log.Warn("add operator failed, span not found",
				zap.String("role", oc.role),
				zap.String("changefeed", oc.changefeedID.Name()),
				zap.String("dispatcherID", replicaSet.ID.String()))
			return false
		}
	}
	randomIdx := rand.Intn(len(affectedReplicaSets))
	primaryID := affectedReplicaSets[randomIdx].ID
	primaryOp := NewMergeSplitDispatcherOperator(oc.spanController, primaryID, affectedReplicaSets[randomIdx], affectedReplicaSets, splitSpans, nil)
	for _, replicaSet := range affectedReplicaSets {
		var op *MergeSplitDispatcherOperator
		if replicaSet.ID == primaryID {
			op = primaryOp
		} else {
			op = NewMergeSplitDispatcherOperator(oc.spanController, primaryID, replicaSet, nil, nil, primaryOp.onFinished)
		}
		oc.pushOperator(op)
	}
	log.Info("add merge split operator",
		zap.String("role", oc.role),
		zap.String("changefeed", oc.changefeedID.Name()),
		zap.String("primary", primaryID.String()),
		zap.Int64("tableID", splitSpans[0].TableID),
		zap.Int("oldSpans", len(affectedReplicaSets)),
		zap.Int("newSpans", len(splitSpans)),
	)
	return true
}

func (oc *Controller) GetLock() *sync.RWMutex {
	oc.mu.Lock()
	return &oc.mu
}

func (oc *Controller) ReleaseLock(mutex *sync.RWMutex) {
	mutex.Unlock()
}
