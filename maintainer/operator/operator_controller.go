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
	mode         int64
	// lastWarnTime tracks the last warning time for each operator to avoid spam logs
	lastWarnTime map[common.DispatcherID]time.Time
}

// NewOperatorController creates a new operator controller
func NewOperatorController(
	changefeedID common.ChangeFeedID,
	spanController *span.Controller,
	batchSize int,
	mode int64,
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
		mode:           mode,
		lastWarnTime:   make(map[common.DispatcherID]time.Time),
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
			log.Debug("send command to dispatcher",
				zap.String("role", oc.role),
				zap.Stringer("changefeedID", oc.changefeedID),
				zap.String("operator", op.String()),
				zap.Any("msg", msg.Message))
		}
		executedCounter++
		if executedCounter >= oc.batchSize {
			return time.Now().Add(nextPollInterval)
		}
	}
}

// RemoveTasksBySchemaID remove all tasks by schema id.
// it is only by the barrier when the schema is dropped by ddl
func (oc *Controller) RemoveTasksBySchemaID(schemaID int64) {
	tasks := oc.spanController.GetRemoveTasksBySchemaID(schemaID)
	for _, task := range tasks {
		oc.removeReplicaSet(newRemoveDispatcherOperator(oc.spanController, task, heartbeatpb.OperatorType_O_Remove))
	}
	oc.spanController.RemoveBySchemaID(schemaID)
}

// RemoveTasksByTableIDs remove all tasks by table ids.
// it is only called by the barrier when the table is dropped by ddl
//
// When the split dispatcher operator is running, a TRUNCATE TABLE DDL can potentially drop the dispatcher.
// This leads to the completion of the split dispatcher operator and the subsequent removal of the span.
// However, the operator callback may erroneously mark the span as absent. To avoid this situation,
// we should first remove the replicaSet and then remove the span to ensure it doesn't remain active.
//
// Note: removeReplicaSet creates operators and touches the operator controller lock hierarchy, so it must
// NOT be executed while holding spanController's internal locks, otherwise deadlock may happen.
func (oc *Controller) RemoveTasksByTableIDs(tables ...int64) {
	tasks := oc.spanController.GetRemoveTasksByTableIDs(tables...)
	for _, task := range tasks {
		oc.removeReplicaSet(newRemoveDispatcherOperator(oc.spanController, task, heartbeatpb.OperatorType_O_Remove))
	}
	oc.spanController.RemoveByTableIDs(tables...)
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) bool {
	oc.mu.RLock()
	if _, ok := oc.operators[op.ID()]; ok {
		oc.mu.RUnlock()
		log.Info("add operator failed, operator already exists",
			zap.String("role", oc.role),
			zap.Stringer("changefeedID", oc.changefeedID),
			zap.String("operator", op.String()))
		return false
	}
	oc.mu.RUnlock()
	span := oc.spanController.GetTaskByID(op.ID())
	if span == nil {
		log.Warn("add operator failed, span not found",
			zap.String("role", oc.role),
			zap.Stringer("changefeedID", oc.changefeedID),
			zap.String("operator", op.String()))
		return false
	}
	oc.pushOperator(op)
	return true
}

func (oc *Controller) UpdateOperatorStatus(id common.DispatcherID, from node.ID, status *heartbeatpb.TableSpanStatus) {
	oc.mu.RLock()
	op, ok := oc.operators[id]
	oc.mu.RUnlock()

	if ok {
		op.OP.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all spans on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	for _, span := range oc.spanController.GetTaskByNodeID(n) {
		oc.mu.RLock()
		_, ok := oc.operators[span.ID]
		oc.mu.RUnlock()
		if !ok {
			oc.spanController.MarkSpanAbsent(span)
		}
	}
	ops := oc.GetAllOperators()
	for _, op := range ops {
		op.OnNodeRemove(n)
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

func (oc *Controller) GetMinCheckpointTs(minCheckpointTs uint64) uint64 {
	ops := oc.GetAllOperators()

	for _, op := range ops {
		if op.BlockTsForward() {
			spanReplication := oc.spanController.GetTaskByID(op.ID())
			if spanReplication == nil {
				log.Info("span replication is nil", zap.String("operator", op.String()))
				continue
			}
			if spanReplication.GetStatus().CheckpointTs < minCheckpointTs {
				minCheckpointTs = spanReplication.GetStatus().CheckpointTs
			}
		}
	}
	return minCheckpointTs
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (
	operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus],
	bool,
) {
	oc.mu.Lock()
	if oc.runningQueue.Len() == 0 {
		oc.mu.Unlock()
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus])
	op := item.OP
	opID := op.ID()
	oc.mu.Unlock()
	if item.IsRemoved.Load() {
		return nil, true
	}
	if op.IsFinished() {
		oc.finalizeOperator(item, opID)
		return nil, true
	}
	// log warn message for stil running operator
	if time.Since(item.CreatedAt) > time.Second*30 {
		now := time.Now()
		oc.mu.Lock()
		lastWarn, exists := oc.lastWarnTime[opID]
		shouldWarn := !exists || now.Sub(lastWarn) >= time.Second*30
		if shouldWarn {
			oc.lastWarnTime[opID] = now
		}
		oc.mu.Unlock()

		if shouldWarn {
			log.Warn("operator is still in running queue",
				zap.Stringer("changefeedID", oc.changefeedID),
				zap.String("operator", opID.String()),
				zap.String("operator", op.String()),
				zap.Any("timeSinceCreated", time.Since(item.CreatedAt)))
		}
	}
	now := time.Now()
	oc.mu.Lock()
	defer oc.mu.Unlock()
	if item.IsRemoved.Load() {
		return nil, true
	}
	if now.Before(item.NotifyAt) {
		heap.Push(&oc.runningQueue, item)
		return nil, false
	}
	// pushes with new notify time.
	item.NotifyAt = time.Now().Add(time.Millisecond * 500)
	heap.Push(&oc.runningQueue, item)
	return op, true
}

func (oc *Controller) finalizeOperator(
	item *operator.OperatorWithTime[common.DispatcherID, *heartbeatpb.TableSpanStatus],
	opID common.DispatcherID,
) {
	if !item.IsRemoved.CompareAndSwap(false, true) {
		return
	}
	op := item.OP
	// Always call the PostFinish method to ensure the operator is cleaned up by itself.
	op.PostFinish()

	oc.mu.Lock()
	if cur, ok := oc.operators[opID]; ok && cur == item {
		delete(oc.operators, opID)
	}
	delete(oc.lastWarnTime, opID)
	oc.mu.Unlock()

	metrics.OperatorCount.WithLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), op.Type(), common.StringMode(oc.mode)).Dec()
	metrics.OperatorDuration.WithLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), op.Type(), common.StringMode(oc.mode)).Observe(time.Since(item.CreatedAt).Seconds())
	log.Info("operator finished",
		zap.String("role", oc.role),
		zap.Stringer("changefeedID", oc.changefeedID),
		zap.String("operatorID", opID.String()),
		zap.String("operator", op.String()))
}

func (oc *Controller) cancelOperator(opID common.DispatcherID) {
	oc.mu.RLock()
	item, ok := oc.operators[opID]
	oc.mu.RUnlock()
	if !ok {
		return
	}
	item.OP.OnTaskRemoved()
	oc.finalizeOperator(item, opID)
}

func (oc *Controller) removeReplicaSet(op *removeDispatcherOperator) {
	oc.mu.RLock()
	old, ok := oc.operators[op.ID()]
	oc.mu.RUnlock()
	if ok {
		log.Info("replica set is removed, replace the old one",
			zap.String("role", oc.role),
			zap.Stringer("changefeedID", oc.changefeedID),
			zap.String("replicaSet", old.OP.ID().String()),
			zap.String("operator", old.OP.String()))
		old.OP.OnTaskRemoved()
		oc.finalizeOperator(old, op.ID())
	}
	oc.pushOperator(op)
}

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	log.Info("add operator to running queue",
		zap.String("role", oc.role),
		zap.Stringer("changefeedID", oc.changefeedID),
		zap.String("operator", op.String()))
	withTime := operator.NewOperatorWithTime(op, time.Now())

	oc.mu.Lock()
	oc.operators[op.ID()] = withTime
	oc.mu.Unlock()

	op.Start()
	// Check affected nodes after Start to avoid operators being forced into terminal states
	// before they have initialized their span state. For example, a move operator can mark
	// a span absent on node removal, and a subsequent Start must not bring it back to an
	// invalid scheduling state with an empty node ID.
	oc.checkAffectedNodes(op)

	oc.mu.Lock()
	heap.Push(&oc.runningQueue, withTime)
	oc.mu.Unlock()

	metrics.OperatorCount.WithLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), op.Type(), common.StringMode(oc.mode)).Inc()
	metrics.TotalOperatorCount.WithLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), op.Type(), common.StringMode(oc.mode)).Inc()
}

func (oc *Controller) checkAffectedNodes(op operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus]) {
	aliveNodes := oc.nodeManager.GetAliveNodes()
	for _, nodeID := range op.AffectedNodes() {
		if _, ok := aliveNodes[nodeID]; !ok {
			op.OnNodeRemove(nodeID)
		}
	}
}

func (oc *Controller) NewMoveOperator(replicaSet *replica.SpanReplication, origin, dest node.ID) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	return NewMoveDispatcherOperator(oc.spanController, replicaSet, origin, dest)
}

func checkMergeOperator(affectedReplicaSets []*replica.SpanReplication) bool {
	if len(affectedReplicaSets) < 2 {
		log.Info("affectedReplicaSets is less than 2, skip merge",
			zap.Any("affectedReplicaSets", affectedReplicaSets))
		return false
	}

	affectedSpans := make([]*heartbeatpb.TableSpan, 0, len(affectedReplicaSets))
	for _, replicaSet := range affectedReplicaSets {
		affectedSpans = append(affectedSpans, replicaSet.Span)
	}

	prevTableSpan := affectedSpans[0]
	nodeID := affectedReplicaSets[0].GetNodeID()
	for idx := 1; idx < len(affectedSpans); idx++ {
		currentTableSpan := affectedSpans[idx]
		if !common.IsTableSpanConsecutive(prevTableSpan, currentTableSpan) {
			log.Info("affectedReplicaSets is not consecutive, skip merge", zap.String("prevTableSpan", common.FormatTableSpan(prevTableSpan)), zap.String("currentTableSpan", common.FormatTableSpan(currentTableSpan)))
			return false
		}
		prevTableSpan = currentTableSpan
		if affectedReplicaSets[idx].GetNodeID() != nodeID {
			log.Info("affectedReplicaSets is not in the same node, skip merge", zap.Any("affectedReplicaSets", affectedReplicaSets))
			return false
		}
	}
	return true
}

// AddMergeOperator creates a merge operator, which merge consecutive replica sets.
// We need create a mergeOperator for the new replicaset, and create len(affectedReplicaSets) empty operator
// to occupy these replica set not evolve other scheduling among merging.
func (oc *Controller) AddMergeOperator(
	affectedReplicaSets []*replica.SpanReplication,
) operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	if !checkMergeOperator(affectedReplicaSets) {
		return nil
	}

	operators := make([]operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(affectedReplicaSets))
	for _, replicaSet := range affectedReplicaSets {
		operator := NewOccupyDispatcherOperator(oc.spanController, replicaSet)
		ret := oc.AddOperator(operator)
		if ret {
			operators = append(operators, operator)
		} else {
			log.Error("failed to add occupy dispatcher operator",
				zap.Stringer("changefeedID", oc.changefeedID),
				zap.Int64("group", replicaSet.GetGroupID()),
				zap.String("span", common.FormatTableSpan(replicaSet.Span)),
				zap.String("operator", operator.String()))
			for _, op := range operators {
				oc.cancelOperator(op.ID())
			}
			return nil
		}
	}

	mergeOperator := NewMergeDispatcherOperator(oc.spanController, affectedReplicaSets, operators)
	ret := oc.AddOperator(mergeOperator)
	if !ret {
		log.Error("failed to add merge dispatcher operator",
			zap.Stringer("changefeedID", oc.changefeedID),
			zap.Any("mergeSpans", affectedReplicaSets),
			zap.String("operator", mergeOperator.String()))
		for _, op := range operators {
			oc.cancelOperator(op.ID())
		}
		oc.spanController.RemoveReplicatingSpan(mergeOperator.newReplicaSet)
		return nil
	}
	log.Info("add merge operator",
		zap.String("role", oc.role),
		zap.Stringer("changefeedID", oc.changefeedID),
		zap.Int("affectedReplicaSets", len(affectedReplicaSets)),
	)
	return mergeOperator
}

func (oc *Controller) GetAllOperators() []operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus] {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	operators := make([]operator.Operator[common.DispatcherID, *heartbeatpb.TableSpanStatus], 0, len(oc.operators))

	for _, op := range oc.operators {
		operators = append(operators, op.OP)
	}
	return operators
}

func (oc *Controller) Close() {
	opTypes := []string{"occupy", "merge", "add", "remove", "move", "split", "merge"}

	for _, opType := range opTypes {
		metrics.OperatorCount.DeleteLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), opType, common.StringMode(oc.mode))
		metrics.TotalOperatorCount.DeleteLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), opType, common.StringMode(oc.mode))
		metrics.OperatorDuration.DeleteLabelValues(common.DefaultKeyspaceNamme, oc.changefeedID.Name(), opType, common.StringMode(oc.mode))
	}
}

// =========== following func only for test ===========
func (oc *Controller) RemoveOp(id common.DispatcherID) {
	oc.mu.Lock()
	defer oc.mu.Unlock()
	delete(oc.operators, id)
	delete(oc.lastWarnTime, id)
}
