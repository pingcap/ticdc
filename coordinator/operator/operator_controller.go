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
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/scheduler/operator"
	"github.com/pingcap/ticdc/server/watcher"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const operatorEpochBumpTimeout = 10 * time.Second

// Controller is the operator controller, it manages all operators.
// And the Controller is responsible for the execution of the operator.
type Controller struct {
	mu sync.RWMutex

	role         string
	changefeedDB *changefeed.ChangefeedDB
	operators    map[common.ChangeFeedID]*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	// epochBumping reserves an owner-changing operator slot while its epoch bump
	// runs outside mu, so another add or move cannot persist a newer epoch first.
	epochBumping  map[common.ChangeFeedID]struct{}
	runningQueue  operator.OperatorQueue[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]
	batchSize     int
	messageCenter messaging.MessageCenter
	selfNode      *node.Info
	backend       changefeed.Backend
	pdClient      pd.Client
	nodeManger    *watcher.NodeManager
}

func NewOperatorController(
	selfNode *node.Info,
	db *changefeed.ChangefeedDB,
	backend changefeed.Backend,
	pdClient pd.Client,
	batchSize int,
) *Controller {
	oc := &Controller{
		role:          "coordinator",
		operators:     make(map[common.ChangeFeedID]*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]),
		epochBumping:  make(map[common.ChangeFeedID]struct{}),
		runningQueue:  make(operator.OperatorQueue[common.ChangeFeedID, *heartbeatpb.MaintainerStatus], 0),
		messageCenter: appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		batchSize:     batchSize,
		changefeedDB:  db,
		selfNode:      selfNode,
		backend:       backend,
		pdClient:      pdClient,
		nodeManger:    appcontext.GetService[*watcher.NodeManager](watcher.NodeManagerName),
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

		oc.mu.RLock()
		msg := r.Schedule()
		oc.mu.RUnlock()

		if msg != nil {
			err := oc.messageCenter.SendCommand(msg)
			if err != nil {
				log.Warn("send command to maintainer failed",
					zap.String("role", oc.role),
					zap.String("operator", r.String()),
					zap.Error(err))
			} else {
				log.Info("send command to maintainer",
					zap.String("role", oc.role),
					zap.String("operator", r.String()))
			}
		}
		executedItem++
		if executedItem >= oc.batchSize {
			return time.Now().Add(time.Millisecond * 50)
		}
	}
}

// AddOperator adds an operator to the controller, if the operator already exists, return false.
func (oc *Controller) AddOperator(op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]) bool {
	oc.mu.Lock()
	cf, ok := oc.precheckAddOperatorLocked(op)
	if !ok {
		oc.mu.Unlock()
		return false
	}
	shouldBumpEpoch := oc.shouldBumpChangefeedEpoch(op)
	if !shouldBumpEpoch {
		oc.pushOperator(op)
		oc.mu.Unlock()
		return true
	}
	oc.epochBumping[op.ID()] = struct{}{}
	// Epoch bump may call PD and etcd, so keep it outside oc.mu. Recheck the
	// operator slot before pushing because stop or another operator may win
	// while the bump is in flight.
	oc.mu.Unlock()

	info, err := oc.bumpChangefeedEpoch(cf)
	oc.mu.Lock()
	defer oc.mu.Unlock()
	delete(oc.epochBumping, op.ID())
	if err != nil {
		log.Warn("add operator failed, cannot bump changefeed epoch",
			zap.String("role", oc.role),
			zap.String("operator", op.String()),
			zap.Stringer("changefeed", op.ID()),
			zap.Error(err))
		return false
	}

	if !oc.recheckAddOperatorLocked(op, cf) {
		return false
	}
	cf.SetInfo(info)
	oc.pushOperator(op)
	return true
}

func (oc *Controller) precheckAddOperatorLocked(
	op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus],
) (*changefeed.Changefeed, bool) {
	if pre, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists",
			zap.String("role", oc.role),
			zap.Stringer("operator", op), zap.Stringer("previousOperator", pre.OP))
		return nil, false
	}
	if _, ok := oc.epochBumping[op.ID()]; ok {
		log.Info("add operator failed, epoch bump already in progress",
			zap.String("role", oc.role),
			zap.Stringer("operator", op))
		return nil, false
	}
	cf := oc.changefeedDB.GetByID(op.ID())
	if cf == nil {
		log.Warn("add operator failed, changefeed not found",
			zap.String("role", oc.role),
			zap.String("operator", op.String()))
		return nil, false
	}
	return cf, true
}

func (oc *Controller) recheckAddOperatorLocked(
	op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus],
	cf *changefeed.Changefeed,
) bool {
	if pre, ok := oc.operators[op.ID()]; ok {
		log.Info("add operator failed, operator already exists after epoch bump",
			zap.String("role", oc.role),
			zap.Stringer("operator", op), zap.Stringer("previousOperator", pre.OP))
		return false
	}
	current := oc.changefeedDB.GetByID(op.ID())
	if current == nil {
		log.Warn("add operator failed, changefeed not found after epoch bump",
			zap.String("role", oc.role),
			zap.String("operator", op.String()))
		return false
	}
	if current != cf {
		log.Warn("add operator failed, changefeed changed after epoch bump",
			zap.String("role", oc.role),
			zap.String("operator", op.String()))
		return false
	}
	return true
}

func (oc *Controller) shouldBumpChangefeedEpoch(
	op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus],
) bool {
	return requiresNewMaintainerOwnership(op) && oc.pdClient != nil && oc.backend != nil
}

func (oc *Controller) bumpChangefeedEpoch(cf *changefeed.Changefeed) (*config.ChangeFeedInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), operatorEpochBumpTimeout)
	defer cancel()

	epoch := pdutil.GenerateChangefeedEpoch(ctx, oc.pdClient)
	return oc.backend.BumpChangefeedEpoch(
		ctx,
		cf.ID,
		epoch,
		changefeed.EpochBumpOptions{},
	)
}

func requiresNewMaintainerOwnership(
	op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus],
) bool {
	switch op.(type) {
	case *AddMaintainerOperator, *MoveMaintainerOperator:
		return true
	default:
		return false
	}
}

// StopChangefeed stop changefeed when the changefeed is stopped/removed.
// if remove is true, it will remove the changefeed from the changefeed DB
// if remove is false, it only marks the changefeed stopped in changefeed DB, so we will not schedule the changefeed again
func (oc *Controller) StopChangefeed(_ context.Context, cfID common.ChangeFeedID, removed bool) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	return oc.stopChangefeed(cfID, removed, 0, false)
}

// StopChangefeedWithMaintainerEpoch stops the current maintainer with the epoch
// it already owns, even if the in-memory changefeed has advanced to a newer
// ownership epoch.
func (oc *Controller) StopChangefeedWithMaintainerEpoch(
	_ context.Context,
	cfID common.ChangeFeedID,
	removed bool,
	maintainerEpoch uint64,
) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	return oc.stopChangefeed(cfID, removed, maintainerEpoch, true)
}

// StopRemoteMaintainerWithMaintainerEpoch stops a reported maintainer without
// changing the local changefeed placement. It is used during coordinator
// bootstrap when the reported maintainer is from an old ownership epoch and
// must occupy the operator slot until the old owner stops.
func (oc *Controller) StopRemoteMaintainerWithMaintainerEpoch(
	cfID common.ChangeFeedID,
	nodeID node.ID,
	removed bool,
	maintainerEpoch uint64,
) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	keyspaceID := common.DefaultKeyspaceID
	changefeed := oc.changefeedDB.GetByID(cfID)
	if changefeed != nil {
		keyspaceID = changefeed.GetKeyspaceID()
	}
	return oc.pushStopChangefeedOperator(keyspaceID, cfID, nodeID, removed, maintainerEpoch)
}

func (oc *Controller) stopChangefeed(
	cfID common.ChangeFeedID,
	removed bool,
	maintainerEpoch uint64,
	hasMaintainerEpoch bool,
) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	changefeed := oc.changefeedDB.GetByID(cfID)
	keyspaceID := common.DefaultKeyspaceID
	if changefeed != nil {
		keyspaceID = changefeed.GetKeyspaceID()
		if !hasMaintainerEpoch {
			maintainerEpoch = changefeed.GetInfo().Epoch
		}
	}

	var originNode node.ID
	var originEpoch uint64
	var useOriginEpoch bool
	if !hasMaintainerEpoch {
		originNode, originEpoch, useOriginEpoch = oc.moveOriginStopTargetLocked(cfID)
	}

	scheduledNode := oc.changefeedDB.StopByChangefeedID(cfID, removed)
	if useOriginEpoch {
		scheduledNode = originNode
		maintainerEpoch = originEpoch
	}
	if scheduledNode == "" {
		log.Info("changefeed is not scheduled, try stop maintainer using coordinator node",
			zap.String("role", oc.role),
			zap.Bool("removed", removed),
			zap.String("changefeed", cfID.Name()))
		scheduledNode = oc.selfNode.ID
	}

	return oc.pushStopChangefeedOperator(keyspaceID, cfID, scheduledNode, removed, maintainerEpoch)
}

// moveOriginStopTargetLocked returns the origin maintainer stop target for an
// in-flight move. The caller must hold oc.mu.
func (oc *Controller) moveOriginStopTargetLocked(cfID common.ChangeFeedID) (node.ID, uint64, bool) {
	old, ok := oc.operators[cfID]
	if !ok {
		return "", 0, false
	}
	moveOp, ok := old.OP.(*MoveMaintainerOperator)
	if !ok {
		return "", 0, false
	}
	return moveOp.originStopTarget()
}

// pushStopChangefeedOperator pushes a stop changefeed operator to the controller.
// it checks if the operator already exists, if exists, it will replace the old one.
// if the old operator is the removing operator, it will skip this operator.
func (oc *Controller) pushStopChangefeedOperator(
	keyspaceID uint32,
	cfID common.ChangeFeedID,
	nodeID node.ID,
	remove bool,
	maintainerEpoch uint64,
) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	op := NewStopChangefeedOperator(keyspaceID, cfID, nodeID, oc.selfNode.ID, oc.backend, remove, maintainerEpoch)
	if old, ok := oc.operators[cfID]; ok {
		oldStop, ok := old.OP.(*StopChangefeedOperator)
		if ok {
			if oldStop.changefeedIsRemoved {
				log.Info("changefeed is in removing progress, skip the stop operator",
					zap.String("role", oc.role),
					zap.String("changefeed", cfID.Name()))
				return oldStop
			}
		}
		log.Info("changefeed is stopped, replace the old one",
			zap.String("role", oc.role),
			zap.String("changefeed", cfID.Name()),
			zap.String("operator", old.OP.String()))
		old.OP.OnTaskRemoved()
		old.OP.PostFinish()
		old.IsRemoved.Store(true)
		delete(oc.operators, old.OP.ID())
	}
	oc.pushOperator(op)
	return op
}

func (oc *Controller) UpdateOperatorStatus(id common.ChangeFeedID, from node.ID,
	status *heartbeatpb.MaintainerStatus,
) {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	op, ok := oc.operators[id]
	if ok {
		op.OP.Check(from, status)
	}
}

// OnNodeRemoved is called when a node is offline,
// the controller will mark all maintainers on the node as absent if no operator is handling it,
// then the controller will notify all operators.
func (oc *Controller) OnNodeRemoved(n node.ID) {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	for _, cf := range oc.changefeedDB.GetByNodeID(n) {
		_, ok := oc.operators[cf.ID]
		if !ok {
			oc.changefeedDB.MarkMaintainerAbsent(cf)
		}
	}
	for _, op := range oc.operators {
		op.OP.OnNodeRemove(n)
	}
}

// GetOperator returns the operator by id.
func (oc *Controller) GetOperator(id common.ChangeFeedID) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	if op, ok := oc.operators[id]; !ok {
		return nil
	} else {
		return op.OP
	}
}

// HasOperator returns true if the operator with the ChangeFeedDisplayName exists in the controller.
func (oc *Controller) HasOperator(dispName common.ChangeFeedDisplayName) bool {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	for id := range oc.operators {
		if id.DisplayName == dispName {
			return true
		}
	}
	return false
}

// OperatorSize returns the number of operators in the controller.
func (oc *Controller) OperatorSize() int {
	oc.mu.RLock()
	defer oc.mu.RUnlock()
	return len(oc.operators)
}

// CountMoveMaintainerOperatorsFromNodes returns the number of in-flight move
// operators whose origin is one of the given nodes.
func (oc *Controller) CountMoveMaintainerOperatorsFromNodes(origins []node.ID) int {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	if len(origins) == 0 {
		return 0
	}
	originSet := make(map[node.ID]struct{}, len(origins))
	for _, origin := range origins {
		originSet[origin] = struct{}{}
	}

	count := 0
	for _, op := range oc.operators {
		moveOp, ok := op.OP.(*MoveMaintainerOperator)
		if !ok {
			continue
		}
		if _, ok := originSet[moveOp.OriginNode()]; ok {
			count++
		}
	}
	return count
}

// CountOperatorsInvolvingNode returns the number of in-flight operators whose
// affected nodes include n.
func (oc *Controller) CountOperatorsInvolvingNode(n node.ID) int {
	oc.mu.RLock()
	defer oc.mu.RUnlock()

	count := 0
	for _, op := range oc.operators {
		for _, affected := range op.OP.AffectedNodes() {
			if affected == n {
				count++
				break
			}
		}
	}
	return count
}

// HasOperatorInvolvingNode returns true if any in-flight operator affects n.
func (oc *Controller) HasOperatorInvolvingNode(n node.ID) bool {
	return oc.CountOperatorsInvolvingNode(n) > 0
}

// pollQueueingOperator returns the operator need to be executed,
// "next" is true to indicate that it may exist in next attempt,
// and false is the end for the poll.
func (oc *Controller) pollQueueingOperator() (operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus], bool) {
	oc.mu.Lock()
	defer oc.mu.Unlock()

	if oc.runningQueue.Len() == 0 {
		return nil, false
	}
	item := heap.Pop(&oc.runningQueue).(*operator.OperatorWithTime[common.ChangeFeedID, *heartbeatpb.MaintainerStatus])
	if item.IsRemoved.Load() {
		return nil, true
	}
	op := item.OP
	opID := item.OP.ID()
	// always call the PostFinish method to ensure the operator is cleaned up by itself.
	if op.IsFinished() {
		op.PostFinish()
		item.IsRemoved.Store(true)
		delete(oc.operators, opID)
		metrics.CoordinatorFinishedOperatorCount.WithLabelValues(op.Type()).Inc()
		metrics.CoordinatorOperatorDuration.WithLabelValues(op.Type()).Observe(time.Since(item.CreatedAt).Seconds())
		log.Info("operator finished",
			zap.String("role", oc.role),
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

// pushOperator add an operator to the controller queue.
func (oc *Controller) pushOperator(op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]) {
	oc.checkAffectedNodes(op)
	log.Info("add operator to running queue",
		zap.String("role", oc.role),
		zap.String("operator", op.String()))
	opWithTime := operator.NewOperatorWithTime(op, time.Now())
	oc.operators[op.ID()] = opWithTime
	op.Start()
	heap.Push(&oc.runningQueue, opWithTime)
	metrics.CoordinatorCreatedOperatorCount.WithLabelValues(op.Type()).Inc()
}

func (oc *Controller) checkAffectedNodes(op operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus]) {
	aliveNodes := oc.nodeManger.GetAliveNodes()
	for _, n := range op.AffectedNodes() {
		if _, ok := aliveNodes[n]; !ok {
			op.OnNodeRemove(n)
		}
	}
}

func (oc *Controller) NewAddMaintainerOperator(cf *changefeed.Changefeed, dest node.ID) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	return NewAddMaintainerOperator(oc.changefeedDB, cf, dest)
}

func (oc *Controller) NewMoveMaintainerOperator(cf *changefeed.Changefeed, origin, dest node.ID) operator.Operator[common.ChangeFeedID, *heartbeatpb.MaintainerStatus] {
	return NewMoveMaintainerOperator(oc.changefeedDB, cf, origin, dest)
}
