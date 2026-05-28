// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"context"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/coordinator/drain"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const dispatcherDrainTargetResendIntvl = 5 * time.Second

type drainSession struct {
	target node.ID
	epoch  uint64

	// targetSyncNodes is the set of nodes that coordinator explicitly keeps in
	// sync with the active dispatcher drain target for this session. It starts
	// from the frozen alive-node set and may grow when compatible later-join
	// nodes finish bootstrap while the session is still active.
	targetSyncNodes map[node.ID]struct{}

	// trackedChangefeeds is the frozen set of running changefeeds that were
	// relevant to this drain session when it started. Drain-aware scheduling must
	// prevent new work from landing on the target, so repeated API polls can
	// reuse this set instead of rescanning all replicating changefeeds.
	trackedChangefeeds []common.ChangeFeedID

	// pendingStatus is the frozen baseline of running changefeeds that had not
	// yet acknowledged this drain epoch when the session was created.
	// The set only shrinks over time during one drain session.
	pendingStatus map[common.ChangeFeedID]struct{}

	dirty    bool
	lastSent time.Time
}

type drainClearState struct {
	// target is the node whose drain target is being cleared at this epoch.
	target node.ID
	epoch  uint64

	// pendingNodes tracks manager-level clear acknowledgements from nodes that
	// were alive when the clear was issued.
	pendingNodes map[node.ID]struct{}

	// dirty/lastSent follow the same resend contract as an active drain target.
	// Clear is not fire-and-forget because losing the empty-target broadcast
	// would leave some nodes with a stale local drain target indefinitely.
	dirty    bool
	lastSent time.Time
}

type drainCompletedState struct {
	target node.ID
}

type drainTargetSnapshot struct {
	target node.ID
	epoch  uint64
}

func drainTargetSnapshotFromBootstrap(resp *heartbeatpb.CoordinatorBootstrapResponse) drainTargetSnapshot {
	if resp == nil {
		return drainTargetSnapshot{}
	}
	return drainTargetSnapshot{
		target: node.ID(resp.GetDispatcherDrainTargetNodeId()),
		epoch:  resp.GetDispatcherDrainTargetEpoch(),
	}
}

func drainTargetSnapshotFromHeartbeat(hb *heartbeatpb.NodeHeartbeat) drainTargetSnapshot {
	if hb == nil {
		return drainTargetSnapshot{}
	}
	return drainTargetSnapshot{
		target: node.ID(hb.GetDispatcherDrainTargetNodeId()),
		epoch:  hb.GetDispatcherDrainTargetEpoch(),
	}
}

func (s drainTargetSnapshot) hasTarget() bool {
	return !s.target.IsEmpty() && s.epoch != 0
}

// DrainNode starts or continues a best-effort drain for one target node.
// It first checks the drain capability learned from bootstrap for every alive
// maintainer manager:
//   - unknown capability keeps returning non-zero while coordinator waits for
//     bootstrap to complete for that node
//   - any legacy capability bypasses the new orchestration and falls back to
//     the historical hard-restart behavior used during mixed-version rolling patch
//
// Supported targets then use coordinator-driven orchestration: it ensures an active
// target epoch, broadcasts the drain target, requests liveness transition, and
// evaluates a one-shot drain observation.
//
// A zero remaining value means all coordinator-observed drain signals have
// converged. It is best-effort because the active session uses an in-memory
// snapshot of relevant changefeeds rather than a cluster-wide persisted proof.
func (c *Controller) DrainNode(ctx context.Context, target node.ID) (int, error) {
	activeTarget, activeEpoch, hasSession := c.getDispatcherDrainTarget()
	if c.nodeManager.GetNodeInfo(target) == nil {
		if c.isCompletedDrainTarget(target) {
			return 0, nil
		}
		if hasSession && activeTarget == target {
			return c.observeRemovedActiveDrainTarget(target, activeEpoch), nil
		}
		return 0, errors.ErrCaptureNotExist.GenWithStackByArgs(target)
	}
	// Drain completion relies on in-memory changefeed state built by coordinator bootstrap.
	// Before bootstrap is complete, always return non-zero remaining to avoid premature zero.
	if c.initialized == nil || !c.initialized.Load() {
		log.Info("drain waiting for coordinator bootstrap",
			zap.Stringer("targetNodeID", target))
		return 1, nil
	}

	if !hasSession {
		blockingNodeID, blockingVersion, capabilityObserved, hasBlocker := c.findDrainProtocolBlocker()
		if hasBlocker && !capabilityObserved {
			log.Info("drain waiting for cluster drain capability observation",
				zap.Stringer("targetNodeID", target),
				zap.Stringer("blockingNodeID", blockingNodeID))
			return 1, nil
		}
		if hasBlocker && blockingVersion == 0 {
			message := "drain target does not support coordinator driven drain protocol, fall back to legacy hard restart"
			if blockingNodeID != target {
				message = "drain cluster contains legacy peer that cannot participate in coordinator driven drain, fall back to legacy hard restart"
			}
			log.Info(message,
				zap.Stringer("targetNodeID", target),
				zap.Stringer("blockingNodeID", blockingNodeID),
				zap.Uint32("blockingDrainProtocolVersion", blockingVersion))
			// A zero remaining value here means coordinator-driven drain is intentionally skipped
			// because at least one alive node cannot run the protocol. The caller is
			// expected to continue with the legacy hard-restart removal flow.
			return 0, nil
		}
	}
	// ensureDispatcherDrainTarget creates the first strict session or reuses the
	// existing frozen session even if node membership changes later.
	targetEpoch, err := c.ensureDispatcherDrainTarget(ctx, target)
	if err != nil {
		return 0, err
	}
	c.syncDrainSchedulingPolicy()
	c.maybeBroadcastDispatcherDrainTarget(true)

	// Drain requests are idempotent. Reissuing the request on each poll keeps
	// the liveness retry loop advancing even if a previous command was dropped.
	c.drainController.RequestDrain(target)

	observation := c.observeDrainNode(target, targetEpoch)
	completionObserved := isBestEffortDrainComplete(
		observation.nodeState,
		observation.drainingObserved,
		observation.stoppingObserved,
		observation.remaining,
	)

	if completionObserved {
		log.Info("drain completion observed, waiting for node removal to clear drain target",
			zap.Stringer("targetNodeID", target),
			zap.Uint64("targetEpoch", targetEpoch))
		return 0, nil
	}

	log.Info("drain completion not yet observed",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", targetEpoch),
		zap.String("nodeState", drainStateString(observation.nodeState)),
		zap.Bool("drainingObserved", observation.drainingObserved),
		zap.Bool("stoppingObserved", observation.stoppingObserved),
		zap.Int("maintainersOnTarget", observation.maintainersOnTarget),
		zap.Int("inflightOpsInvolvingTarget", observation.inflightOpsInvolvingTarget),
		zap.Int("dispatcherCountOnTarget", observation.dispatcherCountOnTarget),
		zap.Int("targetInflightDrainMoveCount", observation.targetInflightDrainMoveCount),
		zap.Int("pendingStatusCount", observation.pendingStatusCount),
		zap.Int("remaining", observation.remaining))
	return ensureDrainRemainingNonZero(observation.remaining), nil
}

// observeRemovedActiveDrainTarget preserves polling semantics during the small
// window where membership no longer contains the target but RemoveNode has not
// closed the matching active session yet. It must not send new commands or
// clear session state; RemoveNode remains the owner of those side effects.
func (c *Controller) observeRemovedActiveDrainTarget(target node.ID, epoch uint64) int {
	observation := c.observeDrainNode(target, epoch)
	completionObserved := isBestEffortDrainComplete(
		observation.nodeState,
		observation.drainingObserved,
		observation.stoppingObserved,
		observation.remaining,
	)
	if completionObserved {
		log.Info("drain completion observed for removed active target",
			zap.Stringer("targetNodeID", target),
			zap.Uint64("targetEpoch", epoch))
		return 0
	}

	log.Info("removed active drain target completion not yet observed",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch),
		zap.String("nodeState", drainStateString(observation.nodeState)),
		zap.Bool("drainingObserved", observation.drainingObserved),
		zap.Bool("stoppingObserved", observation.stoppingObserved),
		zap.Int("maintainersOnTarget", observation.maintainersOnTarget),
		zap.Int("inflightOpsInvolvingTarget", observation.inflightOpsInvolvingTarget),
		zap.Int("dispatcherCountOnTarget", observation.dispatcherCountOnTarget),
		zap.Int("targetInflightDrainMoveCount", observation.targetInflightDrainMoveCount),
		zap.Int("pendingStatusCount", observation.pendingStatusCount),
		zap.Int("remaining", observation.remaining))
	return ensureDrainRemainingNonZero(observation.remaining)
}

// findDrainProtocolBlocker returns the first alive node that still
// prevents coordinator-driven drain orchestration from being safe cluster-wide. Unknown
// capability has higher priority than legacy fallback so bootstrap races never
// silently degrade to hard-restart behavior.
func (c *Controller) findDrainProtocolBlocker() (node.ID, uint32, bool, bool) {
	var legacyBlocker node.ID
	for id := range c.nodeManager.GetAliveNodes() {
		version, observed := c.drainController.GetDrainProtocolVersion(id)
		if !observed {
			return id, 0, false, true
		}
		if !heartbeatpb.SupportsCoordinatorDrivenDrain(version) && legacyBlocker.IsEmpty() {
			legacyBlocker = id
		}
	}
	if legacyBlocker.IsEmpty() {
		return "", 0, false, false
	}
	return legacyBlocker, 0, true, true
}

type drainNodeObservation struct {
	maintainersOnTarget        int
	inflightOpsInvolvingTarget int
	dispatcherCountOnTarget    int
	// targetInflightDrainMoveCount is the sum of maintainer-reported dispatcher
	// move operators still draining work away from the target node.
	targetInflightDrainMoveCount int
	// pendingStatusCount is the number of running changefeeds not converged to the active target epoch.
	pendingStatusCount int
	// remaining is the max of all workload dimensions used by drain completion gating.
	remaining        int
	nodeState        drain.State
	drainingObserved bool
	stoppingObserved bool
}

func (c *Controller) observeDrainNode(target node.ID, epoch uint64) drainNodeObservation {
	observation := drainNodeObservation{
		maintainersOnTarget:        len(c.changefeedDB.GetByNodeID(target)),
		inflightOpsInvolvingTarget: c.operatorController.CountOperatorsInvolvingNode(target),
	}
	observation.dispatcherCountOnTarget, observation.targetInflightDrainMoveCount = c.aggregateDrainTargetProgress(target, epoch)
	observation.pendingStatusCount = c.collectDrainPendingStatus(target, epoch)
	observation.remaining = drainRemainingEstimate(
		observation.maintainersOnTarget,
		observation.inflightOpsInvolvingTarget,
		observation.dispatcherCountOnTarget,
		observation.targetInflightDrainMoveCount,
		observation.pendingStatusCount,
	)

	_, observation.drainingObserved, observation.stoppingObserved = c.drainController.GetStatus(target)
	observation.nodeState = c.drainController.GetState(target)
	return observation
}

// advanceActiveDrainLiveness retries liveness progression only for the current
// active drain session target. Without an active session coordinator must not
// continue an inherited drain based on stale heartbeats alone.
func (c *Controller) advanceActiveDrainLiveness() {
	c.syncDrainSchedulingPolicy()
	if c.initialized == nil || !c.initialized.Load() {
		return
	}

	target, epoch, ok := c.getDispatcherDrainTarget()
	if !ok {
		return
	}

	c.drainController.AdvanceLiveness(
		func(id node.ID) bool {
			return id == target
		},
		func(id node.ID) bool {
			return id == target && c.isDrainReadyToStop(target, epoch)
		},
	)
}

// syncDrainSchedulingPolicy keeps coordinator schedulers conservative after
// failover. When coordinator has no active drain session but still observes
// draining or stopping nodes, scheduling must pause until a new session is
// recreated or those observations disappear.
func (c *Controller) syncDrainSchedulingPolicy() {
	if c.drainController == nil {
		return
	}

	_, _, hasSession := c.getDispatcherDrainTarget()
	frozen := !hasSession && len(c.drainController.GetDrainingOrStoppingNodes()) > 0
	c.drainController.SetSchedulingFrozen(frozen)
}

func (c *Controller) isDrainReadyToStop(target node.ID, epoch uint64) bool {
	if !c.hasActiveDrainSession(target, epoch) {
		return false
	}

	observation := c.observeDrainNode(target, epoch)
	if !c.hasActiveDrainSession(target, epoch) {
		return false
	}

	return observation.nodeState != drain.StateUnknown &&
		observation.drainingObserved &&
		observation.remaining == 0
}

func (c *Controller) hasActiveDrainSession(target node.ID, epoch uint64) bool {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	return c.drainSession != nil &&
		c.drainSession.target == target &&
		c.drainSession.epoch == epoch
}

// collectDrainPendingStatus advances the frozen pending baseline for the active
// drain session and returns how many changefeeds have not yet reported the
// active drain target epoch.
func (c *Controller) collectDrainPendingStatus(target node.ID, epoch uint64) int {
	if target.IsEmpty() || epoch == 0 {
		return 0
	}

	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	session := c.drainSession
	if session == nil || session.target != target || session.epoch != epoch {
		return 0
	}

	if len(session.pendingStatus) == 0 {
		return 0
	}

	for id := range session.pendingStatus {
		cf := c.changefeedDB.GetByID(id)
		if !isDrainStatusConvergenceRelevant(cf) {
			// Removed or non-running changefeeds should not block drain status convergence.
			delete(session.pendingStatus, id)
			continue
		}
		status := cf.GetStatus()
		if status != nil {
			progress := status.GetDrainProgress()
			if progress != nil && progress.GetTargetNodeId() == target.String() && progress.GetTargetEpoch() == epoch {
				delete(session.pendingStatus, id)
			}
		}
	}

	return len(session.pendingStatus)
}

// snapshotDrainTrackedChangefeeds captures the running changefeeds that are
// relevant to this drain session. The returned slice is frozen at session start
// so repeated API polls do not need to rescan the full replicating set.
func (c *Controller) snapshotDrainTrackedChangefeeds() []common.ChangeFeedID {
	cfs := c.changefeedDB.GetReplicating()
	snapshot := make([]common.ChangeFeedID, 0, len(cfs))
	for _, cf := range cfs {
		if !isDrainStatusConvergenceRelevant(cf) {
			continue
		}
		snapshot = append(snapshot, cf.ID)
	}
	return snapshot
}

// snapshotDrainParticipants freezes the alive nodes that participate in one
// best-effort drain session. Later joins must not expand the completion
// estimate, but compatible peers may still be added to the target-sync set so
// they can safely observe and clear the drain target.
func (c *Controller) snapshotDrainParticipants() map[node.ID]struct{} {
	return utils.SliceToSet(c.nodeManager.GetAliveNodeIDs())
}

// isDrainStatusConvergenceRelevant returns whether a changefeed should
// participate in drain status convergence checks.
func isDrainStatusConvergenceRelevant(cf *changefeed.Changefeed) bool {
	if cf == nil {
		return false
	}
	info := cf.GetInfo()
	return info != nil && shouldRunChangefeed(info.State)
}

// ensureDispatcherDrainTarget creates or reuses the single active drain session.
// A new session uses a PD-allocated TSO as the drain epoch so coordinator
// failover and local clock rollback cannot make a node reject the target as
// stale. It rejects requests for a different target while one is active.
func (c *Controller) ensureDispatcherDrainTarget(ctx context.Context, target node.ID) (uint64, error) {
	c.drainSessionMu.Lock()
	if c.drainSession != nil {
		defer c.drainSessionMu.Unlock()
		if c.drainSession.target == target {
			return c.drainSession.epoch, nil
		}
		return 0, errors.ErrSchedulerRequestFailed.GenWithStackByArgs(
			"drain already in progress on capture " + c.drainSession.target.String())
	}
	c.drainSessionMu.Unlock()

	// Allocate the epoch outside drainSessionMu so PD latency does not block
	// active-session polling, broadcast retries, or node-change cleanup.
	physical, logical, err := c.pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	epoch := oracle.ComposeTS(physical, logical)

	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()
	if c.drainSession != nil {
		if c.drainSession.target == target {
			return c.drainSession.epoch, nil
		}
		return 0, errors.ErrSchedulerRequestFailed.GenWithStackByArgs(
			"drain already in progress on capture " + c.drainSession.target.String())
	}
	if c.nodeManager.GetNodeInfo(target) == nil {
		return 0, errors.ErrCaptureNotExist.GenWithStackByArgs(target)
	}

	trackedChangefeeds := c.snapshotDrainTrackedChangefeeds()
	targetSyncNodes := c.snapshotDrainParticipants()
	pendingStatus := utils.SliceToSet(trackedChangefeeds)
	c.drainClearState = nil
	c.drainSession = &drainSession{
		targetSyncNodes:    targetSyncNodes,
		target:             target,
		epoch:              epoch,
		trackedChangefeeds: trackedChangefeeds,
		pendingStatus:      pendingStatus,
		dirty:              true,
	}
	c.drainController.StartDrainTargetSchedulerGate(target, epoch)
	c.drainCompleted = nil
	log.Info("dispatcher drain target activated",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch))
	return epoch, nil
}

// getDispatcherDrainTarget returns the current active drain target and epoch.
// The boolean return value indicates whether a session exists.
func (c *Controller) getDispatcherDrainTarget() (node.ID, uint64, bool) {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()
	if c.drainSession == nil {
		return "", 0, false
	}
	return c.drainSession.target, c.drainSession.epoch, true
}

func (c *Controller) isCompletedDrainTarget(target node.ID) bool {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	return c.drainCompleted != nil && c.drainCompleted.target == target
}

func (c *Controller) recordCompletedDrainTarget(target node.ID) {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	c.drainCompleted = &drainCompletedState{
		target: target,
	}
}

func (c *Controller) clearCompletedDrainTarget(target node.ID) {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	if c.drainCompleted == nil || c.drainCompleted.target != target {
		return
	}
	c.drainCompleted = nil
}

// clearDispatcherDrainTarget closes the matching active drain session and
// broadcasts an empty target at the same epoch to clear stale local targets.
// It is triggered only after target membership removal confirms the target node
// can no longer receive new coordinator-side assignments.
func (c *Controller) clearDispatcherDrainTarget(target node.ID, epoch uint64) {
	c.drainSessionMu.Lock()
	if c.drainSession == nil || c.drainSession.target != target || c.drainSession.epoch != epoch {
		c.drainSessionMu.Unlock()
		return
	}
	aliveNodes := c.nodeManager.GetAliveNodes()
	pendingNodes := make(map[node.ID]struct{}, len(c.drainSession.targetSyncNodes))
	for id := range c.drainSession.targetSyncNodes {
		if _, ok := aliveNodes[id]; !ok {
			continue
		}
		pendingNodes[id] = struct{}{}
	}
	c.drainSession = nil
	startClearGate := len(pendingNodes) != 0
	if len(pendingNodes) == 0 {
		c.drainClearState = nil
	} else {
		// Freeze only the nodes that coordinator kept synchronized with the
		// active target and that are still alive when the clear is issued.
		// Compatible later-join nodes are included here once they were brought
		// into the target-sync set during the active session.
		c.drainClearState = &drainClearState{
			target:       target,
			epoch:        epoch,
			pendingNodes: pendingNodes,
			dirty:        true,
		}
	}
	c.drainController.SwitchDrainTargetSchedulerGateToClear(target, epoch, pendingNodes)
	c.drainSessionMu.Unlock()

	c.syncDrainSchedulingPolicy()
	log.Info("dispatcher drain target cleared",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch),
		zap.Bool("clearAckRequired", startClearGate))
	c.maybeBroadcastDispatcherDrainTarget(true)
}

// maybeBroadcastDispatcherDrainTarget sends the active drain target or pending
// clear tombstone when forced, dirty, or periodic resend is due.
func (c *Controller) maybeBroadcastDispatcherDrainTarget(force bool) {
	c.drainSessionMu.Lock()
	var (
		target       node.ID
		epoch        uint64
		needSend     bool
		recipients   []node.ID
		sendingClear bool
	)
	switch {
	case c.drainSession != nil:
		target = c.drainSession.target
		epoch = c.drainSession.epoch
		recipients = c.aliveDrainTargetRecipientsLocked(c.drainSession.targetSyncNodes)
		needSend = force ||
			c.drainSession.dirty ||
			time.Since(c.drainSession.lastSent) >= dispatcherDrainTargetResendIntvl
	case c.drainClearState != nil:
		epoch = c.drainClearState.epoch
		sendingClear = true
		recipients = c.aliveDrainTargetRecipientsLocked(c.drainClearState.pendingNodes)
		needSend = force ||
			c.drainClearState.dirty ||
			time.Since(c.drainClearState.lastSent) >= dispatcherDrainTargetResendIntvl
	default:
		c.drainSessionMu.Unlock()
		return
	}
	if !needSend {
		c.drainSessionMu.Unlock()
		return
	}
	c.drainSessionMu.Unlock()

	c.broadcastDispatcherDrainTarget(recipients, target, epoch)

	c.drainSessionMu.Lock()
	if sendingClear {
		if c.drainClearState != nil && c.drainClearState.epoch == epoch {
			c.drainClearState.dirty = false
			c.drainClearState.lastSent = time.Now()
		}
		c.drainSessionMu.Unlock()
		return
	}
	if c.drainSession != nil && c.drainSession.target == target && c.drainSession.epoch == epoch {
		c.drainSession.dirty = false
		c.drainSession.lastSent = time.Now()
	}
	c.drainSessionMu.Unlock()
}

// aliveDrainTargetRecipientsLocked returns the currently alive nodes that are
// still relevant for one target-sync or clear-ack set.
// Caller must hold c.drainSessionMu.
func (c *Controller) aliveDrainTargetRecipientsLocked(nodes map[node.ID]struct{}) []node.ID {
	if len(nodes) == 0 || c.nodeManager == nil {
		return nil
	}
	aliveNodes := c.nodeManager.GetAliveNodes()
	recipients := make([]node.ID, 0, len(nodes))
	for id := range nodes {
		if _, ok := aliveNodes[id]; !ok {
			continue
		}
		recipients = append(recipients, id)
	}
	return recipients
}

// broadcastDispatcherDrainTarget sends SetDispatcherDrainTargetRequest to the
// selected nodes as a best-effort broadcast.
func (c *Controller) broadcastDispatcherDrainTarget(recipients []node.ID, target node.ID, epoch uint64) {
	if epoch == 0 || c.messageCenter == nil || c.nodeManager == nil {
		return
	}

	req := &heartbeatpb.SetDispatcherDrainTargetRequest{
		TargetNodeId: target.String(),
		TargetEpoch:  epoch,
	}
	for _, id := range recipients {
		msg := messaging.NewSingleTargetMessage(id, messaging.MaintainerManagerTopic, req)
		if err := c.messageCenter.SendCommand(msg); err != nil {
			log.Warn("send set dispatcher drain target command failed",
				zap.Stringer("nodeID", id),
				zap.Stringer("targetNodeID", target),
				zap.Uint64("targetEpoch", epoch),
				zap.Error(err))
		}
	}
}

// maybeAddDispatcherDrainSyncNode adds a later-join node into the active
// target-sync set after bootstrap has confirmed it supports the drain
// protocol. It returns true when the session changed and should be broadcast.
func (c *Controller) maybeAddDispatcherDrainSyncNode(nodeID node.ID, version uint32) bool {
	if !heartbeatpb.SupportsCoordinatorDrivenDrain(version) ||
		c.nodeManager == nil ||
		c.nodeManager.GetNodeInfo(nodeID) == nil {
		return false
	}

	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	session := c.drainSession
	if session == nil {
		return false
	}
	if _, ok := session.targetSyncNodes[nodeID]; ok {
		return false
	}

	session.targetSyncNodes[nodeID] = struct{}{}
	session.dirty = true
	return true
}

func (c *Controller) recoverStaleDispatcherDrainTargetFromBootstrap(
	responses map[node.ID]*heartbeatpb.CoordinatorBootstrapResponse,
) bool {
	var stale drainTargetSnapshot
	for _, resp := range responses {
		if resp == nil ||
			!heartbeatpb.SupportsCoordinatorDrivenDrain(resp.GetDrainProtocolVersion()) {
			continue
		}
		snapshot := drainTargetSnapshotFromBootstrap(resp)
		if snapshot.hasTarget() && snapshot.epoch > stale.epoch {
			stale = snapshot
		}
	}
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()
	if c.drainClearState != nil &&
		(!stale.hasTarget() || c.drainClearState.epoch > stale.epoch) {
		return c.createOrReplaceDrainClearStateLocked(
			c.drainClearState.target,
			c.drainClearState.epoch,
			c.snapshotDrainClearParticipants(),
		)
	}
	if !stale.hasTarget() {
		return false
	}
	return c.createOrReplaceDrainClearStateLocked(
		stale.target,
		stale.epoch,
		c.snapshotDrainClearParticipants(),
	)
}

func (c *Controller) observeDispatcherDrainTargetHeartbeat(from node.ID, hb *heartbeatpb.NodeHeartbeat) bool {
	return c.observeStaleDispatcherDrainTargetSnapshot(from, drainTargetSnapshotFromHeartbeat(hb))
}

func (c *Controller) observeStaleDispatcherDrainTargetSnapshot(from node.ID, snapshot drainTargetSnapshot) bool {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	if c.drainSession != nil {
		return false
	}

	clearState := c.drainClearState
	if clearState == nil {
		if !c.isDrainClearParticipant(from) {
			return false
		}
		if !snapshot.hasTarget() {
			return false
		}
		return c.createOrReplaceDrainClearStateLocked(
			snapshot.target,
			snapshot.epoch,
			c.snapshotDrainClearParticipants(),
		)
	}

	if _, ok := clearState.pendingNodes[from]; !ok && !c.isDrainClearParticipant(from) {
		return false
	}
	return c.reconcileDrainClearStateSnapshotLocked(from, snapshot)
}

func (c *Controller) reconcileDrainClearStateSnapshotLocked(from node.ID, snapshot drainTargetSnapshot) bool {
	clearState := c.drainClearState
	if clearState == nil || c.drainSession != nil {
		return false
	}

	if snapshot.epoch < clearState.epoch {
		return false
	}

	if snapshot.epoch == clearState.epoch {
		if snapshot.target.IsEmpty() {
			c.ackDrainClearStateLocked(from)
			return false
		}
		return c.addDrainClearPendingNodeLocked(from)
	}

	if snapshot.target.IsEmpty() {
		c.ackDrainClearStateLocked(from)
		return false
	}

	// A higher non-empty target cannot acknowledge the old clear. Treat it as a
	// newer stale target unless a future active drain session owns that epoch.
	return c.createOrReplaceDrainClearStateLocked(
		snapshot.target,
		snapshot.epoch,
		c.snapshotDrainClearParticipants(),
	)
}

func (c *Controller) createOrReplaceDrainClearStateLocked(
	target node.ID,
	epoch uint64,
	pendingNodes map[node.ID]struct{},
) bool {
	if target.IsEmpty() || epoch == 0 || len(pendingNodes) == 0 || c.drainSession != nil {
		return false
	}

	if c.drainClearState != nil {
		if c.drainClearState.epoch > epoch {
			return false
		}
		if c.drainClearState.epoch == epoch {
			before := len(c.drainClearState.pendingNodes)
			utils.CopySetToSet(pendingNodes, c.drainClearState.pendingNodes)
			if len(c.drainClearState.pendingNodes) == before {
				return false
			}
			c.drainClearState.dirty = true
			c.drainController.StartDrainTargetClearGate(
				c.drainClearState.target,
				c.drainClearState.epoch,
				pendingNodes,
			)
			return true
		}
	}

	clonedPendingNodes := make(map[node.ID]struct{}, len(pendingNodes))
	utils.CopySetToSet(pendingNodes, clonedPendingNodes)
	c.drainClearState = &drainClearState{
		target:       target,
		epoch:        epoch,
		pendingNodes: clonedPendingNodes,
		dirty:        true,
	}
	c.drainController.StartDrainTargetClearGate(target, epoch, clonedPendingNodes)
	log.Info("stale dispatcher drain target clear started",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch),
		zap.Int("pendingNodeCount", len(clonedPendingNodes)))
	return true
}

func (c *Controller) addDrainClearPendingNodeLocked(from node.ID) bool {
	clearState := c.drainClearState
	if clearState == nil || !c.isDrainClearParticipant(from) {
		return false
	}
	if _, ok := clearState.pendingNodes[from]; ok {
		return false
	}

	clearState.pendingNodes[from] = struct{}{}
	clearState.dirty = true
	c.drainController.StartDrainTargetClearGate(
		clearState.target,
		clearState.epoch,
		map[node.ID]struct{}{from: {}},
	)
	return true
}

func (c *Controller) ackDrainClearStateLocked(from node.ID) {
	clearState := c.drainClearState
	if clearState == nil {
		return
	}
	if _, ok := clearState.pendingNodes[from]; !ok {
		return
	}

	delete(clearState.pendingNodes, from)
	c.drainController.RemoveDrainTargetClearPendingNode(from, clearState.target, clearState.epoch)
	if len(clearState.pendingNodes) != 0 {
		return
	}

	log.Info("dispatcher drain clear acknowledged by all nodes",
		zap.Stringer("targetNodeID", clearState.target),
		zap.Uint64("targetEpoch", clearState.epoch))
	c.drainController.ClearDrainTargetClearGate(clearState.target, clearState.epoch)
	c.drainClearState = nil
}

func (c *Controller) snapshotDrainClearParticipants() map[node.ID]struct{} {
	participants := make(map[node.ID]struct{})
	if c.nodeManager == nil {
		return participants
	}
	for id := range c.nodeManager.GetAliveNodes() {
		if c.isDrainClearParticipant(id) {
			participants[id] = struct{}{}
		}
	}
	return participants
}

func (c *Controller) isDrainClearParticipant(id node.ID) bool {
	if c.nodeManager == nil || c.nodeManager.GetNodeInfo(id) == nil {
		return false
	}
	if c.bootstrapper == nil || !c.bootstrapper.NodeInitialized(id) {
		return false
	}
	if c.drainController == nil {
		return false
	}
	version, observed := c.drainController.GetDrainProtocolVersion(id)
	return observed && heartbeatpb.SupportsCoordinatorDrivenDrain(version)
}

func (c *Controller) observeDispatcherDrainTargetClearNodeRemoved(id node.ID) {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	clearState := c.drainClearState
	if clearState == nil {
		return
	}
	if _, ok := clearState.pendingNodes[id]; !ok {
		return
	}

	// A removed node can no longer ack. Dropping it from the pending set keeps
	// the clear tombstone from leaking when membership changes mid-clear.
	delete(clearState.pendingNodes, id)
	c.drainController.RemoveDrainTargetClearPendingNode(id, clearState.target, clearState.epoch)
	if len(clearState.pendingNodes) != 0 {
		return
	}

	log.Info("dispatcher drain clear completed after pending nodes were removed",
		zap.Stringer("targetNodeID", clearState.target),
		zap.Uint64("targetEpoch", clearState.epoch))
	c.drainController.ClearDrainTargetClearGate(clearState.target, clearState.epoch)
	c.drainClearState = nil
}

// aggregateDrainTargetProgress sums per-changefeed drain progress counters
// reported for the given drain target epoch.
func (c *Controller) aggregateDrainTargetProgress(target node.ID, epoch uint64) (dispatcherCount int, inflightMoveCount int) {
	if target.IsEmpty() || epoch == 0 {
		return 0, 0
	}

	targetID := target.String()
	c.drainSessionMu.Lock()
	session := c.drainSession
	if session == nil || session.target != target || session.epoch != epoch {
		c.drainSessionMu.Unlock()
		return 0, 0
	}
	tracked := session.trackedChangefeeds
	c.drainSessionMu.Unlock()

	for _, id := range tracked {
		cf := c.changefeedDB.GetByID(id)
		if cf == nil {
			continue
		}
		if !c.changefeedDB.IsReplicating(cf) {
			continue
		}
		status := cf.GetStatus()
		if status == nil {
			continue
		}
		progress := status.GetDrainProgress()
		if progress == nil || progress.GetTargetNodeId() != targetID || progress.GetTargetEpoch() != epoch {
			continue
		}
		dispatcherCount += int(progress.GetTargetDispatcherCount())
		inflightMoveCount += int(progress.GetTargetInflightDrainMoveCount())
	}
	return dispatcherCount, inflightMoveCount
}

func drainStateString(state drain.State) string {
	switch state {
	case drain.StateAlive:
		return "alive"
	case drain.StateDraining:
		return "draining"
	case drain.StateStopping:
		return "stopping"
	case drain.StateUnknown:
		return "unknown"
	default:
		return "unspecified"
	}
}

// isBestEffortDrainComplete checks whether coordinator-observed drain signals
// have converged for the active target epoch.
func isBestEffortDrainComplete(
	nodeState drain.State,
	drainingObserved bool,
	stoppingObserved bool,
	remaining int,
) bool {
	if nodeState == drain.StateUnknown || !drainingObserved {
		return false
	}
	return stoppingObserved && remaining == 0
}

// drainRemainingEstimate uses the larger workload dimension to avoid obvious double counting.
func drainRemainingEstimate(
	maintainersOnTarget int,
	inflightOpsInvolvingTarget int,
	dispatcherCountOnTarget int,
	targetInflightDrainMoveCount int,
	pendingStatusCount int,
) int {
	return max(
		maintainersOnTarget,
		inflightOpsInvolvingTarget,
		dispatcherCountOnTarget,
		targetInflightDrainMoveCount,
		pendingStatusCount,
	)
}

// ensureDrainRemainingNonZero keeps v1 compatibility before observed completion.
func ensureDrainRemainingNonZero(remaining int) int {
	return max(remaining, 1)
}
