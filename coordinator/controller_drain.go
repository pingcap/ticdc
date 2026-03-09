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
	"go.uber.org/zap"
)

const dispatcherDrainTargetResendIntvl = 5 * time.Second

type drainSession struct {
	target node.ID
	epoch  uint64

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

// newDispatcherDrainEpochSeed creates a non-zero epoch seed for this process lifetime.
// It prevents immediate epoch reuse after coordinator restarts.
func newDispatcherDrainEpochSeed() uint64 {
	epoch := uint64(time.Now().UnixNano())
	if epoch == 0 {
		return 1
	}
	return epoch
}

// DrainNode starts or continues draining one target node for the v1 drain API.
// It ensures an active target epoch, broadcasts the drain target, requests liveness
// transition, then evaluates a one-shot drain observation.
//
// Drain completion requires the target to reach STOPPING after DRAINING and for
// all maintainer-side drain work to converge to zero.
// The returned remaining is guaranteed to be non-zero until completion is proven.
func (c *Controller) DrainNode(_ context.Context, target node.ID) (int, error) {
	if c.nodeManager.GetNodeInfo(target) == nil {
		return 0, errors.ErrCaptureNotExist.GenWithStackByArgs(target)
	}
	// Drain completion relies on in-memory changefeed state built by coordinator bootstrap.
	// Before bootstrap is complete, always return non-zero remaining to avoid premature zero.
	if c.initialized == nil || !c.initialized.Load() {
		log.Info("drain waiting for coordinator bootstrap",
			zap.Stringer("targetNodeID", target))
		return 1, nil
	}
	targetEpoch, err := c.ensureDispatcherDrainTarget(target)
	if err != nil {
		return 0, err
	}
	c.maybeBroadcastDispatcherDrainTarget(true)

	c.drainController.RequestDrain(target)

	observation := c.observeDrainNode(target, targetEpoch)

	// drain API must not return 0 until drain completion is proven.
	if isDrainCompletionProven(
		observation.nodeState,
		observation.drainingObserved,
		observation.stoppingObserved,
		observation.remaining,
	) {
		c.clearDispatcherDrainTarget(target, targetEpoch)
		return 0, nil
	}

	if observation.remaining > 0 {
		log.Info("drain completion blocked by remaining work",
			zap.Stringer("targetNodeID", target),
			zap.Uint64("targetEpoch", targetEpoch),
			zap.Int("maintainersOnTarget", observation.maintainersOnTarget),
			zap.Int("inflightOpsInvolvingTarget", observation.inflightOpsInvolvingTarget),
			zap.Int("dispatcherCountOnTarget", observation.dispatcherCountOnTarget),
			zap.Int("targetInflightDrainMoveCount", observation.targetInflightDrainMoveCount),
			zap.Int("pendingStatusCount", observation.pendingStatusCount),
			zap.Int("remaining", observation.remaining))
	}
	return ensureDrainRemainingNonZero(observation.remaining), nil
}

type drainNodeObservation struct {
	// drainNodeObservation captures all one-shot completion signals used by DrainNode.
	// maintainersOnTarget is the number of maintainers still hosted on the target node.
	maintainersOnTarget int
	// inflightOpsInvolvingTarget is the number of operators that still involve the target node.
	inflightOpsInvolvingTarget int
	// dispatcherCountOnTarget is the sum of maintainer-reported dispatchers still on target.
	dispatcherCountOnTarget int
	// targetInflightDrainMoveCount is the sum of maintainer-reported dispatcher
	// move operators still draining work away from the target node.
	targetInflightDrainMoveCount int
	// pendingStatusCount is the number of running changefeeds not converged to the active target epoch.
	pendingStatusCount int
	// remaining is the max of all workload dimensions used by drain completion gating.
	remaining int
	// nodeState is the drain controller state of the target node.
	nodeState drain.State
	// drainingObserved indicates DRAINING has been observed for this target.
	drainingObserved bool
	// stoppingObserved indicates STOPPING has been observed for this target.
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

// snapshotDrainStatusBaseline captures running changefeeds that have not yet
// reported the active drain target epoch. The returned snapshot is used as a
// monotonic pending set during one drain session.
func (c *Controller) snapshotDrainStatusBaseline(target node.ID, epoch uint64) map[common.ChangeFeedID]struct{} {
	cfs := c.changefeedDB.GetReplicating()
	targetID := target.String()
	snapshot := make(map[common.ChangeFeedID]struct{}, len(cfs))
	for _, cf := range cfs {
		if !isDrainStatusConvergenceRelevant(cf) {
			continue
		}
		status := cf.GetStatus()
		if status != nil {
			progress := status.GetDrainProgress()
			if progress != nil && progress.GetTargetNodeId() == targetID && progress.GetTargetEpoch() == epoch {
				// Changefeeds that already acknowledged this drain epoch do not need further tracking.
				continue
			}
		}
		snapshot[cf.ID] = struct{}{}
	}
	return snapshot
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

// ensureDispatcherDrainTarget creates or reuses the single active drain
// session. It rejects requests for a different target while one is active.
func (c *Controller) ensureDispatcherDrainTarget(target node.ID) (uint64, error) {
	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	if c.drainSession != nil {
		if c.drainSession.target == target {
			return c.drainSession.epoch, nil
		}
		return 0, errors.ErrSchedulerRequestFailed.GenWithStackByArgs(
			"drain already in progress on capture " + c.drainSession.target.String())
	}

	c.dispatcherDrainEpoch++
	if c.dispatcherDrainEpoch == 0 {
		c.dispatcherDrainEpoch = 1
	}

	pendingStatus := c.snapshotDrainStatusBaseline(target, c.dispatcherDrainEpoch)
	c.drainClearState = nil
	c.drainSession = &drainSession{
		target:        target,
		epoch:         c.dispatcherDrainEpoch,
		pendingStatus: pendingStatus,
		dirty:         true,
	}
	log.Info("dispatcher drain target activated",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", c.dispatcherDrainEpoch))
	return c.dispatcherDrainEpoch, nil
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

// clearDispatcherDrainTarget closes the matching active drain session and
// broadcasts an empty target at the same epoch to clear stale local targets.
func (c *Controller) clearDispatcherDrainTarget(target node.ID, epoch uint64) {
	c.drainSessionMu.Lock()
	if c.drainSession == nil || c.drainSession.target != target || c.drainSession.epoch != epoch {
		c.drainSessionMu.Unlock()
		return
	}
	pendingNodes := make(map[node.ID]struct{}, len(c.nodeManager.GetAliveNodeIDs()))
	for _, id := range c.nodeManager.GetAliveNodeIDs() {
		pendingNodes[id] = struct{}{}
	}
	c.drainSession = nil
	if len(pendingNodes) == 0 {
		c.drainClearState = nil
	} else {
		// Freeze the nodes that must observe this clear. New nodes do not need
		// to ack an old clear because they bootstrap from the current coordinator state.
		c.drainClearState = &drainClearState{
			target:       target,
			epoch:        epoch,
			pendingNodes: pendingNodes,
			dirty:        true,
		}
	}
	c.drainSessionMu.Unlock()

	log.Info("dispatcher drain target cleared",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch))
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
		sendingClear bool
	)
	switch {
	case c.drainSession != nil:
		target = c.drainSession.target
		epoch = c.drainSession.epoch
		needSend = force ||
			c.drainSession.dirty ||
			time.Since(c.drainSession.lastSent) >= dispatcherDrainTargetResendIntvl
	case c.drainClearState != nil:
		epoch = c.drainClearState.epoch
		sendingClear = true
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

	c.broadcastDispatcherDrainTarget(target, epoch)

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

// broadcastDispatcherDrainTarget sends SetDispatcherDrainTargetRequest to all
// currently alive nodes as a best-effort broadcast.
func (c *Controller) broadcastDispatcherDrainTarget(target node.ID, epoch uint64) {
	if epoch == 0 || c.messageCenter == nil || c.nodeManager == nil {
		return
	}

	req := &heartbeatpb.SetDispatcherDrainTargetRequest{
		TargetNodeId: target.String(),
		TargetEpoch:  epoch,
	}
	for _, id := range c.nodeManager.GetAliveNodeIDs() {
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

func (c *Controller) observeDispatcherDrainTargetHeartbeat(from node.ID, hb *heartbeatpb.NodeHeartbeat) {
	if hb == nil {
		return
	}

	c.drainSessionMu.Lock()
	defer c.drainSessionMu.Unlock()

	clearState := c.drainClearState
	if clearState == nil {
		return
	}
	if _, ok := clearState.pendingNodes[from]; !ok {
		return
	}

	hbEpoch := hb.GetDispatcherDrainTargetEpoch()
	hbTarget := node.ID(hb.GetDispatcherDrainTargetNodeId())
	if hbEpoch < clearState.epoch {
		// Older heartbeat cannot prove this node has observed the clear.
		return
	}
	if hbEpoch == clearState.epoch && !hbTarget.IsEmpty() {
		// Same epoch still carrying a target means the old target is still cached locally.
		return
	}

	// Ack is accepted when the node reports either:
	// 1) the same epoch with an empty target, or
	// 2) any newer epoch, which necessarily supersedes the old clear.
	delete(clearState.pendingNodes, from)
	if len(clearState.pendingNodes) != 0 {
		return
	}

	log.Info("dispatcher drain clear acknowledged by all nodes",
		zap.Stringer("targetNodeID", clearState.target),
		zap.Uint64("targetEpoch", clearState.epoch))
	c.drainClearState = nil
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
	if len(clearState.pendingNodes) != 0 {
		return
	}

	log.Info("dispatcher drain clear completed after pending nodes were removed",
		zap.Stringer("targetNodeID", clearState.target),
		zap.Uint64("targetEpoch", clearState.epoch))
	c.drainClearState = nil
}

// aggregateDrainTargetProgress sums per-changefeed drain progress counters
// reported for the given drain target epoch.
func (c *Controller) aggregateDrainTargetProgress(target node.ID, epoch uint64) (dispatcherCount int, inflightMoveCount int) {
	if target.IsEmpty() || epoch == 0 {
		return 0, 0
	}

	targetID := target.String()
	cfs := c.changefeedDB.GetReplicating()
	for _, cf := range cfs {
		if cf == nil {
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

// isDrainCompletionProven checks whether drain completion can be safely concluded.
// Returning true is intentionally strict because v1 drain API must avoid premature zero remaining.
func isDrainCompletionProven(
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
	remaining := maintainersOnTarget
	if inflightOpsInvolvingTarget > remaining {
		remaining = inflightOpsInvolvingTarget
	}
	if dispatcherCountOnTarget > remaining {
		remaining = dispatcherCountOnTarget
	}
	if targetInflightDrainMoveCount > remaining {
		remaining = targetInflightDrainMoveCount
	}
	if pendingStatusCount > remaining {
		remaining = pendingStatusCount
	}
	return remaining
}

// ensureDrainRemainingNonZero keeps v1 compatibility before completion is proven.
func ensureDrainRemainingNonZero(remaining int) int {
	if remaining == 0 {
		return 1
	}
	return remaining
}
