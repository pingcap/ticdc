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

	// pendingStatusInitialized indicates the pending status baseline has been frozen for this session.
	// The baseline only shrinks over time and must not be re-snapshotted when it becomes empty.
	pendingStatusInitialized bool
	pendingStatus            map[common.ChangeFeedID]struct{}

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

	if observation.dispatcherCountOnTarget > 0 ||
		observation.pendingStatusCount > 0 ||
		observation.targetInflightDrainMoveCount > 0 {
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
	observation.dispatcherCountOnTarget = c.aggregateDrainTargetDispatcherCount(target, epoch)
	observation.targetInflightDrainMoveCount = c.aggregateDrainTargetInflightMoveCount(target, epoch)
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

// collectDrainPendingStatus tracks a frozen baseline of running changefeeds and
// returns how many of them have not reported the active drain target epoch yet.
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

	if !session.pendingStatusInitialized {
		session.pendingStatusInitialized = true
		session.pendingStatus = c.snapshotDrainStatusBaseline(target, epoch)
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
	if len(cfs) == 0 {
		return nil
	}

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

	c.drainSession = &drainSession{
		target: target,
		epoch:  c.dispatcherDrainEpoch,
		dirty:  true,
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
	c.drainSession = nil
	c.drainSessionMu.Unlock()

	log.Info("dispatcher drain target cleared",
		zap.Stringer("targetNodeID", target),
		zap.Uint64("targetEpoch", epoch))
	c.broadcastDispatcherDrainTarget("", epoch)
}

// maybeBroadcastDispatcherDrainTarget sends the active drain target when forced,
// dirty, or periodic resend is due.
func (c *Controller) maybeBroadcastDispatcherDrainTarget(force bool) {
	c.drainSessionMu.Lock()
	if c.drainSession == nil {
		c.drainSessionMu.Unlock()
		return
	}
	target := c.drainSession.target
	epoch := c.drainSession.epoch
	needSend := force ||
		c.drainSession.dirty ||
		time.Since(c.drainSession.lastSent) >= dispatcherDrainTargetResendIntvl
	if !needSend {
		c.drainSessionMu.Unlock()
		return
	}
	c.drainSessionMu.Unlock()

	c.broadcastDispatcherDrainTarget(target, epoch)

	c.drainSessionMu.Lock()
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

// aggregateDrainTargetDispatcherCount sums per-changefeed dispatcher counts
// reported for the given drain target epoch.
func (c *Controller) aggregateDrainTargetDispatcherCount(target node.ID, epoch uint64) int {
	if target.IsEmpty() || epoch == 0 {
		return 0
	}

	targetID := target.String()
	dispatcherCount := 0
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
	}
	return dispatcherCount
}

// aggregateDrainTargetInflightMoveCount sums per-changefeed drain move counts
// reported for the given drain target epoch.
func (c *Controller) aggregateDrainTargetInflightMoveCount(target node.ID, epoch uint64) int {
	if target.IsEmpty() || epoch == 0 {
		return 0
	}

	targetID := target.String()
	inflightMoveCount := 0
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
		inflightMoveCount += int(progress.GetTargetInflightDrainMoveCount())
	}
	return inflightMoveCount
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
