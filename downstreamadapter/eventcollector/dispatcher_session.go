// Copyright 2026 PingCAP, Inc.
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

package eventcollector

import (
	"slices"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

const remoteProbePendingTimeout = 5 * time.Second

// dispatcherConnState owns the EventService registration state for one dispatcher.
// It does not send messages. Its job is to apply atomic state transitions and
// return the side-effect inputs that dispatcherSession should execute.
//
// The state machine tracks three independent EventService registration facts:
//  1. currentEventServiceID: the event service currently trusted for data.
//  2. localReadyPending: whether the local registration is still waiting for
//     local ready.
//  3. pendingRemoteEventServiceID: the remote reuse probe currently waiting for
//     ready or not reusable.
//
// This lets the collector represent the common startup path correctly when remote read is enabled:
//   - after add: current="", localReadyPending=true, pendingRemoteEventServiceID=""
//   - after choosing a remote candidate: current="", localReadyPending=true,
//     pendingRemoteEventServiceID="<remote>"
//   - after remote ready first: current="<remote>", localReadyPending=true,
//     pendingRemoteEventServiceID=""
//   - after local ready later: current="<local>", localReadyPending=false,
//     pendingRemoteEventServiceID=""
//
// This means local registration and remote probing can overlap. A remote service
// may serve data first (its ready usually arrives before the local ready because
// the local subscription has to initialize first). If local becomes ready
// first, it is held until the bounded remote probe completes. Once remote is
// serving, a later local ready must catch up to its progress before taking over.
type dispatcherConnState struct {
	sync.RWMutex
	// removed marks the session as terminal after removal starts. New
	// registrations, resets, and late signal events are ignored once it is set.
	removed bool
	// currentEventServiceID is the event service whose ready signal has been
	// accepted and whose data or heartbeat progress is currently trusted.
	currentEventServiceID node.ID
	// localReadyPending means the local register request has been sent but local
	// ready has not been accepted. It may be true while a remote service is
	// already current; local ready wins after its resolved ts covers the current
	// dispatcher checkpoint.
	localReadyPending bool
	// pendingRemoteEventServiceID is the remote EventService currently being
	// probed for reuse. It waits for either ready or not reusable, and only one
	// remote probe is active at a time.
	pendingRemoteEventServiceID node.ID
	// remoteCandidates are the remaining remote EventServices to probe after the
	// current pending remote reports not reusable.
	remoteCandidates []string
	// remoteResolvedTs is the resolved ts reported by the remote EventService
	// currently serving. It is only meaningful while currentEventServiceID is a
	// remote service and is cleared when the local service wins back. The local
	// ready must cover it before taking over: the sink checkpoint alone is not a
	// safe baseline before the remote delivers its first events because it still
	// equals the dispatcher start ts, which would let a barely started local
	// subscription win the race and stall the table while catching up from TiKV.
	remoteResolvedTs   uint64
	remoteProbePending bool
	heldLocalReady     bool
}

func (d *dispatcherConnState) beginRemoteProbePending() bool {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return false
	}
	d.remoteProbePending = true
	return true
}

// Registration state transitions.
func (d *dispatcherConnState) beginRegisterToLocal() bool {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return false
	}
	d.localReadyPending = true
	return true
}

// beginRegisterToRemote records that the dispatcher is attempting to register to
// a specific remote EventService.
func (d *dispatcherConnState) beginRegisterToRemote(serverID node.ID) bool {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return false
	}
	d.pendingRemoteEventServiceID = serverID
	return true
}

type cleanupTargets []node.ID

// appendCleanupTargetUnique appends target into the cleanup list if it is
// non-empty, not equal to skip, and not already present.
func (t *cleanupTargets) appendCleanupTargetUnique(target node.ID, skip node.ID) {
	if target.IsEmpty() || target == skip {
		return
	}
	if slices.Contains(*t, target) {
		return
	}
	*t = append(*t, target)
}

// readyDecision describes what dispatcherSession should do after receiving a
// ready event from an EventService.
//
// commitTarget is the EventService whose ready is accepted and should receive a
// reset request. cleanupTargets are EventServices that should receive remove
// requests to clean up stale registrations.
type readyDecision struct {
	commitTarget   node.ID
	cleanupTargets cleanupTargets
}

// Signal-event state transitions.
//
// acceptReady enforces the ready acceptance rules:
//  1. once local is already serving, any later remote ready is stale and should
//     only trigger cleanup;
//  2. local ready can be accepted while local registration is still pending;
//     when a remote is already serving, the local resolved ts must cover both
//     the current dispatcher checkpoint and the progress the remote reported;
//  3. remote ready is accepted only from the single remote candidate currently
//     being probed.
func (d *dispatcherConnState) acceptReady(
	from node.ID,
	localServerID node.ID,
	readyResolvedTs uint64,
	requiredCheckpointTs uint64,
	remoteDeliveredTs uint64,
	dispatcherStartTs uint64,
) readyDecision {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return readyDecision{}
	}

	if from == localServerID {
		if !d.localReadyPending {
			return readyDecision{}
		}
		d.heldLocalReady = true
		if !d.currentEventServiceID.IsEmpty() &&
			d.currentEventServiceID != localServerID {
			// The local event service may only win back once its resolved ts
			// covers the position the current remote has actually delivered to
			// the sink, so the switch never stalls the table. Before the remote
			// delivers its first events the sink position still equals the
			// dispatcher start ts, which would let a barely started local
			// subscription win the race; hold on the remote's reported progress
			// until data actually flows.
			baseline := max(requiredCheckpointTs, remoteDeliveredTs)
			if baseline <= dispatcherStartTs && d.remoteResolvedTs > baseline {
				baseline = d.remoteResolvedTs
			}
			if readyResolvedTs < baseline {
				return readyDecision{}
			}
		} else {
			// A local ready only proves that the EventService accepted the
			// registration. It does not prove that the local subscription has
			// caught up. Prefer a reusable remote source until it succeeds or
			// the active probe timeout releases the local fallback.
			if d.remoteProbePending || !d.pendingRemoteEventServiceID.IsEmpty() {
				return readyDecision{}
			}
		}
		decision := readyDecision{
			commitTarget:   localServerID,
			cleanupTargets: make(cleanupTargets, 0, 2),
		}
		decision.cleanupTargets.appendCleanupTargetUnique(d.currentEventServiceID, localServerID)
		decision.cleanupTargets.appendCleanupTargetUnique(d.pendingRemoteEventServiceID, localServerID)

		d.currentEventServiceID = localServerID
		d.localReadyPending = false
		d.pendingRemoteEventServiceID = ""
		d.remoteCandidates = nil
		d.remoteResolvedTs = 0
		d.heldLocalReady = false
		return decision
	}

	// Once local is serving, any ready from a remote EventService can only be
	// stale and should be cleaned up. Another local ready does not change state
	// unless a local re-register is pending, which is handled above.
	if d.currentEventServiceID == localServerID {
		decision := readyDecision{
			cleanupTargets: make(cleanupTargets, 0, 1),
		}
		decision.cleanupTargets.appendCleanupTargetUnique(from, "")
		return decision
	}

	if d.pendingRemoteEventServiceID != from {
		return readyDecision{}
	}

	d.currentEventServiceID = from
	// Keep localReadyPending unchanged: after the local service catches up, a
	// later local ready may move the dispatcher back to local.
	d.pendingRemoteEventServiceID = ""
	d.remoteCandidates = nil
	// Record the remote progress the local service must catch up to before it
	// may win back. This is more meaningful than the sink checkpoint, which
	// still equals the start ts before the remote delivers its first events.
	d.remoteResolvedTs = readyResolvedTs
	return readyDecision{
		commitTarget: from,
	}
}

func (d *dispatcherConnState) acceptHeldLocalReady(localServerID node.ID) readyDecision {
	d.Lock()
	defer d.Unlock()
	if d.removed || !d.heldLocalReady || !d.currentEventServiceID.IsEmpty() ||
		d.remoteProbePending || !d.pendingRemoteEventServiceID.IsEmpty() {
		return readyDecision{}
	}
	d.currentEventServiceID = localServerID
	d.localReadyPending = false
	d.heldLocalReady = false
	return readyDecision{commitTarget: localServerID}
}

// abortRemoteProbe releases the local fallback after the remote probe times
// out. It returns the one remote EventService that may have received a
// registration and therefore needs best-effort cleanup.
func (d *dispatcherConnState) abortRemoteProbe() node.ID {
	d.Lock()
	defer d.Unlock()
	if d.removed || !d.currentEventServiceID.IsEmpty() {
		return ""
	}
	remote := d.pendingRemoteEventServiceID
	d.remoteProbePending = false
	d.pendingRemoteEventServiceID = ""
	d.remoteCandidates = nil
	return remote
}

func (d *dispatcherConnState) beginRemove(localServerID node.ID) ([]node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return nil, true
	}
	targets := make(cleanupTargets, 0, 3)
	targets.appendCleanupTargetUnique(localServerID, "")
	targets.appendCleanupTargetUnique(d.currentEventServiceID, "")
	targets.appendCleanupTargetUnique(d.pendingRemoteEventServiceID, "")
	d.removed = true
	d.currentEventServiceID = ""
	d.localReadyPending = false
	d.pendingRemoteEventServiceID = ""
	d.remoteCandidates = nil
	d.heldLocalReady = false
	d.remoteResolvedTs = 0
	d.remoteProbePending = false
	return []node.ID(targets), false
}

// Read-only state queries used by session orchestration and other collector
// components.
func (d *dispatcherConnState) getCurrentEventServiceID() node.ID {
	d.RLock()
	defer d.RUnlock()
	return d.currentEventServiceID
}

func (d *dispatcherConnState) isReceivingDataEvent() bool {
	d.RLock()
	defer d.RUnlock()
	return !d.currentEventServiceID.IsEmpty()
}

func (d *dispatcherConnState) isRemoved() bool {
	d.RLock()
	defer d.RUnlock()
	return d.removed
}

// Remote-probing transitions.
//
// beginRemoteProbing starts remote reuse probing using a list of candidates. It
// seeds pendingRemoteEventServiceID with the first candidate and keeps the
// remaining nodes in remoteCandidates.
func (d *dispatcherConnState) beginRemoteProbing(nodes []string) (node.ID, bool, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return "", false, false
	}
	// If the dispatcher is already reading from an event service or checking
	// remotes, ignore the new candidates.
	if !d.currentEventServiceID.IsEmpty() || !d.pendingRemoteEventServiceID.IsEmpty() {
		return "", false, false
	}
	// The coordinator answer has arrived. Keep the active timeout running while
	// a candidate is being probed; it bounds both coordinator and remote waits.
	d.remoteProbePending = false
	if len(nodes) == 0 {
		return "", false, true
	}
	candidate := node.ID(nodes[0])
	d.pendingRemoteEventServiceID = candidate
	d.remoteCandidates = nodes[1:]
	return candidate, true, false
}

// advanceRemoteProbeAfterNotReusable accepts only the not reusable response
// from the active remote probe and advances the fallback chain one candidate at
// a time.
func (d *dispatcherConnState) advanceRemoteProbeAfterNotReusable(from node.ID) (node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed || d.pendingRemoteEventServiceID != from {
		return "", false
	}
	if len(d.remoteCandidates) == 0 {
		d.pendingRemoteEventServiceID = ""
		return "", true
	}
	candidate := node.ID(d.remoteCandidates[0])
	d.remoteCandidates = d.remoteCandidates[1:]
	d.pendingRemoteEventServiceID = candidate
	return candidate, true
}

// dispatcherSession owns side-effect orchestration for one dispatcher. It asks
// dispatcherConnState to apply atomic state transitions, then turns the results
// into register/remove/reset requests.
type dispatcherSession struct {
	// requestMu serializes connState transitions and the dispatcher requests
	// emitted by this session. Lock ordering: requestMu -> connState.
	requestMu sync.Mutex
	// target provides immutable dispatcher metadata used by register/reset/remove requests.
	target dispatcher.DispatcherService
	// localServerID identifies the collector side when talking to EventService.
	localServerID node.ID
	// sendMessage delivers dispatcher requests generated by this session.
	// It is called while requestMu is held, so it must only enqueue or delegate
	// the message and must not perform network I/O or other long-blocking work.
	sendMessage func(*messaging.TargetMessage)
	// advanceEpochForReset advances the dispatcher's epoch and returns the new value.
	advanceEpochForReset func(resetTs uint64) uint64
	// readyCallback is only set during the initial local registration path.
	readyCallback func()
	// connState tracks which EventService this session is currently talking to.
	connState dispatcherConnState
	// remoteProbeTimer bounds the remote-first window. It is protected by
	// requestMu together with connState transitions and dispatcher requests.
	remoteProbeTimer      *time.Timer
	remoteProbeGeneration uint64
}

func newDispatcherSession(
	target dispatcher.DispatcherService,
	localServerID node.ID,
	sendMessage func(*messaging.TargetMessage),
	advanceEpochForReset func(resetTs uint64) uint64,
	readyCallback func(),
) *dispatcherSession {
	return &dispatcherSession{
		target:               target,
		localServerID:        localServerID,
		sendMessage:          sendMessage,
		advanceEpochForReset: advanceEpochForReset,
		readyCallback:        readyCallback,
	}
}

// Register/reset/remove request entry points.

func (s *dispatcherSession) startLocalRegistration() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if !s.beginRegister(s.localServerID) {
		return
	}
	s.sendRegisterRequest(s.localServerID)
}

func (s *dispatcherSession) beginRemoteProbePending() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if !s.connState.beginRemoteProbePending() {
		return
	}
	s.remoteProbeGeneration++
	generation := s.remoteProbeGeneration
	if s.remoteProbeTimer != nil {
		s.remoteProbeTimer.Stop()
	}
	s.remoteProbeTimer = time.AfterFunc(remoteProbePendingTimeout, func() {
		s.handleRemoteProbeTimeout(generation)
	})
}

func (s *dispatcherSession) stopRemoteProbeTimerLocked() {
	s.remoteProbeGeneration++
	if s.remoteProbeTimer != nil {
		s.remoteProbeTimer.Stop()
		s.remoteProbeTimer = nil
	}
}

func (s *dispatcherSession) handleRemoteProbeTimeout(generation uint64) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if generation != s.remoteProbeGeneration {
		return
	}
	// Invalidate this generation before changing the connection state. This also
	// makes an already-queued timer callback harmless in deterministic tests.
	s.remoteProbeGeneration++
	s.remoteProbeTimer = nil
	remote := s.connState.abortRemoteProbe()
	if !remote.IsEmpty() {
		s.removeFromLocked(remote)
	}
	s.acceptHeldLocalReadyLocked()
}

func (s *dispatcherSession) retryCurrentRegistrationIfRemovedFrom(serverID node.ID) bool {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.connState.getCurrentEventServiceID() != serverID {
		return false
	}
	log.Info("dispatcher removed in current event service, retry registration",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcherID", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	if !s.beginRegister(serverID) {
		return false
	}
	s.sendRegisterRequest(serverID)
	return true
}

func (s *dispatcherSession) sendRegisterRequest(serverID node.ID) {
	// For local registration, OnlyReuse is set to false which means the target may initialize a new
	// source if needed.
	// For remote probing, OnlyReuse is set to true which means the target should
	// only accept the dispatcher if it can reuse an existing source.
	onlyReuse := serverID != s.localServerID
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.newDispatcherRegisterRequest(s.localServerID.String(), onlyReuse),
	)
	s.sendMessage(msg)
}

// beginRegister records the in-flight registrations before the register request
// is sent. Local and remote registration are tracked independently because a
// dispatcher may wait for local ready and a remote reusable candidate at the
// same time.
func (s *dispatcherSession) beginRegister(serverID node.ID) bool {
	if serverID == s.localServerID {
		return s.connState.beginRegisterToLocal()
	}
	return s.connState.beginRegisterToRemote(serverID)
}

// commitLocalRegistration commits the accepted local registration by sending
// RESET to the local EventService.
func (s *dispatcherSession) commitLocalRegistration() {
	s.doReset(s.localServerID, s.target.GetCheckpointTs())
}

func (s *dispatcherSession) resetCurrentEventService() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.connState.isRemoved() {
		return
	}
	serverID := s.connState.getCurrentEventServiceID()
	if serverID.IsEmpty() {
		log.Warn("skip reset because current event service is empty",
			zap.Stringer("changefeedID", s.target.GetChangefeedID()),
			zap.Stringer("dispatcher", s.target.GetId()))
		return
	}
	s.doResetLocked(serverID, s.target.GetCheckpointTs())
}

// doReset sends RESET to the target event service and advances the
// collector epoch for the new stream.
func (s *dispatcherSession) doReset(serverID node.ID, resetTs uint64) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.connState.isRemoved() {
		return
	}
	s.doResetLocked(serverID, resetTs)
}

func (s *dispatcherSession) doResetLocked(serverID node.ID, resetTs uint64) {
	epoch := s.advanceEpochForReset(resetTs)
	resetRequest := s.newDispatcherResetRequest(
		s.localServerID.String(),
		resetTs,
		epoch,
	)
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, resetRequest)
	s.sendMessage(msg)
	log.Info("send reset dispatcher request to event service",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("epoch", epoch),
		zap.Uint64("resetTs", resetTs))
}

// remove marks the session as terminal, snapshots all cleanup targets, then
// sends remove requests outside the state lock.
func (s *dispatcherSession) remove() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	s.stopRemoteProbeTimerLocked()
	cleanupTargets, alreadyRemoved := s.connState.beginRemove(s.localServerID)
	if alreadyRemoved {
		return
	}
	for _, target := range cleanupTargets {
		s.removeFromLocked(target)
	}
}

// removeFromLocked sends REMOVE to the target event service. The request may
// represent either terminal removal of the dispatcher session or best-effort
// cleanup of a stale registration on another event service.
func (s *dispatcherSession) removeFromLocked(serverID node.ID) {
	log.Info("send remove dispatcher request to event service",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.newDispatcherRemoveRequest(s.localServerID.String()),
	)
	s.sendMessage(msg)
}

func (s *dispatcherSession) handleSignalEvent(event dispatcher.DispatcherEvent) {
	if s.connState.isRemoved() {
		return
	}
	from := *event.From
	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		s.handleReadyEvent(from, event.GetCommitTs())
	case commonEvent.TypeNotReusableEvent:
		if from == s.localServerID {
			log.Panic("should not happen: local event service should not send not reusable event")
		}
		s.requestMu.Lock()
		defer s.requestMu.Unlock()
		nextCandidate, accepted := s.connState.advanceRemoteProbeAfterNotReusable(from)
		if !accepted {
			return
		}
		if nextCandidate.IsEmpty() {
			s.stopRemoteProbeTimerLocked()
			s.acceptHeldLocalReadyLocked()
			return
		}
		s.sendRegisterRequest(nextCandidate)
	default:
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
}

func (s *dispatcherSession) acceptHeldLocalReadyLocked() {
	accepted := s.connState.acceptHeldLocalReady(s.localServerID)
	if accepted.commitTarget.IsEmpty() {
		return
	}
	s.handleAcceptedLocalReadyLocked(s.target.GetCheckpointTs())
}

// handleReadyEvent applies the ready decision produced by connState: clean up
// any stale registrations, then commit whichever target won the ready race.
func (s *dispatcherSession) handleReadyEvent(from node.ID, readyResolvedTs uint64) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	// connState decides whether this ready should be accepted and which stale
	// registrations must be cleaned up. Session only applies the side effects.
	requiredCheckpointTs := s.target.GetCheckpointTs()
	// remoteDeliveredTs is the highest resolved ts the sink has received from
	// the current event service, i.e. the position the remote has actually
	// delivered. The local service must catch up to it before winning back so
	// the switch never stalls the table while the local catches up from TiKV.
	remoteDeliveredTs := s.target.GetResolvedTs()
	accepted := s.connState.acceptReady(
		from,
		s.localServerID,
		readyResolvedTs,
		requiredCheckpointTs,
		remoteDeliveredTs,
		s.target.GetStartTs(),
	)
	for _, target := range accepted.cleanupTargets {
		s.removeFromLocked(target)
	}
	if accepted.commitTarget.IsEmpty() {
		return
	}
	if accepted.commitTarget != s.localServerID {
		s.stopRemoteProbeTimerLocked()
	}
	if accepted.commitTarget == s.localServerID {
		s.handleAcceptedLocalReadyLocked(requiredCheckpointTs)
		return
	}
	s.handleAcceptedRemoteReadyLocked(accepted.commitTarget)
}

func (s *dispatcherSession) handleAcceptedLocalReadyLocked(resetTs uint64) {
	if s.readyCallback != nil {
		// This path is used during the initial add flow before the dispatcher is
		// committed. Local is still authoritative, so any speculative remote
		// registration must already be canceled above.
		readyCallback := s.readyCallback
		s.readyCallback = nil
		readyCallback()
		return
	}
	log.Info("received ready signal from local event service, prepare to reset the dispatcher",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()))
	s.doResetLocked(s.localServerID, resetTs)
}

func (s *dispatcherSession) handleAcceptedRemoteReadyLocked(serverID node.ID) {
	if s.readyCallback != nil {
		log.Panic("ready callback should be nil when accepting remote ready",
			zap.Stringer("changefeedID", s.target.GetChangefeedID()),
			zap.Stringer("dispatcher", s.target.GetId()),
			zap.Stringer("eventServiceID", serverID))
	}
	log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.target.GetId()),
		zap.Stringer("eventServiceID", serverID))
	s.doResetLocked(serverID, s.target.GetCheckpointTs())
}

// Dispatcher request builders.
func (s *dispatcherSession) newDispatcherRegisterRequest(serverID string, onlyReuse bool) *messaging.DispatcherRequest {
	startTs := s.target.GetStartTs()
	syncPointInterval := s.target.GetSyncPointInterval()
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			StartTs:      startTs,
			// ServerId is the id of the request sender.
			ServerId:                      serverID,
			ActionType:                    eventpb.ActionType_ACTION_TYPE_REGISTER,
			FilterConfig:                  s.target.GetFilterConfig(),
			EnableSyncPoint:               s.target.EnableSyncPoint(),
			SyncPointInterval:             uint64(syncPointInterval.Seconds()),
			SyncPointTs:                   syncpoint.CalculateStartSyncPointTs(startTs, syncPointInterval, s.target.GetSkipSyncpointAtStartTs()),
			OnlyReuse:                     onlyReuse,
			BdrMode:                       s.target.GetBDRMode(),
			Mode:                          s.target.GetMode(),
			Epoch:                         0,
			Timezone:                      s.target.GetTimezone(),
			Integrity:                     s.target.GetIntegrityConfig(),
			OutputRawChangeEvent:          s.target.IsOutputRawChangeEvent(),
			TxnAtomicity:                  string(s.target.GetTxnAtomicity()),
			EnableIgnoreUpdateOnlyColumns: s.target.EnableIgnoreUpdateOnlyColumns(),
			LowLatencyMode:                s.target.IsLowLatencyMode(),
		},
	}
}

func (s *dispatcherSession) newDispatcherResetRequest(serverID string, resetTs uint64, epoch uint64) *messaging.DispatcherRequest {
	syncPointInterval := s.target.GetSyncPointInterval()

	// After reset during normal runtime, redundant syncpoints can be filtered at
	// the event collector side, so only the case that resetTs equals startTs
	// needs special handling.
	skipSyncpointSameAsResetTs := false
	if resetTs == s.target.GetStartTs() {
		skipSyncpointSameAsResetTs = s.target.GetSkipSyncpointAtStartTs()
	}
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			StartTs:      resetTs,
			// ServerId is the id of the request sender.
			ServerId:          serverID,
			ActionType:        eventpb.ActionType_ACTION_TYPE_RESET,
			FilterConfig:      s.target.GetFilterConfig(),
			EnableSyncPoint:   s.target.EnableSyncPoint(),
			SyncPointInterval: uint64(syncPointInterval.Seconds()),
			SyncPointTs:       syncpoint.CalculateStartSyncPointTs(resetTs, syncPointInterval, skipSyncpointSameAsResetTs),
			// OnlyReuse:         false,
			BdrMode:                       s.target.GetBDRMode(),
			Mode:                          s.target.GetMode(),
			Epoch:                         epoch,
			Timezone:                      s.target.GetTimezone(),
			Integrity:                     s.target.GetIntegrityConfig(),
			OutputRawChangeEvent:          s.target.IsOutputRawChangeEvent(),
			TxnAtomicity:                  string(s.target.GetTxnAtomicity()),
			EnableIgnoreUpdateOnlyColumns: s.target.EnableIgnoreUpdateOnlyColumns(),
			LowLatencyMode:                s.target.IsLowLatencyMode(),
		},
	}
}

func (s *dispatcherSession) newDispatcherRemoveRequest(serverID string) *messaging.DispatcherRequest {
	return &messaging.DispatcherRequest{
		DispatcherRequest: &eventpb.DispatcherRequest{
			ChangefeedId: s.target.GetChangefeedID().ToPB(),
			DispatcherId: s.target.GetId().ToPB(),
			TableSpan:    s.target.GetTableSpan(),
			// ServerId is the id of the request sender.
			ServerId:   serverID,
			ActionType: eventpb.ActionType_ACTION_TYPE_REMOVE,
			Mode:       s.target.GetMode(),
		},
	}
}

// startRemoteProbing begins probing reusable remote event services one by one.
func (s *dispatcherSession) startRemoteProbing(nodes []string) {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	candidate, ok, probeFinished := s.connState.beginRemoteProbing(nodes)
	if probeFinished {
		s.stopRemoteProbeTimerLocked()
		s.acceptHeldLocalReadyLocked()
		return
	}
	if !ok {
		return
	}
	log.Info("set remote candidates",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcherID", s.target.GetId()),
		zap.Int64("tableID", s.target.GetTableSpan().TableID),
		zap.Strings("nodes", nodes))
	s.sendRegisterRequest(candidate)
}

// Read-only session queries.
func (s *dispatcherSession) getEventServiceID() node.ID {
	return s.connState.getCurrentEventServiceID()
}

func (s *dispatcherSession) isReceivingDataEvent() bool {
	return s.connState.isReceivingDataEvent()
}
