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
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// remoteProbeTimeout bounds how long a single remote reuse probe may stay in
// flight without a ready or not-reusable response before the dispatcher gives
// up on that candidate and falls back to the local event service. Without it, a
// silent remote would leave the dispatcher permanently waiting because the
// local ready is held while the probe is in flight.
const remoteProbeTimeout = 5 * time.Second

// remoteProbeLocalCatchUpMargin is the minimum physical-ts lead the local
// subscription must have over the dispatcher start ts before its ready may win
// while a remote reuse probe is still in flight. A fresh subscription that has
// barely started should yield to the remote (which already serves the span); a
// reused subscription that is already serving real data may win immediately so
// the dispatcher never waits for a probe it does not need.
const remoteProbeLocalCatchUpMargin = time.Second

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
// may serve data first, but after the local service catches up to the progress
// the remote reported (and the dispatcher checkpoint), a later local ready moves
// the dispatcher back to local and cleans up remote registrations. While a
// remote reuse probe is still in flight the local ready is held, so the remote
// (which already serves the span) wins over a local subscription that has not
// served data yet; the probe expires after remoteProbeTimeout to unblock the
// local fallback.
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
	remoteResolvedTs uint64
	// remoteProbeStartAt marks the start of the remote reuse probing effort:
	// from when the reusable-event-service request is sent (t0) through the
	// candidate register round-trips. While it is non-zero the local ready is
	// held so the remote (which already serves the span) wins over a local
	// subscription that has not served data yet. It is cleared when the probe
	// concludes (remote accepted, no reusable candidate, or timeout).
	remoteProbeStartAt time.Time
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

// beginRemoteProbeRequest records that the dispatcher has asked the log
// coordinator for reusable remote event services. The local ready is held from
// this point (not only after a candidate register is sent) so the remote gets a
// chance even when the coordinator response is slower than the local ready.
func (d *dispatcherConnState) beginRemoteProbeRequest() {
	d.Lock()
	defer d.Unlock()
	if d.removed || !d.remoteProbeStartAt.IsZero() {
		return
	}
	d.remoteProbeStartAt = time.Now()
}

// localHasCaughtUp reports whether the local subscription has already pulled at
// least remoteProbeLocalCatchUpMargin past the dispatcher start ts, i.e. it is
// serving real data and winning the local ready immediately does not risk a
// stall. A fresh subscription that has barely started returns false so the
// remote probe (which already serves the span) gets a chance.
func (d *dispatcherConnState) localHasCaughtUp(localResolvedTs uint64, dispatcherStartTs uint64) bool {
	if localResolvedTs <= dispatcherStartTs {
		return false
	}
	return oracle.ExtractPhysical(localResolvedTs)-oracle.ExtractPhysical(dispatcherStartTs) >=
		remoteProbeLocalCatchUpMargin.Milliseconds()
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
	d.remoteProbeStartAt = time.Now()
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
//  2. local ready can be accepted while local registration is still pending,
//     but only after any in-flight remote reuse probe has concluded (the remote
//     already serves the span and should win over a local subscription that has
//     not served data yet) and, when a remote is already serving, after the
//     local resolved ts covers both the current dispatcher checkpoint and the
//     progress the remote reported;
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
		if !d.currentEventServiceID.IsEmpty() &&
			d.currentEventServiceID != localServerID {
			// The local event service may only win back once its resolved ts
			// covers the position the current remote has actually delivered to
			// the sink, so the switch never stalls the table. Before the remote
			// delivers its first events the sink position still equals the
			// dispatcher start ts, which would let a barely started local
			// subscription win the race; hold on the remote's reported progress
			// until data actually flows.
			baseline := requiredCheckpointTs
			if remoteDeliveredTs > baseline {
				baseline = remoteDeliveredTs
			}
			if baseline <= dispatcherStartTs && d.remoteResolvedTs > baseline {
				baseline = d.remoteResolvedTs
			}
			if readyResolvedTs < baseline {
				return readyDecision{}
			}
		} else if !d.remoteProbeStartAt.IsZero() {
			// A remote reuse probe is in flight, either waiting for the log
			// coordinator response or registering a candidate. A local ready
			// only means the local registration is complete, not that the local
			// subscription has served any data yet: a fresh subscription's
			// resolved ts still starts at the move checkpoint. Prefer the
			// remote, which already serves the span, over letting the local win
			// by arriving first. However, if the local subscription has already
			// pulled well past the move checkpoint (e.g. the node reused an
			// existing subscription), it is already as fast as the remote and
			// should win immediately instead of waiting. The probe expires after
			// remoteProbeTimeout so a silent coordinator/remote cannot block the
			// fallback.
			if !d.localHasCaughtUp(readyResolvedTs, dispatcherStartTs) {
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
		d.remoteProbeStartAt = time.Time{}
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
	d.remoteProbeStartAt = time.Time{}
	// Record the remote progress the local service must catch up to before it
	// may win back. This is more meaningful than the sink checkpoint, which
	// still equals the start ts before the remote delivers its first events.
	d.remoteResolvedTs = readyResolvedTs
	return readyDecision{
		commitTarget: from,
	}
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
	d.remoteResolvedTs = 0
	d.remoteProbeStartAt = time.Time{}
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
func (d *dispatcherConnState) beginRemoteProbing(nodes []string) (node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed {
		return "", false
	}
	// If the dispatcher is already reading from an event service or checking
	// remotes, ignore the new candidates.
	if !d.currentEventServiceID.IsEmpty() || !d.pendingRemoteEventServiceID.IsEmpty() {
		return "", false
	}
	if len(nodes) == 0 {
		// No remote can reuse the span, so the probing effort concludes and the
		// local event service may take over.
		d.remoteProbeStartAt = time.Time{}
		return "", false
	}
	candidate := node.ID(nodes[0])
	d.pendingRemoteEventServiceID = candidate
	d.remoteProbeStartAt = time.Now()
	d.remoteCandidates = nodes[1:]
	return candidate, true
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
		// No more candidates: the probing effort concludes and the local event
		// service may take over.
		d.pendingRemoteEventServiceID = ""
		d.remoteProbeStartAt = time.Time{}
		return "", true
	}
	candidate := node.ID(d.remoteCandidates[0])
	d.remoteCandidates = d.remoteCandidates[1:]
	d.pendingRemoteEventServiceID = candidate
	d.remoteProbeStartAt = time.Now()
	return candidate, true
}

// expireRemoteProbeLocked abandons a remote reuse probe that has been in flight
// longer than timeout without a ready or not-reusable response. It advances to
// the next candidate when one remains, otherwise it clears the pending probe so
// the dispatcher can fall back to the local event service. The probe may still
// be waiting for the log coordinator response (no pending candidate yet); in
// that case the whole effort is abandoned the same way.
func (d *dispatcherConnState) expireRemoteProbeLocked(now time.Time, timeout time.Duration) (node.ID, bool) {
	d.Lock()
	defer d.Unlock()
	if d.removed || d.remoteProbeStartAt.IsZero() {
		return "", false
	}
	if now.Sub(d.remoteProbeStartAt) < timeout {
		return "", false
	}
	if d.pendingRemoteEventServiceID.IsEmpty() {
		// Still waiting for the reusable-event-service response. Conclude the
		// probing effort and let the local event service take over.
		d.remoteProbeStartAt = time.Time{}
		return "", true
	}
	if len(d.remoteCandidates) == 0 {
		d.pendingRemoteEventServiceID = ""
		d.remoteProbeStartAt = time.Time{}
		return "", true
	}
	candidate := node.ID(d.remoteCandidates[0])
	d.remoteCandidates = d.remoteCandidates[1:]
	d.pendingRemoteEventServiceID = candidate
	d.remoteProbeStartAt = now
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
		s.handleReadyEvent(from, uint64(event.GetCommitTs()))
	case commonEvent.TypeNotReusableEvent:
		if from == s.localServerID {
			log.Panic("should not happen: local event service should not send not reusable event")
		}
		s.requestMu.Lock()
		defer s.requestMu.Unlock()
		nextCandidate, accepted := s.connState.advanceRemoteProbeAfterNotReusable(from)
		if !accepted || nextCandidate.IsEmpty() {
			return
		}
		s.sendRegisterRequest(nextCandidate)
	default:
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
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
	candidate, ok := s.connState.beginRemoteProbing(nodes)
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

// beginRemoteProbeRequest marks the start of the remote reuse probing effort.
// It is called when the reusable-event-service request is sent, so the local
// ready is held even before the log coordinator responds.
func (s *dispatcherSession) beginRemoteProbeRequest() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	s.connState.beginRemoteProbeRequest()
}

// expireStaleRemoteProbe abandons a remote reuse probe that has been waiting
// too long, advancing to the next candidate or clearing the pending probe so
// the local event service can take over. It is called periodically so a silent
// remote cannot block the dispatcher forever.
func (s *dispatcherSession) expireStaleRemoteProbe() {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if s.connState.isRemoved() {
		return
	}
	nextCandidate, advanced := s.connState.expireRemoteProbeLocked(time.Now(), remoteProbeTimeout)
	if !advanced {
		return
	}
	log.Info("remote reuse probe timed out",
		zap.Stringer("changefeedID", s.target.GetChangefeedID()),
		zap.Stringer("dispatcherID", s.target.GetId()),
		zap.Stringer("nextCandidate", nextCandidate))
	if !nextCandidate.IsEmpty() {
		s.sendRegisterRequest(nextCandidate)
	}
}

// Read-only session queries.
func (s *dispatcherSession) getEventServiceID() node.ID {
	return s.connState.getCurrentEventServiceID()
}

func (s *dispatcherSession) isReceivingDataEvent() bool {
	return s.connState.isReceivingDataEvent()
}
