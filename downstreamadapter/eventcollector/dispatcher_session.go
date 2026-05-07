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
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

type dispatcherConnState struct {
	sync.RWMutex
	// 1) if eventServiceID is set to a remote event service,
	//   it means the dispatcher is trying to register to the remote event service,
	//   but eventServiceID may be changed if registration failed.
	// 2) if eventServiceID is set to local event service,
	//   it means the dispatcher has received ready signal from local event service,
	//   and eventServiceID will never change.
	eventServiceID node.ID
	// whether has received ready signal from `serverID`
	readyEventReceived atomic.Bool
	// the remote event services which may contain data this dispatcher needed
	remoteCandidates []string
}

func (d *dispatcherConnState) clear() {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = ""
	d.readyEventReceived.Store(false)
}

func (d *dispatcherConnState) setEventServiceID(serverID node.ID) {
	d.Lock()
	defer d.Unlock()
	d.eventServiceID = serverID
}

func (d *dispatcherConnState) getEventServiceID() node.ID {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID
}

func (d *dispatcherConnState) isCurrentEventService(serverID node.ID) bool {
	d.RLock()
	defer d.RUnlock()
	return d.eventServiceID == serverID
}

func (d *dispatcherConnState) isReceivingDataEvent() bool {
	d.RLock()
	defer d.RUnlock()
	return !d.eventServiceID.IsEmpty() && d.readyEventReceived.Load()
}

func (d *dispatcherConnState) trySetRemoteCandidates(nodes []string) bool {
	d.Lock()
	defer d.Unlock()
	// reading from a event service or checking remotes already, ignore
	if d.eventServiceID != "" {
		return false
	}
	if len(nodes) == 0 {
		return false
	}
	d.remoteCandidates = nodes
	return true
}

func (d *dispatcherConnState) getNextRemoteCandidate() node.ID {
	d.Lock()
	defer d.Unlock()
	if len(d.remoteCandidates) > 0 {
		d.eventServiceID = node.ID(d.remoteCandidates[0])
		d.remoteCandidates = d.remoteCandidates[1:]
		return d.eventServiceID
	}
	return ""
}

func (d *dispatcherConnState) clearRemoteCandidates() {
	d.Lock()
	defer d.Unlock()
	d.remoteCandidates = nil
}

type dispatcherSession struct {
	owner         *dispatcherStat
	readyCallback func()

	connState dispatcherConnState
}

func newDispatcherSession(owner *dispatcherStat, readyCallback func()) *dispatcherSession {
	return &dispatcherSession{
		owner:         owner,
		readyCallback: readyCallback,
	}
}

func (s *dispatcherSession) clear() {
	// TODO: this design is bad because we may receive stale heartbeat response,
	// which make us call clear and register again. But the register may be ignore,
	// so we will not receive any ready event.
	s.connState.clear()
}

// registerTo register the dispatcher to the specified event service.
func (s *dispatcherSession) registerTo(serverID node.ID) {
	// `onlyReuse` is used to control the register behavior at logservice side
	// it should be set to `false` when register to a local event service,
	// and set to `true` when register to a remote event service.
	onlyReuse := serverID != s.owner.eventCollector.getLocalServerID()
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.owner.newDispatcherRegisterRequest(s.owner.eventCollector.getLocalServerID().String(), onlyReuse),
	)
	s.owner.eventCollector.enqueueMessageForSend(msg)
}

// commitReady is used to notify the event service to start sending events.
func (s *dispatcherSession) commitReady(serverID node.ID) {
	s.doReset(serverID, s.owner.getResetTs())
}

// reset is used to reset the dispatcher to the specified commitTs,
// it will remove the dispatcher from the dynamic stream and add it back.
func (s *dispatcherSession) reset(serverID node.ID) {
	s.doReset(serverID, s.owner.getResetTs())
}

func (s *dispatcherSession) doReset(serverID node.ID, resetTs uint64) {
	var epoch uint64
	for {
		currentState := s.owner.loadCurrentEpochState()
		nextState := newDispatcherEpochState(currentState.epoch+1, 0, resetTs)
		if s.owner.currentEpoch.CompareAndSwap(currentState, nextState) {
			epoch = nextState.epoch
			break
		}
	}
	resetRequest := s.owner.newDispatcherResetRequest(
		s.owner.eventCollector.getLocalServerID().String(),
		resetTs,
		epoch,
	)
	msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, resetRequest)
	s.owner.eventCollector.enqueueMessageForSend(msg)
	log.Info("send reset dispatcher request to event service",
		zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.owner.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID),
		zap.Uint64("epoch", epoch),
		zap.Uint64("resetTs", resetTs))
}

// remove is used to remove the dispatcher from the event service.
func (s *dispatcherSession) remove() {
	s.removeFrom(s.owner.eventCollector.getLocalServerID())
	eventServiceID := s.getEventServiceID()
	if eventServiceID != "" && eventServiceID != s.owner.eventCollector.getLocalServerID() {
		s.removeFrom(eventServiceID)
	}
}

// removeFrom is used to remove the dispatcher from the specified event service.
func (s *dispatcherSession) removeFrom(serverID node.ID) {
	log.Info("send remove dispatcher request to event service",
		zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
		zap.Stringer("dispatcher", s.owner.getDispatcherID()),
		zap.Stringer("eventServiceID", serverID))
	msg := messaging.NewSingleTargetMessage(
		serverID,
		messaging.EventServiceTopic,
		s.owner.newDispatcherRemoveRequest(s.owner.eventCollector.getLocalServerID().String()),
	)
	s.owner.eventCollector.enqueueMessageForSend(msg)
}

// "signalEvent" refers to the types of events that may modify the event service with which this dispatcher communicates.
// "signalEvent" includes TypeReadyEvent/TypeNotReusableEvent
func (s *dispatcherSession) handleSignalEvent(event dispatcher.DispatcherEvent) {
	localServerID := s.owner.eventCollector.getLocalServerID()

	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		// if the dispatcher has received ready signal from local event service,
		// ignore all types of signal events.
		if s.isCurrentEventService(localServerID) {
			// If we receive a ready event from a remote service while connected to the local
			// service, it implies a stale registration. Send a remove request to clean it up.
			if event.From != nil && *event.From != localServerID {
				s.removeFrom(*event.From)
			}
			return
		}

		// if the event is neither from local event service nor from the current event service, ignore it.
		if *event.From != localServerID && !s.isCurrentEventService(*event.From) {
			return
		}

		if *event.From == localServerID {
			if s.readyCallback != nil {
				// If readyCallback is set, this dispatcher is performing its initial
				// registration with the local event service. Therefore, no deregistration
				// from a previous service is necessary.
				s.connState.setEventServiceID(localServerID)
				s.connState.readyEventReceived.Store(true)
				s.readyCallback()
				return
			}
			// note: this must be the first ready event from local event service
			oldEventServiceID := s.getEventServiceID()
			if oldEventServiceID != "" {
				s.removeFrom(oldEventServiceID)
			}
			log.Info("received ready signal from local event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
				zap.Stringer("dispatcher", s.owner.getDispatcherID()))

			s.connState.setEventServiceID(localServerID)
			s.connState.readyEventReceived.Store(true)
			s.connState.clearRemoteCandidates()
			s.commitReady(localServerID)
		} else {
			// note: this ready event must be from a remote event service which the dispatcher is trying to register to.
			// TODO: if receive too much redudant ready events from remote service, we may need reset again?
			if s.connState.readyEventReceived.Load() {
				log.Info("received ready signal from the same server again, ignore it",
					zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
					zap.Stringer("dispatcher", s.owner.getDispatcherID()),
					zap.Stringer("eventServiceID", *event.From))
				return
			}
			log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
				zap.Stringer("dispatcher", s.owner.getDispatcherID()),
				zap.Stringer("eventServiceID", *event.From))
			s.connState.readyEventReceived.Store(true)
			s.commitReady(*event.From)
		}
	case commonEvent.TypeNotReusableEvent:
		if *event.From == localServerID {
			log.Panic("should not happen: local event service should not send not reusable event")
		}
		candidate := s.connState.getNextRemoteCandidate()
		if candidate != "" {
			s.registerTo(candidate)
		}
	default:
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
}

func (s *dispatcherSession) setRemoteCandidates(nodes []string) {
	if len(nodes) == 0 {
		return
	}
	if s.connState.trySetRemoteCandidates(nodes) {
		log.Info("set remote candidates",
			zap.Stringer("changefeedID", s.owner.target.GetChangefeedID()),
			zap.Stringer("dispatcherID", s.owner.getDispatcherID()),
			zap.Int64("tableID", s.owner.target.GetTableSpan().TableID),
			zap.Strings("nodes", nodes))
		candidate := s.connState.getNextRemoteCandidate()
		s.registerTo(candidate)
	}
}

func (s *dispatcherSession) getEventServiceID() node.ID {
	return s.connState.getEventServiceID()
}

func (s *dispatcherSession) isCurrentEventService(serverID node.ID) bool {
	return s.connState.isCurrentEventService(serverID)
}

func (s *dispatcherSession) isReceivingDataEvent() bool {
	return s.connState.isReceivingDataEvent()
}
