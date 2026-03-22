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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/zap"
)

// dispatcherBootstrapState is intentionally empty for now.
// It marks the stream-open layer between control convergence and data delivery.
type dispatcherBootstrapState struct{}

func (d *dispatcherStat) shouldResendResetOnRepeatedReady(from node.ID) bool {
	if d.readyCallback != nil {
		return false
	}
	if d.lastEventSeq.Load() != 0 {
		return false
	}
	log.Info("received repeated ready signal before handshake, resend reset",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Stringer("eventServiceID", from))
	d.commitReady(from)
	return true
}

func (d *dispatcherStat) handleSignalEvent(event dispatcher.DispatcherEvent) {
	localServerID := d.eventCollector.getLocalServerID()
	from := *event.From

	switch event.GetType() {
	case commonEvent.TypeReadyEvent:
		readyEvent, ok := event.Event.(*commonEvent.ReadyEvent)
		if !ok {
			log.Panic("ready event is not a ready event", zap.Any("event", event))
		}
		currentServerID, currentIncarnation, _ := d.connState.getBinding()
		if currentServerID == from && isIncarnationStale(currentIncarnation, readyEvent.Incarnation) {
			return
		}
		d.clearPendingRegister(from)
		if d.connState.isStreamingFrom(localServerID) {
			if from == localServerID {
				d.shouldResendResetOnRepeatedReady(localServerID)
			}
			if from != localServerID {
				d.removeFrom(from)
			}
			return
		}
		if from != localServerID && !d.connState.isCurrentEventService(from) {
			return
		}

		if from == localServerID {
			if d.readyCallback != nil {
				d.connState.setReady(localServerID, readyEvent.Incarnation)
				d.readyCallback()
				return
			}
			oldEventServiceID := d.connState.getEventServiceID()
			if oldEventServiceID != "" && oldEventServiceID != localServerID {
				d.removeFrom(oldEventServiceID)
			}
			log.Info("received ready signal from local event service, prepare to reset the dispatcher",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()))
			d.connState.setReady(localServerID, readyEvent.Incarnation)
			d.connState.clearRemoteCandidates()
			d.commitReady(localServerID)
			return
		}

		if d.connState.isStreamingFrom(from) {
			if d.shouldResendResetOnRepeatedReady(from) {
				return
			}
			log.Info("received ready signal from the same server again, ignore it",
				zap.Stringer("changefeedID", d.target.GetChangefeedID()),
				zap.Stringer("dispatcher", d.getDispatcherID()),
				zap.Stringer("eventServiceID", from))
			return
		}
		log.Info("received ready signal from remote event service, prepare to reset the dispatcher",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Stringer("eventServiceID", from))
		d.connState.setReady(from, readyEvent.Incarnation)
		d.commitReady(from)
	case commonEvent.TypeNotReusableEvent:
		notReusableEvent, ok := event.Event.(*commonEvent.NotReusableEvent)
		if !ok {
			log.Panic("not reusable event is not a not reusable event", zap.Any("event", event))
		}
		currentServerID, currentIncarnation, _ := d.connState.getBinding()
		if currentServerID == from && isIncarnationStale(currentIncarnation, notReusableEvent.Incarnation) {
			return
		}
		d.clearPendingRegister(from)
		if from == localServerID {
			log.Panic("should not happen: local event service should not send not reusable event")
		}
		if candidate := d.connState.getNextRemoteCandidate(); candidate != "" {
			d.registerTo(candidate)
			return
		}
		d.connState.clearIfCurrentEventService(from)
	default:
		log.Panic("should not happen: unknown signal event type", zap.Int("eventType", event.GetType()))
	}
}

func (d *dispatcherStat) handleHandshakeEvent(event dispatcher.DispatcherEvent) {
	log.Info("handle handshake event",
		zap.Stringer("changefeedID", d.target.GetChangefeedID()),
		zap.Stringer("dispatcher", d.getDispatcherID()),
		zap.Any("event", event))

	handshakeEvent, ok := event.Event.(*commonEvent.HandshakeEvent)
	if !ok {
		log.Panic("handshake event is not a handshake event", zap.Any("event", event))
	}
	currentServerID, currentIncarnation, _ := d.connState.getBinding()
	if currentServerID != "" && *event.From == currentServerID && isIncarnationStale(currentIncarnation, handshakeEvent.Incarnation) {
		return
	}
	if currentServerID != "" && *event.From != currentServerID {
		return
	}
	if !d.isFromCurrentEpoch(event) {
		log.Info("receive a handshake event from a stale epoch, ignore it",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Any("event", event.Event))
		return
	}
	if event.GetSeq() != 1 {
		log.Warn("should not happen: handshake event sequence number is not 1",
			zap.Stringer("changefeedID", d.target.GetChangefeedID()),
			zap.Stringer("dispatcher", d.getDispatcherID()),
			zap.Uint64("sequence", event.GetSeq()))
		return
	}
	if handshakeEvent.TableInfo != nil {
		d.tableInfo.Store(handshakeEvent.TableInfo)
	}
	d.connState.setReady(*event.From, handshakeEvent.Incarnation)
	d.lastEventSeq.Store(handshakeEvent.Seq)
	d.clearPendingReset(handshakeEvent.Epoch)
}
