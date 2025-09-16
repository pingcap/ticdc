// Copyright 2025 PingCAP, Inc.
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

package messaging

import (
	"sync/atomic"

	. "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/prometheus/client_golang/prometheus"
)

// localMessageTarget implements the SendMessageChannel interface.
// It is used to send messages to the local server.
// It simply pushes the messages to the messageCenter's channel directly.
type localMessageTarget struct {
	localId  node.ID
	sequence atomic.Uint64

	// The gather channel from the message center.
	// We only need to push and pull the messages from those channel.
	recvEventCh chan *TargetMessage
	recvCmdCh   chan *TargetMessage

	sendEventCounter   prometheus.Counter
	dropMessageCounter prometheus.Counter
	sendCmdCounter     prometheus.Counter
}

func (s *localMessageTarget) sendEvent(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvEventCh, msg)
	if err != nil {
		s.recordCongestedMessageError(streamTypeEvent)
	} else {
		s.sendEventCounter.Inc()
	}
	return err
}

func (s *localMessageTarget) sendCommand(msg *TargetMessage) error {
	err := s.sendMsgToChan(s.recvCmdCh, msg)
	if err != nil {
		s.recordCongestedMessageError(streamTypeCommand)
	} else {
		s.sendCmdCounter.Inc()
	}
	return err
}

func newLocalMessageTarget(id node.ID,
	gatherRecvEventChan chan *TargetMessage,
	gatherRecvCmdChan chan *TargetMessage,
) *localMessageTarget {
	return &localMessageTarget{
		localId:            id,
		recvEventCh:        gatherRecvEventChan,
		recvCmdCh:          gatherRecvCmdChan,
		sendEventCounter:   metrics.MessagingSendMsgCounter.WithLabelValues("event"),
		dropMessageCounter: metrics.MessagingDropMsgCounter.WithLabelValues("message"),
		sendCmdCounter:     metrics.MessagingSendMsgCounter.WithLabelValues("command"),
	}
}

func (s *localMessageTarget) recordCongestedMessageError(typeE string) {
	metrics.MessagingErrorCounter.WithLabelValues(typeE, "message_congested").Inc()
}

func (s *localMessageTarget) sendMsgToChan(ch chan *TargetMessage, msg ...*TargetMessage) error {
	for i, m := range msg {
		m.To = s.localId
		m.From = s.localId
		m.Sequence = s.sequence.Add(1)
		select {
		case ch <- m:
		default:
			remains := len(msg) - i
			s.dropMessageCounter.Add(float64(remains))
			return AppError{Type: ErrorTypeMessageCongested, Reason: "Send message is congested"}
		}
	}
	return nil
}
