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
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"go.uber.org/zap"
)

func (c *EventCollector) handleBootstrapMessage(targetMessage *messaging.TargetMessage) error {
	for _, msg := range targetMessage.Message {
		e, ok := msg.(event.Event)
		if !ok {
			log.Warn("unknown bootstrap message type, ignore it",
				zap.String("type", targetMessage.Type.String()),
				zap.Any("msg", msg))
			continue
		}
		stat := c.getDispatcherStatByID(e.GetDispatcherID())
		if stat == nil {
			continue
		}
		dispatcherEvent := dispatcher.NewDispatcherEvent(&targetMessage.From, e)
		switch e.GetType() {
		case event.TypeReadyEvent, event.TypeNotReusableEvent:
			stat.handleSignalEvent(dispatcherEvent)
		case event.TypeHandshakeEvent:
			stat.handleHandshakeEvent(dispatcherEvent)
		default:
			log.Warn("unexpected bootstrap event type, ignore it",
				zap.String("type", targetMessage.Type.String()),
				zap.Int("eventType", e.GetType()))
		}
	}
	return nil
}
