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
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

func (c *EventCollector) processDSFeedback(ctx context.Context) error {
	log.Info("Start process feedback from dynamic stream")
	defer log.Info("Stop process feedback from dynamic stream")
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case feedback := <-c.ds.Feedback():
			if feedback.FeedbackType == dynstream.ReleasePath {
				if v, ok := c.changefeedMap.Load(feedback.Area); ok {
					v.(*changefeedStat).memoryReleaseCount.Add(1)
				}
				log.Info("release dispatcher memory in DS", zap.Any("dispatcherID", feedback.Path))
				c.ds.Release(feedback.Path)
			}
		case feedback := <-c.redoDs.Feedback():
			if feedback.FeedbackType == dynstream.ReleasePath {
				if v, ok := c.changefeedMap.Load(feedback.Area); ok {
					v.(*changefeedStat).memoryReleaseCount.Add(1)
				}
				log.Info("release dispatcher memory in redo DS", zap.Any("dispatcherID", feedback.Path))
				c.redoDs.Release(feedback.Path)
			}
		}
	}
}

func (c *EventCollector) runDispatchMessage(ctx context.Context, inCh <-chan *messaging.TargetMessage, mode int64) error {
	ds := c.getDynamicStream(mode)
	metricDispatcherReceivedKVEventCount, metricDispatcherReceivedResolvedTsEventCount := c.getMetric(mode)
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case targetMessage := <-inCh:
			for _, msg := range targetMessage.Message {
				switch e := msg.(type) {
				case event.Event:
					switch e.GetType() {
					case event.TypeBatchResolvedEvent:
						events := e.(*event.BatchResolvedEvent).Events
						from := &targetMessage.From
						resolvedTsCount := int32(0)
						for _, resolvedEvent := range events {
							ds.Push(resolvedEvent.DispatcherID, dispatcher.NewDispatcherEvent(from, resolvedEvent))
							resolvedTsCount += resolvedEvent.Len()
						}
						metricDispatcherReceivedResolvedTsEventCount.Add(float64(resolvedTsCount))
					case event.TypeReadyEvent,
						event.TypeNotReusableEvent,
						event.TypeHandshakeEvent:
						log.Warn("bootstrap event leaked into data plane worker, ignore it",
							zap.String("type", targetMessage.Type.String()),
							zap.Int("eventType", e.GetType()),
							zap.Stringer("from", targetMessage.From),
							zap.Stringer("dispatcherID", e.GetDispatcherID()))
					default:
						metricDispatcherReceivedKVEventCount.Add(float64(e.Len()))
						dispatcherEvent := dispatcher.NewDispatcherEvent(&targetMessage.From, e)
						ds.Push(e.GetDispatcherID(), dispatcherEvent)
					}
				default:
					log.Warn("unknown message type, ignore it",
						zap.String("type", targetMessage.Type.String()),
						zap.Any("msg", msg))
				}
			}
		}
	}
}
