// Copyright 2024 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/ticdc/utils/dynstream"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	receiveChanSize          = 1024 * 8
	commonMsgRetryQuota      = 3
	controlReconcileInterval = time.Second
)

type DispatcherMessage struct {
	Message    *messaging.TargetMessage
	Droppable  bool
	RetryQuota int
}

type dispatcherRemoveKey struct {
	dispatcherID common.DispatcherID
	serverID     node.ID
}

type dispatcherRemoveTombstone struct {
	message *messaging.TargetMessage
}

func newDispatcherMessage(msg *messaging.TargetMessage, droppable bool, retryQuota int) DispatcherMessage {
	return DispatcherMessage{
		Message:    msg,
		Droppable:  droppable,
		RetryQuota: retryQuota,
	}
}

func (d *DispatcherMessage) decrAndCheckRetry() bool {
	if !d.Droppable {
		return true
	}
	d.RetryQuota--
	return d.RetryQuota > 0
}

type changefeedStat struct {
	changefeedID common.ChangeFeedID

	metricMemoryUsageMax      prometheus.Gauge
	metricMemoryUsageUsed     prometheus.Gauge
	metricMemoryUsageMaxRedo  prometheus.Gauge
	metricMemoryUsageUsedRedo prometheus.Gauge
	dispatcherCount           atomic.Int32
	memoryReleaseCount        atomic.Uint32
}

func newChangefeedStat(changefeedID common.ChangeFeedID) *changefeedStat {
	return &changefeedStat{
		changefeedID:              changefeedID,
		metricMemoryUsageMax:      metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector", "max", changefeedID.Keyspace(), changefeedID.Name()),
		metricMemoryUsageUsed:     metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector", "used", changefeedID.Keyspace(), changefeedID.Name()),
		metricMemoryUsageMaxRedo:  metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector-redo", "max", changefeedID.Keyspace(), changefeedID.Name()),
		metricMemoryUsageUsedRedo: metrics.DynamicStreamMemoryUsage.WithLabelValues("event-collector-redo", "used", changefeedID.Keyspace(), changefeedID.Name()),
	}
}

func (c *changefeedStat) removeMetrics() {
	metrics.DynamicStreamMemoryUsage.DeleteLabelValues("event-collector", "max", c.changefeedID.Keyspace(), c.changefeedID.Name())
	metrics.DynamicStreamMemoryUsage.DeleteLabelValues("event-collector", "used", c.changefeedID.Keyspace(), c.changefeedID.Name())
	metrics.DynamicStreamMemoryUsage.DeleteLabelValues("event-collector-redo", "max", c.changefeedID.Keyspace(), c.changefeedID.Name())
	metrics.DynamicStreamMemoryUsage.DeleteLabelValues("event-collector-redo", "used", c.changefeedID.Keyspace(), c.changefeedID.Name())
}

type EventCollector struct {
	serverId         node.ID
	dispatcherMap    sync.Map
	changefeedMap    sync.Map
	removeTombstones sync.Map

	mc messaging.MessageCenter

	logCoordinatorClient  *LogCoordinatorClient
	dispatcherMessageChan *chann.DrainableChann[DispatcherMessage]

	receiveChannels     []chan *messaging.TargetMessage
	redoReceiveChannels []chan *messaging.TargetMessage
	ds                  dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]
	redoDs              dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler]

	g      *errgroup.Group
	cancel context.CancelFunc

	metricDispatcherReceivedKVEventCount             prometheus.Counter
	metricDispatcherReceivedResolvedTsEventCount     prometheus.Counter
	metricReceiveEventLagDuration                    prometheus.Observer
	metricRedoDispatcherReceivedKVEventCount         prometheus.Counter
	metricRedoDispatcherReceivedResolvedTsEventCount prometheus.Counter
	metricDSEventChanSize                            prometheus.Gauge
	metricDSPendingQueue                             prometheus.Gauge
	metricDSEventChanSizeRedo                        prometheus.Gauge
	metricDSPendingQueueRedo                         prometheus.Gauge
}

func New(serverId node.ID) *EventCollector {
	receiveChannels := make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency)
	redoReceiveChannels := make([]chan *messaging.TargetMessage, config.DefaultBasicEventHandlerConcurrency)
	for i := 0; i < config.DefaultBasicEventHandlerConcurrency; i++ {
		receiveChannels[i] = make(chan *messaging.TargetMessage, receiveChanSize)
		redoReceiveChannels[i] = make(chan *messaging.TargetMessage, receiveChanSize)
	}
	eventCollector := &EventCollector{
		serverId:                             serverId,
		dispatcherMap:                        sync.Map{},
		dispatcherMessageChan:                chann.NewAutoDrainChann[DispatcherMessage](),
		mc:                                   appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter),
		receiveChannels:                      receiveChannels,
		redoReceiveChannels:                  redoReceiveChannels,
		metricDispatcherReceivedKVEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent", "eventDispatcher"),
		metricDispatcherReceivedResolvedTsEventCount:     metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs", "eventDispatcher"),
		metricReceiveEventLagDuration:                    metrics.EventCollectorReceivedEventLagDuration.WithLabelValues("Msg"),
		metricRedoDispatcherReceivedKVEventCount:         metrics.DispatcherReceivedEventCount.WithLabelValues("KVEvent", "redoDispatcher"),
		metricRedoDispatcherReceivedResolvedTsEventCount: metrics.DispatcherReceivedEventCount.WithLabelValues("ResolvedTs", "redoDispatcher"),
		metricDSEventChanSize:                            metrics.DynamicStreamEventChanSize.WithLabelValues("event-collector"),
		metricDSPendingQueue:                             metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-collector"),
		metricDSEventChanSizeRedo:                        metrics.DynamicStreamEventChanSize.WithLabelValues("event-collector-redo"),
		metricDSPendingQueueRedo:                         metrics.DynamicStreamPendingQueueLen.WithLabelValues("event-collector-redo"),
	}

	eventCollector.logCoordinatorClient = newLogCoordinatorClient(eventCollector)
	eventCollector.ds = NewEventDynamicStream(false)
	eventCollector.redoDs = NewEventDynamicStream(true)
	eventCollector.mc.RegisterHandler(messaging.EventCollectorTopic, eventCollector.MessageCenterHandler)
	eventCollector.mc.RegisterHandler(messaging.RedoEventCollectorTopic, eventCollector.RedoMessageCenterHandler)

	return eventCollector
}

func (c *EventCollector) Run(ctx context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)
	c.g = g
	c.cancel = cancel

	for _, ch := range c.receiveChannels {
		g.Go(func() error {
			return c.runDispatchMessage(ctx, ch, common.DefaultMode)
		})
	}

	for _, ch := range c.redoReceiveChannels {
		g.Go(func() error {
			return c.runDispatchMessage(ctx, ch, common.RedoMode)
		})
	}

	g.Go(func() error { return c.logCoordinatorClient.run(ctx) })
	g.Go(func() error { return c.processDSFeedback(ctx) })
	g.Go(func() error { return c.controlCongestion(ctx) })
	g.Go(func() error { return c.sendDispatcherRequests(ctx) })
	g.Go(func() error { return c.reconcileDispatcherControl(ctx) })
	g.Go(func() error { return c.updateMetrics(ctx) })

	log.Info("event collector is running")
}

func (c *EventCollector) Close() {
	log.Info("event collector is closing")
	c.cancel()
	_ = c.g.Wait()
	c.redoDs.Close()
	c.ds.Close()
	log.Info("event collector is closed")
}

func (c *EventCollector) AddDispatcher(target dispatcher.DispatcherService, memoryQuota uint64) {
	c.PrepareAddDispatcher(target, memoryQuota, nil)
	c.logCoordinatorClient.requestReusableEventService(target)
}

func (c *EventCollector) HasDispatcher(dispatcherID common.DispatcherID) bool {
	_, ok := c.dispatcherMap.Load(dispatcherID)
	return ok
}

func (c *EventCollector) PrepareAddDispatcher(
	target dispatcher.DispatcherService,
	memoryQuota uint64,
	readyCallback func(),
) {
	changefeedID := target.GetChangefeedID()
	log.Info("add dispatcher", zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcher", target.GetId()))
	defer func() {
		log.Info("add dispatcher done",
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", target.GetId()), zap.Int64("tableID", target.GetTableSpan().GetTableID()),
			zap.Uint64("startTs", target.GetStartTs()), zap.Int64("mode", target.GetMode()))
	}()
	metrics.EventCollectorRegisteredDispatcherCount.Inc()

	stat := newDispatcherStat(target, c, readyCallback)
	c.dispatcherMap.Store(target.GetId(), stat)

	v, _ := c.changefeedMap.LoadOrStore(changefeedID.ID(), newChangefeedStat(changefeedID))
	cfStat := v.(*changefeedStat)
	cfStat.dispatcherCount.Add(1)

	ds := c.getDynamicStream(target.GetMode())
	areaSetting := dynstream.NewAreaSettingsWithMaxPendingSize(memoryQuota, dynstream.MemoryControlForEventCollector, "eventCollector")
	err := ds.AddPath(target.GetId(), stat, areaSetting)
	if err != nil {
		log.Warn("add dispatcher to dynamic stream failed", zap.Error(err))
	}
	stat.run()
}

func (c *EventCollector) CommitAddDispatcher(target dispatcher.DispatcherService, startTs uint64) {
	changefeedID := target.GetChangefeedID()
	log.Info("commit add dispatcher", zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", target.GetId()),
		zap.Int64("tableID", target.GetTableSpan().GetTableID()), zap.Uint64("startTs", startTs))
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		log.Warn("dispatcher not found when commit add dispatcher",
			zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", target.GetId()),
			zap.Int64("tableID", target.GetTableSpan().GetTableID()), zap.Uint64("startTs", startTs))
		return
	}
	stat := value.(*dispatcherStat)
	stat.commitReady(c.getLocalServerID())
}

func (c *EventCollector) RemoveDispatcher(target dispatcher.DispatcherService) {
	changefeedID := target.GetChangefeedID()
	log.Info("remove dispatcher", zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", target.GetId()))
	defer func() {
		log.Info("remove dispatcher done",
			zap.Stringer("changefeedID", changefeedID), zap.Stringer("dispatcherID", target.GetId()),
			zap.Int64("tableID", target.GetTableSpan().GetTableID()))
	}()
	value, ok := c.dispatcherMap.Load(target.GetId())
	if !ok {
		return
	}
	stat := value.(*dispatcherStat)
	stat.remove()

	ds := c.getDynamicStream(target.GetMode())
	err := ds.RemovePath(target.GetId())
	if err != nil {
		log.Error("remove dispatcher from dynamic stream failed", zap.Error(err))
	}
	c.dispatcherMap.Delete(target.GetId())

	v, ok := c.changefeedMap.Load(changefeedID.ID())
	if !ok {
		log.Warn("changefeed stat not found when removing dispatcher", zap.Stringer("changefeedID", changefeedID))
		return
	}
	cfStat := v.(*changefeedStat)
	remaining := cfStat.dispatcherCount.Add(-1)
	log.Info("remove dispatcher from changefeed stat",
		zap.Stringer("changefeedID", changefeedID),
		zap.Int32("remaining", remaining))
	if remaining == 0 {
		if _, ok := c.changefeedMap.LoadAndDelete(changefeedID.ID()); ok {
			stat := v.(*changefeedStat)
			stat.removeMetrics()
			log.Info("last dispatcher removed, clean up changefeed stat", zap.Stringer("changefeedID", target.GetChangefeedID()))
		}
	}
}

func (c *EventCollector) controlCongestion(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			messages := c.newCongestionControlMessages()
			for serverID, m := range messages {
				if len(m.GetAvailables()) != 0 {
					msg := messaging.NewSingleTargetMessage(serverID, messaging.EventServiceTopic, m)
					if err := c.mc.SendCommand(msg); err != nil {
						log.Warn("send congestion control message failed", zap.Error(err))
					}
				}
			}
		}
	}
}

func (c *EventCollector) newCongestionControlMessages() map[node.ID]*event.CongestionControl {
	changefeedMemoryReleaseCount := make(map[common.ChangeFeedID]uint32)
	getAndResetMemoryReleaseCount := func(changefeedID common.ChangeFeedID) uint32 {
		if count, ok := changefeedMemoryReleaseCount[changefeedID]; ok {
			return count
		}
		v, ok := c.changefeedMap.Load(changefeedID.ID())
		if !ok {
			return 0
		}
		count := v.(*changefeedStat).memoryReleaseCount.Swap(0)
		changefeedMemoryReleaseCount[changefeedID] = count
		return count
	}

	changefeedPathMemory := make(map[common.ChangeFeedID]map[common.DispatcherID]uint64)
	changefeedTotalMemory := make(map[common.ChangeFeedID]uint64)
	changefeedUsageRatio := make(map[common.ChangeFeedID]float64)

	for _, quota := range c.ds.GetMetrics().MemoryControl.AreaMemoryMetrics {
		statValue, ok := c.changefeedMap.Load(quota.Area())
		if !ok {
			continue
		}
		cfID := statValue.(*changefeedStat).changefeedID
		if changefeedPathMemory[cfID] == nil {
			changefeedPathMemory[cfID] = make(map[common.DispatcherID]uint64)
		}
		for dispatcherID, available := range quota.PathMetrics() {
			changefeedPathMemory[cfID][dispatcherID] = uint64(available)
		}
		changefeedTotalMemory[cfID] = uint64(quota.AvailableMemory())
		changefeedUsageRatio[cfID] = calcUsageRatio(quota.MemoryUsage(), quota.MaxMemory())
	}

	for _, quota := range c.redoDs.GetMetrics().MemoryControl.AreaMemoryMetrics {
		statValue, ok := c.changefeedMap.Load(quota.Area())
		if !ok {
			continue
		}
		cfID := statValue.(*changefeedStat).changefeedID
		if changefeedPathMemory[cfID] == nil {
			changefeedPathMemory[cfID] = make(map[common.DispatcherID]uint64)
		}
		for dispatcherID, available := range quota.PathMetrics() {
			if existing, exists := changefeedPathMemory[cfID][dispatcherID]; exists {
				changefeedPathMemory[cfID][dispatcherID] = min(existing, uint64(available))
			} else {
				changefeedPathMemory[cfID][dispatcherID] = uint64(available)
			}
		}
		updateMinUint64MapValue(changefeedTotalMemory, cfID, uint64(quota.AvailableMemory()))
		changefeedUsageRatio[cfID] = max(changefeedUsageRatio[cfID], calcUsageRatio(quota.MemoryUsage(), quota.MaxMemory()))
	}

	if len(changefeedPathMemory) == 0 {
		return nil
	}

	nodeDispatcherMemory := make(map[node.ID]map[common.ChangeFeedID]map[common.DispatcherID]uint64)

	c.dispatcherMap.Range(func(k, v interface{}) bool {
		stat := v.(*dispatcherStat)
		eventServiceID := stat.connState.getEventServiceID()
		if eventServiceID == "" {
			return true
		}

		dispatcherID := stat.target.GetId()
		changefeedID := stat.target.GetChangefeedID()

		if nodeDispatcherMemory[eventServiceID] == nil {
			nodeDispatcherMemory[eventServiceID] = make(map[common.ChangeFeedID]map[common.DispatcherID]uint64)
		}
		if nodeDispatcherMemory[eventServiceID][changefeedID] == nil {
			nodeDispatcherMemory[eventServiceID][changefeedID] = make(map[common.DispatcherID]uint64)
		}

		if pathMemory, exists := changefeedPathMemory[changefeedID][dispatcherID]; exists {
			nodeDispatcherMemory[eventServiceID][changefeedID][dispatcherID] = uint64(pathMemory)
		}
		return true
	})

	result := make(map[node.ID]*event.CongestionControl)
	for nodeID, changefeedDispatchers := range nodeDispatcherMemory {
		congestionControl := event.NewCongestionControlWithVersion(event.CongestionControlVersion2)

		for changefeedID, dispatcherMemory := range changefeedDispatchers {
			if len(dispatcherMemory) == 0 {
				continue
			}

			totalAvailable, ok := changefeedTotalMemory[changefeedID]
			if !ok {
				continue
			}
			congestionControl.AddAvailableMemoryWithDispatchersAndUsageAndReleaseCount(
				changefeedID.ID(),
				totalAvailable,
				changefeedUsageRatio[changefeedID],
				dispatcherMemory,
				getAndResetMemoryReleaseCount(changefeedID),
			)
		}

		if len(congestionControl.GetAvailables()) > 0 {
			result[nodeID] = congestionControl
		}
	}
	return result
}

func updateMinUint64MapValue(m map[common.ChangeFeedID]uint64, key common.ChangeFeedID, value uint64) {
	if existing, exists := m[key]; exists {
		m[key] = min(existing, value)
	} else {
		m[key] = value
	}
}

func updateMaxUint64MapValue(m map[common.ChangeFeedID]uint64, key common.ChangeFeedID, value uint64) {
	if existing, exists := m[key]; exists {
		m[key] = max(existing, value)
	} else {
		m[key] = value
	}
}

func calcUsageRatio(usedMemory int64, maxMemory int64) float64 {
	if maxMemory <= 0 {
		return 0
	}
	ratio := float64(usedMemory) / float64(maxMemory)
	if ratio < 0 {
		return 0
	}
	if ratio > 1 {
		return 1
	}
	return ratio
}

func (c *EventCollector) updateMetrics(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	updateMetric := func(mode int64) {
		ds := c.getDynamicStream(mode)
		dsMetrics := ds.GetMetrics()
		if common.IsRedoMode(mode) {
			c.metricDSEventChanSizeRedo.Set(float64(dsMetrics.EventChanSize))
			c.metricDSPendingQueueRedo.Set(float64(dsMetrics.PendingQueueLen))
		} else {
			c.metricDSEventChanSize.Set(float64(dsMetrics.EventChanSize))
			c.metricDSPendingQueue.Set(float64(dsMetrics.PendingQueueLen))
		}
		for _, areaMetric := range dsMetrics.MemoryControl.AreaMemoryMetrics {
			statValue, ok := c.changefeedMap.Load(areaMetric.Area())
			if !ok {
				continue
			}
			stat := statValue.(*changefeedStat)
			if common.IsRedoMode(mode) {
				stat.metricMemoryUsageMaxRedo.Set(float64(areaMetric.MaxMemory()))
				stat.metricMemoryUsageUsedRedo.Set(float64(areaMetric.MemoryUsage()))
			} else {
				stat.metricMemoryUsageMax.Set(float64(areaMetric.MaxMemory()))
				stat.metricMemoryUsageUsed.Set(float64(areaMetric.MemoryUsage()))
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			updateMetric(common.DefaultMode)
			updateMetric(common.RedoMode)
		}
	}
}

func (c *EventCollector) getDynamicStream(mode int64) dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherEvent, *dispatcherStat, *EventsHandler] {
	if common.IsRedoMode(mode) {
		return c.redoDs
	}
	return c.ds
}

func (c *EventCollector) getMetric(mode int64) (prometheus.Counter, prometheus.Counter) {
	if common.IsRedoMode(mode) {
		return c.metricRedoDispatcherReceivedKVEventCount, c.metricRedoDispatcherReceivedResolvedTsEventCount
	}
	return c.metricDispatcherReceivedKVEventCount, c.metricDispatcherReceivedResolvedTsEventCount
}
