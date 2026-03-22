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

package eventservice

import (
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

func (c *eventBroker) addDispatcher(info DispatcherInfo) error {
	id := info.GetID()
	span := info.GetTableSpan()
	changefeedID := info.GetChangefeedID()
	target := node.ID(info.GetServerID())
	if statPtr := c.getDispatcher(id); statPtr != nil {
		current := statPtr.Load()
		switch {
		case current.epoch > info.GetEpoch():
			c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusStale, 0)
			return nil
		case current.epoch == info.GetEpoch():
			c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusAccepted, 0)
			return nil
		}
	}

	status := c.getOrSetChangefeedStatus(changefeedID, info.GetSyncPointInterval())
	dispatcher := newDispatcherStat(info, uint64(len(c.taskChan)), uint64(len(c.messageCh)), nil, status)
	dispatcherPtr := &atomic.Pointer[dispatcherStat]{}
	dispatcherPtr.Store(dispatcher)
	status.addDispatcher(id, dispatcherPtr)
	if span.Equal(common.KeyspaceDDLSpan(span.KeyspaceID)) {
		c.tableTriggerDispatchers.Store(id, dispatcherPtr)
		c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusAccepted, 0)
		log.Info("table trigger dispatcher register dispatcher",
			zap.Uint64("clusterID", c.tidbClusterID),
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("startTs", info.GetStartTs()))
		return nil
	}

	start := time.Now()
	success := c.eventStore.RegisterDispatcher(
		changefeedID,
		id,
		span,
		info.GetStartTs(),
		func(resolvedTs uint64, latestCommitTs uint64) {
			d := dispatcherPtr.Load()
			if d.isRemoved.Load() {
				return
			}
			c.onNotify(d, resolvedTs, latestCommitTs)
		},
		info.IsOnlyReuse(),
		info.GetBdrMode(),
	)

	if !success {
		if !info.IsOnlyReuse() {
			log.Error("register dispatcher to eventStore failed",
				zap.Stringer("changefeedID", changefeedID),
				zap.Stringer("dispatcherID", id), zap.Int64("tableID", span.GetTableID()),
				zap.Uint64("startTs", info.GetStartTs()), zap.String("span", common.FormatTableSpan(span)))
		}
		status.removeDispatcher(id)
		if status.isEmpty() {
			c.changefeedMap.Delete(changefeedID)
			metrics.EventServiceAvailableMemoryQuotaGaugeVec.DeleteLabelValues(changefeedID.String())
			metrics.EventServiceScanWindowBaseTsGaugeVec.DeleteLabelValues(changefeedID.String())
			metrics.EventServiceScanWindowIntervalGaugeVec.DeleteLabelValues(changefeedID.String())
		}
		c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusRejected, 1)
		c.sendNotReusableEvent(node.ID(info.GetServerID()), dispatcher)
		return nil
	}

	keyspaceMeta := common.KeyspaceMeta{
		ID:   span.KeyspaceID,
		Name: changefeedID.Keyspace(),
	}
	err := c.schemaStore.RegisterTable(keyspaceMeta, span.GetTableID(), info.GetStartTs())
	if err != nil {
		log.Error("register table to schemaStore failed",
			zap.Uint32("keyspaceID", span.KeyspaceID),
			zap.Stringer("dispatcherID", id), zap.Int64("tableID", span.GetTableID()),
			zap.Uint64("startTs", info.GetStartTs()), zap.String("span", common.FormatTableSpan(span)),
			zap.Error(err),
		)
		dispatcher.isRemoved.Store(true)
		c.eventStore.UnregisterDispatcher(changefeedID, id)
		status.removeDispatcher(id)
		if status.isEmpty() {
			c.changefeedMap.Delete(changefeedID)
			metrics.EventServiceAvailableMemoryQuotaGaugeVec.DeleteLabelValues(changefeedID.String())
			metrics.EventServiceScanWindowBaseTsGaugeVec.DeleteLabelValues(changefeedID.String())
			metrics.EventServiceScanWindowIntervalGaugeVec.DeleteLabelValues(changefeedID.String())
		}
		c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusRejected, 2)
		return err
	}
	c.dispatchers.Store(id, dispatcherPtr)
	c.metricsCollector.metricDispatcherCount.Inc()
	c.sendDispatcherControlEvent(target, id, info.GetEpoch(), event.DispatcherControlActionRegister, event.DispatcherControlStatusAccepted, 0)
	log.Info("register dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID),
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("dispatcherID", id),
		zap.Int64("mode", info.GetMode()),
		zap.Int64("tableID", span.GetTableID()),
		zap.String("span", common.FormatTableSpan(span)),
		zap.Uint64("startTs", info.GetStartTs()),
		zap.String("txnAtomocity", string(info.GetTxnAtomicity())),
		zap.Duration("duration", time.Since(start)))
	return nil
}

func (c *eventBroker) removeDispatcher(dispatcherInfo DispatcherInfo) {
	id := dispatcherInfo.GetID()
	target := node.ID(dispatcherInfo.GetServerID())

	var isTableTriggerDispatcher bool
	statPtr, ok := c.dispatchers.Load(id)
	if !ok {
		statPtr, ok = c.tableTriggerDispatchers.Load(id)
		if !ok {
			c.sendDispatcherControlEvent(target, id, dispatcherInfo.GetEpoch(), event.DispatcherControlActionRemove, event.DispatcherControlStatusNotFound, 0)
			return
		}
		isTableTriggerDispatcher = true
	}

	stat := statPtr.(*atomic.Pointer[dispatcherStat]).Load()
	if dispatcherInfo.GetEpoch() != 0 && stat.epoch > dispatcherInfo.GetEpoch() {
		c.sendDispatcherControlEvent(target, id, dispatcherInfo.GetEpoch(), event.DispatcherControlActionRemove, event.DispatcherControlStatusStale, 0)
		return
	}
	stat.isRemoved.Store(true)

	if isTableTriggerDispatcher {
		c.tableTriggerDispatchers.Delete(id)
	} else {
		c.dispatchers.Delete(id)
	}

	stat.changefeedStat.removeDispatcher(id)
	c.metricsCollector.metricDispatcherCount.Dec()
	changefeedID := dispatcherInfo.GetChangefeedID()

	if stat.changefeedStat.isEmpty() {
		log.Info("All dispatchers for the changefeed are removed, remove the changefeed status",
			zap.Stringer("changefeedID", changefeedID),
		)
		c.changefeedMap.Delete(changefeedID)
		metrics.EventServiceAvailableMemoryQuotaGaugeVec.DeleteLabelValues(changefeedID.String())
		metrics.EventServiceScanWindowBaseTsGaugeVec.DeleteLabelValues(changefeedID.String())
		metrics.EventServiceScanWindowIntervalGaugeVec.DeleteLabelValues(changefeedID.String())
	}

	c.eventStore.UnregisterDispatcher(changefeedID, id)

	span := dispatcherInfo.GetTableSpan()
	keyspaceMeta := common.KeyspaceMeta{
		ID:   span.KeyspaceID,
		Name: changefeedID.Keyspace(),
	}
	c.schemaStore.UnregisterTable(keyspaceMeta, span.TableID)

	log.Info("remove dispatcher",
		zap.Uint64("clusterID", c.tidbClusterID), zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("dispatcherID", id), zap.Int64("tableID", dispatcherInfo.GetTableSpan().GetTableID()),
		zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
	)
	c.sendDispatcherControlEvent(target, id, dispatcherInfo.GetEpoch(), event.DispatcherControlActionRemove, event.DispatcherControlStatusAccepted, 0)
}

func (c *eventBroker) resetDispatcher(dispatcherInfo DispatcherInfo) error {
	dispatcherID := dispatcherInfo.GetID()
	start := time.Now()
	target := node.ID(dispatcherInfo.GetServerID())
	statPtr := c.getDispatcher(dispatcherID)
	if statPtr == nil {
		log.Warn("reset a non-exist dispatcher, ignore it",
			zap.Stringer("changefeedID", dispatcherInfo.GetChangefeedID()),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", dispatcherInfo.GetTableSpan().GetTableID()),
			zap.String("span", common.FormatTableSpan(dispatcherInfo.GetTableSpan())),
			zap.Uint64("startTs", dispatcherInfo.GetStartTs()))
		c.sendDispatcherControlEvent(target, dispatcherID, dispatcherInfo.GetEpoch(), event.DispatcherControlActionReset, event.DispatcherControlStatusNotFound, 0)
		return nil
	}
	metrics.EventServiceResetDispatcherCount.Inc()

	oldStat := statPtr.Load()
	if oldStat.epoch > dispatcherInfo.GetEpoch() {
		c.sendDispatcherControlEvent(target, dispatcherID, dispatcherInfo.GetEpoch(), event.DispatcherControlActionReset, event.DispatcherControlStatusStale, 0)
		return nil
	}
	if oldStat.epoch == dispatcherInfo.GetEpoch() {
		c.sendDispatcherControlEvent(target, dispatcherID, dispatcherInfo.GetEpoch(), event.DispatcherControlActionReset, event.DispatcherControlStatusAccepted, 0)
		return nil
	}

	oldStat.isRemoved.Store(true)

	changefeedID := dispatcherInfo.GetChangefeedID()
	span := dispatcherInfo.GetTableSpan()
	var tableInfo *common.TableInfo
	if !span.Equal(common.KeyspaceDDLSpan(span.KeyspaceID)) {
		var err error
		keyspaceMeta := common.KeyspaceMeta{
			ID:   span.KeyspaceID,
			Name: changefeedID.Keyspace(),
		}
		tableInfo, err = c.schemaStore.GetTableInfo(keyspaceMeta, span.GetTableID(), dispatcherInfo.GetStartTs())
		if err != nil {
			log.Error("get table info from schemaStore failed",
				zap.Stringer("changefeedID", changefeedID),
				zap.Stringer("dispatcherID", dispatcherID),
				zap.Int64("tableID", span.GetTableID()),
				zap.Uint64("startTs", dispatcherInfo.GetStartTs()),
				zap.String("span", common.FormatTableSpan(span)),
				zap.Error(err))
			c.sendDispatcherControlEvent(target, dispatcherID, dispatcherInfo.GetEpoch(), event.DispatcherControlActionReset, event.DispatcherControlStatusRejected, 3)
			return err
		}
	}
	status := c.getOrSetChangefeedStatus(changefeedID, dispatcherInfo.GetSyncPointInterval())

	newStat := newDispatcherStat(dispatcherInfo, uint64(len(c.taskChan)), uint64(len(c.messageCh)), tableInfo, status)
	newStat.copyStatistics(oldStat)

	for {
		if statPtr.CompareAndSwap(oldStat, newStat) {
			status.addDispatcher(dispatcherID, statPtr)
			break
		}
		log.Warn("reset dispatcher failed since the dispatcher is changed concurrently",
			zap.Stringer("changefeedID", changefeedID),
			zap.Stringer("dispatcherID", dispatcherID),
			zap.Int64("tableID", span.GetTableID()),
			zap.String("span", common.FormatTableSpan(span)),
			zap.Uint64("oldStartTs", oldStat.info.GetStartTs()),
			zap.Uint64("newStartTs", dispatcherInfo.GetStartTs()),
			zap.Uint64("oldEpoch", oldStat.epoch),
			zap.Uint64("newEpoch", newStat.epoch))
		oldStat = statPtr.Load()
		if oldStat.epoch >= dispatcherInfo.GetEpoch() {
			return nil
		}
		oldStat.isRemoved.Store(true)
	}

	log.Info("reset dispatcher",
		zap.Stringer("changefeedID", newStat.changefeedStat.changefeedID),
		zap.Stringer("dispatcherID", newStat.id), zap.Int64("tableID", newStat.info.GetTableSpan().GetTableID()),
		zap.String("span", common.FormatTableSpan(newStat.info.GetTableSpan())),
		zap.Uint64("originStartTs", oldStat.info.GetStartTs()),
		zap.Uint64("newStartTs", dispatcherInfo.GetStartTs()),
		zap.Uint64("newEpoch", newStat.epoch),
		zap.Duration("resetTime", time.Since(start)))
	c.sendDispatcherControlEvent(target, dispatcherID, dispatcherInfo.GetEpoch(), event.DispatcherControlActionReset, event.DispatcherControlStatusAccepted, 0)

	return nil
}

func (c *eventBroker) getOrSetChangefeedStatus(changefeedID common.ChangeFeedID, syncPointInterval time.Duration) *changefeedStatus {
	stat, ok := c.changefeedMap.Load(changefeedID)
	if !ok {
		stat = newChangefeedStatus(changefeedID, syncPointInterval)
		log.Info("new changefeed status", zap.Stringer("changefeedID", changefeedID))
		c.changefeedMap.Store(changefeedID, stat)
		metrics.EventServiceScanWindowBaseTsGaugeVec.WithLabelValues(changefeedID.String()).Set(0)
		metrics.EventServiceScanWindowIntervalGaugeVec.WithLabelValues(changefeedID.String()).Set(defaultScanInterval.Seconds())
	}
	return stat.(*changefeedStatus)
}

func (c *eventBroker) handleDispatcherHeartbeat(heartbeat *DispatcherHeartBeatWithServerID) {
	responseMap := make(map[string]*event.DispatcherHeartbeatResponse)
	now := time.Now().Unix()
	for _, dp := range heartbeat.heartbeat.DispatcherProgresses {
		dispatcherPtr := c.getDispatcher(dp.DispatcherID)
		if dispatcherPtr == nil {
			response, ok := responseMap[heartbeat.serverID]
			if !ok {
				response = event.NewDispatcherHeartbeatResponse()
				response.Incarnation = c.incarnation
				responseMap[heartbeat.serverID] = response
			}
			response.Append(event.NewDispatcherState(dp.DispatcherID, event.DSStateRemoved))
			continue
		}
		dispatcher := dispatcherPtr.Load()
		if dispatcher.checkpointTs.Load() < dp.CheckpointTs {
			dispatcher.checkpointTs.Store(dp.CheckpointTs)
		}
		dispatcher.lastReceivedHeartbeatTime.Store(now)
	}
	c.sendDispatcherResponse(responseMap)
}

func (c *eventBroker) handleCongestionControl(from node.ID, m *event.CongestionControl) {
	availables := m.GetAvailables()
	if len(availables) == 0 {
		return
	}

	holder := make(map[common.GID]uint64, len(availables))
	usage := make(map[common.GID]float64, len(availables))
	memoryRelease := make(map[common.GID]uint32, len(availables))
	dispatcherAvailable := make(map[common.DispatcherID]uint64, len(availables))
	for _, item := range availables {
		holder[item.Gid] = item.Available
		if m.HasUsageRatio() {
			usage[item.Gid] = item.UsageRatio
		}
		memoryRelease[item.Gid] = item.MemoryReleaseCount
		for dispatcherID, available := range item.DispatcherAvailable {
			dispatcherAvailable[dispatcherID] = available
		}
	}

	now := time.Now()
	c.changefeedMap.Range(func(k, v interface{}) bool {
		changefeedID := k.(common.ChangeFeedID)
		changefeed := v.(*changefeedStatus)
		availableInMsg, ok := holder[changefeedID.ID()]
		if ok {
			changefeed.availableMemoryQuota.Store(from, atomic.NewUint64(availableInMsg))
			metrics.EventServiceAvailableMemoryQuotaGaugeVec.WithLabelValues(changefeedID.String()).Set(float64(availableInMsg))
		}
		if m.HasUsageRatio() {
			if ratio, okUsage := usage[changefeedID.ID()]; okUsage && ok {
				changefeed.updateMemoryUsage(now, ratio, memoryRelease[changefeedID.ID()])
			}
		}
		return true
	})

	c.dispatchers.Range(func(k, v interface{}) bool {
		dispatcherID := k.(common.DispatcherID)
		dispatcher := v.(*atomic.Pointer[dispatcherStat]).Load()
		available, ok := dispatcherAvailable[dispatcherID]
		if ok {
			dispatcher.availableMemoryQuota.Store(available)
		}
		return true
	})
}

func (c *eventBroker) sendDispatcherResponse(responseMap map[string]*event.DispatcherHeartbeatResponse) {
	for serverID, response := range responseMap {
		msg := messaging.NewSingleTargetMessage(node.ID(serverID), messaging.EventCollectorTopic, response)
		c.msgSender.SendCommand(msg)
	}
}
