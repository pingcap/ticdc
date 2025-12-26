// Copyright 2025 PingCAP, Inc.
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

package dispatchermanager

import (
	"context"
	"math"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/metrics"
	pkgRedo "github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

func initRedoComponet(
	ctx context.Context,
	manager *DispatcherManager,
	changefeedID common.ChangeFeedID,
	tableTriggerRedoDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	newChangefeed bool,
) error {
	if manager.config.Consistent == nil || !pkgRedo.IsConsistentEnabled(util.GetOrZero(manager.config.Consistent.Level)) {
		return nil
	}
	manager.RedoEnable = true
	manager.redoDispatcherMap = newDispatcherMap[*dispatcher.RedoDispatcher]()
	manager.redoSink = redo.New(ctx, changefeedID, startTs, manager.config.Consistent)
	manager.redoSchemaIDToDispatchers = dispatcher.NewSchemaIDToDispatchers()

	totalQuota := manager.sinkQuota
	consistentMemoryUsage := manager.config.Consistent.MemoryUsage
	if consistentMemoryUsage == nil {
		consistentMemoryUsage = config.GetDefaultReplicaConfig().Consistent.MemoryUsage
	}
	manager.redoQuota = totalQuota * consistentMemoryUsage.MemoryQuotaPercentage / 100
	manager.sinkQuota = totalQuota - manager.redoQuota

	// init table trigger redo dispatcher when tableTriggerRedoDispatcherID is not nil
	if tableTriggerRedoDispatcherID != nil {
		err := manager.NewTableTriggerRedoDispatcher(tableTriggerRedoDispatcherID, startTs, newChangefeed)
		if err != nil {
			return err
		}
	}
	// register redo metrics
	manager.metricTableTriggerRedoDispatcherCount = metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher")
	manager.metricRedoEventDispatcherCount = metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher")
	manager.metricRedoCreateDispatcherDuration = metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher")

	// RedoMessageDs need register on every node
	appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterRedoMessageDs(manager)
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		err := manager.redoSink.Run(ctx)
		manager.handleError(ctx, err)
	}()
	return nil
}

func (e *DispatcherManager) NewTableTriggerRedoDispatcher(id *heartbeatpb.DispatcherID, startTs uint64, newChangefeed bool) error {
	if e.GetTableTriggerEventDispatcher() != nil {
		log.Error("table trigger redo dispatcher existed!")
	}
	infos := map[common.DispatcherID]dispatcherCreateInfo{}
	dispatcherID := common.NewDispatcherIDFromPB(id)
	infos[dispatcherID] = dispatcherCreateInfo{
		Id:        dispatcherID,
		TableSpan: common.KeyspaceDDLSpan(e.keyspaceID),
		StartTs:   startTs,
		SchemaID:  0,
	}
	err := e.newRedoDispatchers(infos, newChangefeed)
	if err != nil {
		return errors.Trace(err)
	}
	// redo meta should keep the same node with table trigger event dispatcher
	// table trigger event dispatcher and table trigger redo dispatcher must exist on the same node
	redoDispatcher := e.GetTableTriggerRedoDispatcher()
	redoDispatcher.SetRedoMeta(e.config.Consistent)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := e.collectRedoMeta(e.ctx)
		e.handleError(e.ctx, err)
	}()
	log.Info("table trigger redo dispatcher created",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", redoDispatcher.GetId()),
		zap.Uint64("startTs", redoDispatcher.GetStartTs()),
	)
	return nil
}

func (e *DispatcherManager) newRedoDispatchers(infos map[common.DispatcherID]dispatcherCreateInfo, removeDDLTs bool) error {
	start := time.Now()

	dispatcherIds, _, startTsList, tableSpans, schemaIds := prepareCreateDispatcher(infos, e.redoDispatcherMap)
	if len(dispatcherIds) == 0 {
		return nil
	}

	if e.latestRedoWatermark.Get().CheckpointTs == 0 {
		// If the checkpointTs is 0, means there is no dispatchers before. So we need to init it with the smallest startTs of these dispatchers
		smallestStartTs := int64(math.MaxInt64)
		for _, startTs := range startTsList {
			if startTs < smallestStartTs {
				smallestStartTs = startTs
			}
		}
		e.latestRedoWatermark.Set(&heartbeatpb.Watermark{
			CheckpointTs: uint64(smallestStartTs),
			ResolvedTs:   uint64(smallestStartTs),
		})
	}

	for idx, id := range dispatcherIds {
		rd := dispatcher.NewRedoDispatcher(
			id,
			tableSpans[idx],
			uint64(startTsList[idx]),
			schemaIds[idx],
			e.redoSchemaIDToDispatchers,
			false, // skipSyncpointAtStartTs
			e.redoSink,
			e.sharedInfo,
		)
		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		if rd.IsTableTriggerEventDispatcher() {
			e.SetTableTriggerRedoDispatcher(rd)
		} else {
			e.redoSchemaIDToDispatchers.Set(schemaIds[idx], id)
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(rd, e.redoQuota)
		}

		redoSeq := e.redoDispatcherMap.Set(rd.GetId(), rd)
		rd.SetSeq(redoSeq)

		if rd.IsTableTriggerEventDispatcher() {
			e.metricTableTriggerRedoDispatcherCount.Inc()
		} else {
			e.metricRedoEventDispatcherCount.Inc()
		}

		log.Info("new redo dispatcher created",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("tableSpan", common.FormatTableSpan(tableSpans[idx])),
			zap.Int64("startTs", startTsList[idx]))
	}
	e.metricRedoCreateDispatcherDuration.Observe(time.Since(start).Seconds() / float64(len(dispatcherIds)))
	log.Info("batch create new redo dispatchers",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Int("count", len(dispatcherIds)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

func (e *DispatcherManager) mergeRedoDispatcher(dispatcherIDs []common.DispatcherID, mergedDispatcherID common.DispatcherID) *MergeCheckTask {
	// Step 1: check the dispatcherIDs and mergedDispatcherID are valid:
	//         1. whether the mergedDispatcherID is not exist in the dispatcherMap
	//         2. whether the dispatcherIDs exist in the dispatcherMap
	//         3. whether the dispatcherIDs belong to the same table
	//         4. whether the dispatcherIDs have consecutive ranges
	//         5. whether the dispatcher in working status.
	ok := prepareMergeDispatcher(e.changefeedID, dispatcherIDs, e.redoDispatcherMap, mergedDispatcherID, e.sharedInfo.GetStatusesChan())
	if !ok {
		return nil
	}

	mergedSpan, fakeStartTs, schemaID := createMergedSpan(e.changefeedID, dispatcherIDs, e.redoDispatcherMap)
	if mergedSpan == nil {
		return nil
	}

	mergedDispatcher := dispatcher.NewRedoDispatcher(
		mergedDispatcherID,
		mergedSpan,
		fakeStartTs, // real startTs will be calculated later.
		schemaID,
		e.redoSchemaIDToDispatchers,
		false, // skipSyncpointAtStartTs
		e.redoSink,
		e.sharedInfo,
	)

	log.Info("new redo dispatcher created(merge dispatcher)",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", mergedDispatcherID),
		zap.String("tableSpan", common.FormatTableSpan(mergedSpan)))

	registerMergeDispatcher(e.changefeedID, dispatcherIDs, e.redoDispatcherMap, mergedDispatcherID, mergedDispatcher, e.redoSchemaIDToDispatchers, e.metricRedoEventDispatcherCount, e.redoQuota)
	return newMergeCheckTask(e, mergedDispatcher, dispatcherIDs)
}

func (e *DispatcherManager) cleanRedoDispatcher(id common.DispatcherID, schemaID int64) {
	e.redoDispatcherMap.Delete(id)
	e.redoSchemaIDToDispatchers.Delete(schemaID, id)
	tableTriggerRedoDispatcher := e.GetTableTriggerEventDispatcher()
	if tableTriggerRedoDispatcher != nil && tableTriggerRedoDispatcher.GetId() == id {
		e.SetTableTriggerRedoDispatcher(nil)
		e.metricTableTriggerRedoDispatcherCount.Dec()
	} else {
		e.metricRedoEventDispatcherCount.Dec()
	}
	log.Info("redo dispatcher completely stopped, and delete it from event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", id),
	)
}

func (e *DispatcherManager) closeRedoMeta(removeChangefeed bool) {
	if removeChangefeed && e.GetTableTriggerRedoDispatcher() != nil {
		redoMeta := e.GetTableTriggerRedoDispatcher().GetRedoMeta()
		if redoMeta != nil {
			redoMeta.Cleanup(context.Background())
		}
	}
}

func (e *DispatcherManager) InitalizeTableTriggerRedoDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
	if e.GetTableTriggerRedoDispatcher() == nil {
		return nil
	}
	needAddDispatcher, err := e.GetTableTriggerRedoDispatcher().InitializeTableSchemaStore(schemaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if !needAddDispatcher {
		return nil
	}
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.GetTableTriggerRedoDispatcher(), e.redoQuota)
	return nil
}

func (e *DispatcherManager) UpdateRedoMeta(checkpointTs, resolvedTs uint64) {
	// only update meta on the one node
	if e.GetTableTriggerRedoDispatcher() != nil {
		e.GetTableTriggerRedoDispatcher().UpdateMeta(checkpointTs, resolvedTs)
		return
	}
	log.Error("should not reach here. only update redo meta on the tableTriggerRedoDispatcher")
}

func (e *DispatcherManager) SetRedoResolvedTs(resolvedTs uint64) bool {
	return util.CompareAndMonotonicIncrease(&e.redoGlobalTs, resolvedTs)
}

func (e *DispatcherManager) collectRedoMeta(ctx context.Context) error {
	ticker := time.NewTicker(time.Duration(*e.config.Consistent.FlushIntervalInMs))
	defer ticker.Stop()
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
	var preResolvedTs uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if e.GetTableTriggerRedoDispatcher() == nil {
				log.Error("should not reach here. only collect redo meta on the tableTriggerRedoDispatcher")
				continue
			}
			logMeta := e.GetTableTriggerRedoDispatcher().GetFlushedMeta()
			if preResolvedTs >= logMeta.ResolvedTs {
				continue
			}
			err := mc.SendCommand(
				messaging.NewSingleTargetMessage(
					e.GetMaintainerID(),
					messaging.MaintainerManagerTopic,
					&heartbeatpb.RedoResolvedTsProgressMessage{
						ChangefeedID: e.changefeedID.ToPB(),
						ResolvedTs:   logMeta.ResolvedTs,
					},
				))
			if err != nil {
				log.Error("failed to send redo request message", zap.Error(err))
			}
			preResolvedTs = logMeta.ResolvedTs
		}
	}
}

func (e *DispatcherManager) GetRedoDispatcherMap() *DispatcherMap[*dispatcher.RedoDispatcher] {
	return e.redoDispatcherMap
}

func (e *DispatcherManager) GetTableTriggerRedoDispatcher() *dispatcher.RedoDispatcher {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tableTriggerRedoDispatcher
}

func (e *DispatcherManager) SetTableTriggerRedoDispatcher(rd *dispatcher.RedoDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tableTriggerRedoDispatcher = rd
}

func (e *DispatcherManager) GetAllRedoDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.redoSchemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.GetTableTriggerRedoDispatcher() != nil {
		dispatcherIDs = append(dispatcherIDs, e.GetTableTriggerRedoDispatcher().GetId())
	}
	return dispatcherIDs
}
