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
	"sync"
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
	redoTableTriggerEventDispatcherID *heartbeatpb.DispatcherID,
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

	// init redo table trigger event dispatcher when redoTableTriggerEventDispatcherID is not nil
	if redoTableTriggerEventDispatcherID != nil {
		err := manager.NewRedoTableTriggerEventDispatcher(redoTableTriggerEventDispatcherID, startTs, newChangefeed)
		if err != nil {
			return err
		}
	}
	// register redo metrics
	manager.metricRedoTableTriggerEventDispatcherCount = metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher")
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

func (e *DispatcherManager) NewRedoTableTriggerEventDispatcher(id *heartbeatpb.DispatcherID, startTs uint64, newChangefeed bool) error {
	if e.GetTableTriggerEventDispatcher() != nil {
		log.Error("redo table trigger event dispatcher existed!")
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
	// table trigger event dispatcher and redo table trigger event dispatcher must exist on the same node
	e.GetRedoTableTriggerEventDispatcher().SetRedoMeta(e.config.Consistent)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		err := e.collectRedoMeta(e.ctx)
		e.handleError(e.ctx, err)
	}()
	log.Info("redo table trigger event dispatcher created",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", e.GetRedoTableTriggerEventDispatcher().GetId()),
		zap.Uint64("startTs", e.GetRedoTableTriggerEventDispatcher().GetStartTs()),
	)
	return nil
}

func (e *DispatcherManager) newRedoDispatchers(infos map[common.DispatcherID]dispatcherCreateInfo, removeDDLTs bool) error {
	start := time.Now()

	dispatcherIds, tableIds, startTsList, tableSpans, schemaIds := prepareCreateDispatcher(infos, e.redoDispatcherMap)
	if len(dispatcherIds) == 0 {
		return nil
	}

	// When initializing the dispatcher manager, both the redo dispatcher and the common dispatcher exist.
	// The common dispatcher obtains the true start timestamp (start-ts) if the sink type is MySQL,
	// this start-ts is always greater than or equal to the global start-ts.
	// However, the redo dispatcher must receive data before the common dispatcher, and the common dispatcher replicates based on the global redo timestamp.
	// If the redo dispatcher’s start-ts is less than that of the common dispatcher,
	// we will encounter a checkpoint-ts greater than the resolved-ts in the redo metadata.
	// This results in the redo metadata recording an incorrect log, which can cause a panic if no additional redo metadata logs are flushed.
	// Therefore, we must ensure that the start-ts remains consistent with the common dispatcher by querying the recovery info from the MySQL sink.
	newStartTsList, _, _, err := e.getTableRecoveryInfoFromMysqlSink(tableIds, startTsList, removeDDLTs)
	if err != nil {
		return errors.Trace(err)
	}

	if e.latestRedoWatermark.Get().CheckpointTs == 0 {
		// If the checkpointTs is 0, means there is no dispatchers before. So we need to init it with the smallest startTs of these dispatchers
		smallestStartTs := int64(math.MaxInt64)
		for _, startTs := range newStartTsList {
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
			uint64(newStartTsList[idx]),
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
			e.SetRedoTableTriggerEventDispatcher(rd)
		} else {
			e.redoSchemaIDToDispatchers.Set(schemaIds[idx], id)
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(rd, e.redoQuota)
		}

		redoSeq := e.redoDispatcherMap.Set(rd.GetId(), rd)
		rd.SetSeq(redoSeq)

		if rd.IsTableTriggerEventDispatcher() {
			e.metricRedoTableTriggerEventDispatcherCount.Inc()
		} else {
			e.metricRedoEventDispatcherCount.Inc()
		}

		log.Info("new redo dispatcher created",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("tableSpan", common.FormatTableSpan(tableSpans[idx])),
			zap.Int64("startTs", newStartTsList[idx]))
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
	e.redoCurrentOperatorMap.Delete(id)
	log.Debug("delete current working remove operator for redo dispatcher",
		zap.String("changefeedID", e.changefeedID.String()),
		zap.String("dispatcherID", id.String()),
	)
	redoTableTriggerEventDispatcher := e.GetRedoTableTriggerEventDispatcher()
	if redoTableTriggerEventDispatcher != nil && redoTableTriggerEventDispatcher.GetId() == id {
		e.SetRedoTableTriggerEventDispatcher(nil)
		e.metricRedoTableTriggerEventDispatcherCount.Dec()
	} else {
		e.metricRedoEventDispatcherCount.Dec()
	}
	log.Info("redo dispatcher completely stopped, and delete it from event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", id),
	)
}

func (e *DispatcherManager) closeRedoMeta(removeChangefeed bool) {
	if removeChangefeed && e.GetRedoTableTriggerEventDispatcher() != nil {
		redoMeta := e.GetRedoTableTriggerEventDispatcher().GetRedoMeta()
		if redoMeta != nil {
			redoMeta.Cleanup(context.Background())
		}
	}
}

func (e *DispatcherManager) InitalizeRedoTableTriggerEventDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
	if e.GetRedoTableTriggerEventDispatcher() == nil {
		return nil
	}
	needAddDispatcher, err := e.GetRedoTableTriggerEventDispatcher().InitializeTableSchemaStore(schemaInfo)
	if err != nil {
		return errors.Trace(err)
	}
	if !needAddDispatcher {
		return nil
	}
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.GetRedoTableTriggerEventDispatcher(), e.redoQuota)
	return nil
}

func (e *DispatcherManager) UpdateRedoMeta(checkpointTs, resolvedTs uint64) {
	// only update meta on the one node
	if e.GetRedoTableTriggerEventDispatcher() != nil {
		e.GetRedoTableTriggerEventDispatcher().UpdateMeta(checkpointTs, resolvedTs)
		return
	}
	log.Error("should not reach here. only update redo meta on the redoTableTriggerEventDispatcher")
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
			if e.GetRedoTableTriggerEventDispatcher() == nil {
				log.Error("should not reach here. only collect redo meta on the redoTableTriggerEventDispatcher")
				continue
			}
			logMeta := e.GetRedoTableTriggerEventDispatcher().GetFlushedMeta()
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

func (e *DispatcherManager) GetRedoTableTriggerEventDispatcher() *dispatcher.RedoDispatcher {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.redoTableTriggerEventDispatcher
}

func (e *DispatcherManager) SetRedoTableTriggerEventDispatcher(rd *dispatcher.RedoDispatcher) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.redoTableTriggerEventDispatcher = rd
}

func (e *DispatcherManager) GetAllRedoDispatchers(schemaID int64) []common.DispatcherID {
	dispatcherIDs := e.redoSchemaIDToDispatchers.GetDispatcherIDs(schemaID)
	if e.GetRedoTableTriggerEventDispatcher() != nil {
		dispatcherIDs = append(dispatcherIDs, e.GetRedoTableTriggerEventDispatcher().GetId())
	}
	return dispatcherIDs
}

func (e *DispatcherManager) GetRedoCurrentOperatorMap() *sync.Map {
	return &e.redoCurrentOperatorMap
}
