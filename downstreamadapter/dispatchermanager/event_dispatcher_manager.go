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

package dispatchermanager

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mysql"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/apperror"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

/*
EventDispatcherManager manages dispatchers for a changefeed instance with responsibilities including:

1. Initializing and managing the sink for the changefeed.
2. Communicating with the maintainer through the HeartBeatCollector by:
  - Collecting and batching messages from all dispatchers
  - Forwarding table status, block status, and heartbeat messages to the maintainer

3. Creating and removing dispatchers, including the table trigger event dispatcher
4. Collecting errors from all dispatchers and the sink module, reporting them to the maintainer

Architecture:
- Each changefeed in an instance has exactly one EventDispatcherManager
- Each EventDispatcherManager has exactly one backend sink
*/
type EventDispatcherManager struct {
	changefeedID common.ChangeFeedID

	// meta is used to store the meta info of the event dispatcher manager
	// it's used to avoid data race when we update the maintainerID and maintainerEpoch
	meta struct {
		sync.Mutex
		maintainerEpoch uint64
		maintainerID    node.ID
	}

	pdClock pdutil.Clock

	config       *config.ChangefeedConfig
	filterConfig *eventpb.FilterConfig
	// only not nil when enable sync point
	// TODO: changefeed update config
	syncPointConfig *syncpoint.SyncPointConfig

	// tableTriggerEventDispatcher is a special dispatcher, that is responsible for handling ddl and checkpoint events.
	tableTriggerEventDispatcher *dispatcher.Dispatcher
	// dispatcherMap restore all the dispatchers in the EventDispatcherManager, including table trigger event dispatcher
	dispatcherMap *DispatcherMap
	// schemaIDToDispatchers is store the schemaID info for all normal dispatchers.
	schemaIDToDispatchers *dispatcher.SchemaIDToDispatchers

	// statusesChan is used to store the status of dispatchers when status changed
	// and push to heartbeatRequestQueue
	statusesChan chan dispatcher.TableSpanStatusWithSeq
	// heartbeatRequestQueue is used to store the heartbeat request from all the dispatchers.
	// heartbeat collector will consume the heartbeat request from the queue and send the response to each dispatcher.
	heartbeatRequestQueue *HeartbeatRequestQueue

	// heartBeatTask is responsible for collecting the heartbeat info from all the dispatchers
	// and report to the maintainer periodicity.
	heartBeatTask *HeartBeatTask

	// blockStatusesChan will fetch the block status about ddl event and sync point event
	// and push to blockStatusRequestQueue
	blockStatusesChan chan *heartbeatpb.TableSpanBlockStatus
	// blockStatusRequestQueue is used to store the block status request from all the dispatchers.
	// heartbeat collector will consume the block status request from the queue and report to the maintainer.
	blockStatusRequestQueue *BlockStatusRequestQueue

	// sink is used to send all the events to the downstream.
	sink sink.Sink

	latestWatermark Watermark

	// collect the error in all the dispatchers and sink module
	// when we get the error, we will report the error to the maintainer
	errCh chan error

	closing atomic.Bool
	closed  atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	metricTableTriggerEventDispatcherCount prometheus.Gauge
	metricEventDispatcherCount             prometheus.Gauge
	metricCreateDispatcherDuration         prometheus.Observer
	metricCheckpointTs                     prometheus.Gauge
	metricCheckpointTsLag                  prometheus.Gauge
	metricResolvedTs                       prometheus.Gauge
	metricResolvedTsLag                    prometheus.Gauge
}

// return actual startTs of the table trigger event dispatcher
// when the table trigger event dispatcher is in this event dispatcher manager
func NewEventDispatcherManager(
	changefeedID common.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	tableTriggerEventDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	maintainerID node.ID,
	newChangefeed bool,
) (*EventDispatcherManager, uint64, error) {
	failpoint.Inject("NewEventDispatcherManagerDelay", nil)

	ctx, cancel := context.WithCancel(context.Background())
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)

	filterCfg := &eventpb.FilterConfig{
		CaseSensitive:  cfConfig.CaseSensitive,
		ForceReplicate: cfConfig.ForceReplicate,
		FilterConfig:   toFilterConfigPB(cfConfig.Filter),
	}
	log.Info("New EventDispatcherManager",
		zap.Stringer("changefeedID", changefeedID),
		zap.String("config", cfConfig.String()),
		zap.String("filterConfig", filterCfg.String()),
	)
	manager := &EventDispatcherManager{
		dispatcherMap:                          newDispatcherMap(),
		changefeedID:                           changefeedID,
		pdClock:                                pdClock,
		statusesChan:                           make(chan dispatcher.TableSpanStatusWithSeq, 8192),
		blockStatusesChan:                      make(chan *heartbeatpb.TableSpanBlockStatus, 1024*1024),
		errCh:                                  make(chan error, 1),
		cancel:                                 cancel,
		config:                                 cfConfig,
		filterConfig:                           filterCfg,
		schemaIDToDispatchers:                  dispatcher.NewSchemaIDToDispatchers(),
		latestWatermark:                        NewWatermark(0),
		metricTableTriggerEventDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricEventDispatcherCount:             metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCreateDispatcherDuration:         metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTs:                     metrics.EventDispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricCheckpointTsLag:                  metrics.EventDispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTs:                       metrics.EventDispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
		metricResolvedTsLag:                    metrics.EventDispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Namespace(), changefeedID.Name()),
	}

	// Set the epoch and maintainerID of the event dispatcher manager
	manager.meta.maintainerEpoch = cfConfig.Epoch
	manager.meta.maintainerID = maintainerID

	// Set Sync Point Config
	if cfConfig.EnableSyncPoint {
		// TODO: confirm that parameter validation is done at the setting location, so no need to check again here
		manager.syncPointConfig = &syncpoint.SyncPointConfig{
			SyncPointInterval:  cfConfig.SyncPointInterval,
			SyncPointRetention: cfConfig.SyncPointRetention,
		}
	}

	var err error
	manager.sink, err = sink.New(ctx, manager.config, manager.changefeedID)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// Register Event Dispatcher Manager in HeartBeatCollector,
	// which is responsible for communication with the maintainer.
	err = appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterEventDispatcherManager(manager)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	var tableTriggerStartTs uint64 = 0
	// init table trigger event dispatcher when tableTriggerEventDispatcherID is not nil
	if tableTriggerEventDispatcherID != nil {
		tableTriggerStartTs, err = manager.NewTableTriggerEventDispatcher(tableTriggerEventDispatcherID, startTs, newChangefeed)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	}

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		err = manager.sink.Run(ctx)
		if err != nil && !errors.Is(errors.Cause(err), context.Canceled) {
			select {
			case <-ctx.Done():
				return
			case manager.errCh <- err:
			default:
				log.Error("error channel is full, discard error",
					zap.Stringer("changefeedID", changefeedID),
					zap.Error(err),
				)
			}
		}
	}()

	// collect errors from error channel
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectErrors(ctx)
	}()

	// collect heart beat info from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectComponentStatusWhenChanged(ctx)
	}()

	// collect block status from all dispatchers
	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		manager.collectBlockStatusRequest(ctx)
	}()

	log.Info("event dispatcher manager created",
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("maintainerID", maintainerID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("tableTriggerStartTs", tableTriggerStartTs),
	)
	return manager, tableTriggerStartTs, nil
}

func (e *EventDispatcherManager) NewTableTriggerEventDispatcher(id *heartbeatpb.DispatcherID, startTs uint64, newChangefeed bool) (uint64, error) {
	err := e.newDispatchers([]dispatcherCreateInfo{
		{
			Id:        common.NewDispatcherIDFromPB(id),
			TableSpan: common.DDLSpan,
			StartTs:   startTs,
			SchemaID:  0,
		},
	}, newChangefeed)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Info("table trigger event dispatcher created",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", e.tableTriggerEventDispatcher.GetId()),
		zap.Uint64("startTs", e.tableTriggerEventDispatcher.GetStartTs()),
	)
	return e.tableTriggerEventDispatcher.GetStartTs(), nil
}

func (e *EventDispatcherManager) InitalizeTableTriggerEventDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
	if e.tableTriggerEventDispatcher == nil {
		return nil
	}

	needAddDispatcher, err := e.tableTriggerEventDispatcher.InitializeTableSchemaStore(schemaInfo)
	if err != nil {
		return errors.Trace(err)
	}

	if !needAddDispatcher {
		return nil
	}
	// before bootstrap finished, cannot send any event.
	success := e.tableTriggerEventDispatcher.EmitBootstrap()
	if !success {
		return errors.ErrDispatcherFailed.GenWithStackByArgs()
	}

	// table trigger event dispatcher can register to event collector to receive events after finish the initial table schema store from the maintainer.
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.tableTriggerEventDispatcher, e.config.MemoryQuota)

	// when sink is not mysql-class, table trigger event dispatcher need to receive the checkpointTs message from maintainer.
	if e.sink.SinkType() != common.MysqlSinkType {
		appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterCheckpointTsMessageDs(e)
	}
	return nil
}

// removeDDLTs means we don't need to check startTs from ddl_ts_table when sink is mysql-class,
// but we need to remove the ddl_ts item of this changefeed, to obtain a clean environment.
// removeDDLTs is true only when meet the following conditions:
// 1. newDispatchers is called by NewTableTriggerEventDispatcher(just means when creating table trigger event dispatcher)
// 2. changefeed is total new created, or resumed with overwriteCheckpointTs
func (e *EventDispatcherManager) newDispatchers(infos []dispatcherCreateInfo, removeDDLTs bool) error {
	start := time.Now()
	currentPdTs := e.pdClock.CurrentTS()

	dispatcherIds := make([]common.DispatcherID, 0, len(infos))
	tableIds := make([]int64, 0, len(infos))
	startTsList := make([]int64, 0, len(infos))
	tableSpans := make([]*heartbeatpb.TableSpan, 0, len(infos))
	schemaIds := make([]int64, 0, len(infos))
	for _, info := range infos {
		id := info.Id
		if _, ok := e.dispatcherMap.Get(id); ok {
			continue
		}
		dispatcherIds = append(dispatcherIds, id)
		tableIds = append(tableIds, info.TableSpan.TableID)
		startTsList = append(startTsList, int64(info.StartTs))
		tableSpans = append(tableSpans, info.TableSpan)
		schemaIds = append(schemaIds, info.SchemaID)
	}

	if len(dispatcherIds) == 0 {
		return nil
	}

	// When sink is mysql-class, we need to query the startTs from the downstream.
	// Because we have to sync data at least from the last ddl commitTs to avoid write old data to new schema
	// While for other type sink, they don't have the problem of writing old data to new schema,
	// so we just return the startTs we get.
	// Besides, we batch the creation for the dispatchers,
	// mainly because we need to batch the query for startTs when sink is mysql-class to reduce the time cost.
	//
	// When we enable syncpoint, we also need to know the last ddl commitTs whether is a syncpoint event.
	// because the commitTs of a syncpoint event can be the same as a ddl event
	// If there is a ddl event and a syncpoint event at the same time, we ensure the syncpoint event always after the ddl event.
	// So we need to know whether the commitTs is from a syncpoint event or a ddl event,
	// to decide whether we need to send generate the syncpoint event of this commitTs to downstream.
	var newStartTsList []int64
	startTsIsSyncpointList := make([]bool, len(startTsList))
	var err error
	if e.sink.SinkType() == common.MysqlSinkType {
		newStartTsList, startTsIsSyncpointList, err = e.sink.(*mysql.Sink).GetStartTsList(tableIds, startTsList, removeDDLTs)
		if err != nil {
			return errors.Trace(err)
		}
		log.Info("calculate real startTs for dispatchers",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Any("receiveStartTs", startTsList),
			zap.Any("realStartTs", newStartTsList),
			zap.Bool("removeDDLTs", removeDDLTs),
		)
	} else {
		newStartTsList = startTsList
	}

	if e.latestWatermark.Get().CheckpointTs == 0 {
		// If the checkpointTs is 0, means there is no dispatchers before. So we need to init it with the smallest startTs of these dispatchers
		smallestStartTs := int64(math.MaxInt64)
		for _, startTs := range newStartTsList {
			if startTs < smallestStartTs {
				smallestStartTs = startTs
			}
		}
		e.latestWatermark = NewWatermark(uint64(smallestStartTs))
	}

	for idx, id := range dispatcherIds {
		d := dispatcher.NewDispatcher(
			e.changefeedID,
			id, tableSpans[idx], e.sink,
			uint64(newStartTsList[idx]),
			e.statusesChan,
			e.blockStatusesChan,
			schemaIds[idx],
			e.schemaIDToDispatchers,
			e.syncPointConfig,
			startTsIsSyncpointList[idx],
			e.filterConfig,
			currentPdTs,
			e.errCh,
			e.config.BDRMode)

		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		if d.IsTableTriggerEventDispatcher() {
			if util.GetOrZero(e.config.SinkConfig.SendAllBootstrapAtStart) {
				d.BootstrapState = dispatcher.BootstrapNotStarted
			}
			e.tableTriggerEventDispatcher = d
		} else {
			e.schemaIDToDispatchers.Set(schemaIds[idx], id)
			// we don't register table trigger event dispatcher in event collector, when created.
			// Table trigger event dispatcher is a special dispatcher,
			// it need to wait get the initial table schema store from the maintainer, then will register to event collector to receive events.
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(d, e.config.MemoryQuota)
		}

		seq := e.dispatcherMap.Set(id, d)
		d.SetSeq(seq)

		if d.IsTableTriggerEventDispatcher() {
			e.metricTableTriggerEventDispatcherCount.Inc()
		} else {
			e.metricEventDispatcherCount.Inc()
		}

		log.Info("new dispatcher created",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("tableSpan", common.FormatTableSpan(tableSpans[idx])),
			zap.Int64("startTs", newStartTsList[idx]))

	}
	e.metricCreateDispatcherDuration.Observe(time.Since(start).Seconds() / float64(len(dispatcherIds)))
	log.Info("batch create new dispatchers",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Int("count", len(dispatcherIds)),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// collectErrors collect the errors from the error channel and report to the maintainer.
func (e *EventDispatcherManager) collectErrors(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-e.errCh:
			if !errors.Is(errors.Cause(err), context.Canceled) {
				log.Error("Event Dispatcher Manager Meets Error",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Error(err),
				)

				// report error to maintainer
				var message heartbeatpb.HeartBeatRequest
				message.ChangefeedID = e.changefeedID.ToPB()
				message.Err = &heartbeatpb.RunningError{
					Time:    time.Now().String(),
					Node:    appcontext.GetID(),
					Code:    string(apperror.ErrorCode(err)),
					Message: err.Error(),
				}
				e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})

				// resend message until the event dispatcher manager is closed
				// the first error is matter most, so we just need to resend it continue and ignore the other errors.
				ticker := time.NewTicker(time.Second * 5)
				for {
					select {
					case <-ctx.Done():
						ticker.Stop()
						return
					case <-ticker.C:
						e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
					}
				}
			}
		}
	}
}

// collectBlockStatusRequest collect the block status from the block status channel and report to the maintainer.
func (e *EventDispatcherManager) collectBlockStatusRequest(ctx context.Context) {
	for {
		blockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		select {
		case <-ctx.Done():
			return
		case blockStatus := <-e.blockStatusesChan:
			blockStatusMessage = append(blockStatusMessage, blockStatus)

			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case blockStatus := <-e.blockStatusesChan:
					blockStatusMessage = append(blockStatusMessage, blockStatus)
				case <-delay.C:
					break loop
				}
			}

			// Release resources promptly
			if !delay.Stop() {
				select {
				case <-delay.C:
				default:
				}
			}

			var message heartbeatpb.BlockStatusRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.BlockStatuses = blockStatusMessage
			e.blockStatusRequestQueue.Enqueue(&BlockStatusRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
}

// collectComponentStatusWhenStatesChanged collect the component status info when the dispatchers states changed,
// such as --> working; --> stopped; --> stopping
// we will do a batch for the status, then send to heartbeatRequestQueue
func (e *EventDispatcherManager) collectComponentStatusWhenChanged(ctx context.Context) {
	for {
		statusMessage := make([]*heartbeatpb.TableSpanStatus, 0)
		// why we need compare with latest watermark? for not backward the watermark?
		watermark := e.latestWatermark.Get()
		newWatermark := &heartbeatpb.Watermark{
			CheckpointTs: watermark.CheckpointTs,
			ResolvedTs:   watermark.ResolvedTs,
			Seq:          watermark.Seq,
		}
		select {
		case <-ctx.Done():
			return
		case tableSpanStatus := <-e.statusesChan:
			statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
			newWatermark.Seq = tableSpanStatus.Seq
			if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newWatermark.CheckpointTs {
				newWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
			}
			if tableSpanStatus.ResolvedTs != 0 && tableSpanStatus.ResolvedTs < newWatermark.ResolvedTs {
				newWatermark.ResolvedTs = tableSpanStatus.ResolvedTs
			}
			delay := time.NewTimer(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.statusesChan:
					statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
					if newWatermark.Seq < tableSpanStatus.Seq {
						newWatermark.Seq = tableSpanStatus.Seq
					}
					if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newWatermark.CheckpointTs {
						newWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
					}
					if tableSpanStatus.ResolvedTs != 0 && tableSpanStatus.ResolvedTs < newWatermark.ResolvedTs {
						newWatermark.ResolvedTs = tableSpanStatus.ResolvedTs
					}
				case <-delay.C:
					break loop
				}
			}
			// Release resources promptly
			if !delay.Stop() {
				select {
				case <-delay.C:
				default:
				}
			}
			var message heartbeatpb.HeartBeatRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.Statuses = statusMessage
			message.Watermark = newWatermark
			e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
}

// aggregateDispatcherHeartbeats aggregates heartbeat information from all dispatchers and generates a HeartBeatRequest.
// The function performs the following tasks:
// 1. Aggregates status and watermark information from all dispatchers
// 2. Handles removal of stopped dispatchers
// 3. Updates metrics for checkpoint and resolved timestamps
//
// Parameters:
//   - needCompleteStatus: when true, includes detailed status for all dispatchers in the response.
//     When false, only includes minimal information and watermarks to reduce message size.
//
// Returns a HeartBeatRequest containing the aggregated information.
func (e *EventDispatcherManager) aggregateDispatcherHeartbeats(needCompleteStatus bool) *heartbeatpb.HeartBeatRequest {
	message := heartbeatpb.HeartBeatRequest{
		ChangefeedID:    e.changefeedID.ToPB(),
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
	}

	toRemoveDispatcherIDs := make([]common.DispatcherID, 0)
	removedDispatcherSchemaIDs := make([]int64, 0)
	heartBeatInfo := &dispatcher.HeartBeatInfo{}

	eventServiceDispatcherHeartbeat := &event.DispatcherHeartbeat{
		Version:              event.DispatcherHeartbeatVersion,
		DispatcherCount:      0,
		DispatcherProgresses: make([]event.DispatcherProgress, 0, 32),
	}

	seq := e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem *dispatcher.Dispatcher) {
		dispatcherItem.GetHeartBeatInfo(heartBeatInfo)
		// If the dispatcher is in removing state, we need to check if it's closed successfully.
		// If it's closed successfully, we could clean it up.
		// TODO: we need to consider how to deal with the checkpointTs of the removed dispatcher if the message will be discarded.
		if heartBeatInfo.IsRemoving {
			watermark, ok := dispatcherItem.TryClose()
			if ok {
				// remove successfully
				message.Watermark.UpdateMin(watermark)
				// If the dispatcher is removed successfully, we need to add the tableSpan into message whether needCompleteStatus is true or not.
				message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
					ID:              id.ToPB(),
					ComponentStatus: heartbeatpb.ComponentState_Stopped,
					CheckpointTs:    watermark.CheckpointTs,
				})
				toRemoveDispatcherIDs = append(toRemoveDispatcherIDs, id)
				removedDispatcherSchemaIDs = append(removedDispatcherSchemaIDs, dispatcherItem.GetSchemaID())
			}
		}

		message.Watermark.UpdateMin(heartBeatInfo.Watermark)
		if needCompleteStatus {
			message.Statuses = append(message.Statuses, &heartbeatpb.TableSpanStatus{
				ID:                 id.ToPB(),
				ComponentStatus:    heartBeatInfo.ComponentStatus,
				CheckpointTs:       heartBeatInfo.Watermark.CheckpointTs,
				EventSizePerSecond: dispatcherItem.GetEventSizePerSecond(),
			})
			eventServiceDispatcherHeartbeat.Append(event.NewDispatcherProgress(id, heartBeatInfo.Watermark.CheckpointTs))
		}
	})
	message.Watermark.Seq = seq
	e.latestWatermark.Set(message.Watermark)

	// if the event dispatcher manager is closing, we don't to remove the stopped dispatchers.
	if !e.closing.Load() {
		for idx, id := range toRemoveDispatcherIDs {
			e.cleanDispatcher(id, removedDispatcherSchemaIDs[idx])
		}
	}

	// If needCompleteStatus is true, we need to send the dispatcher heartbeat to the event service.
	if needCompleteStatus {
		if e.tableTriggerEventDispatcher != nil {
			// add tableTriggerEventDispatcher heartbeat
			heartBeatInfo := &dispatcher.HeartBeatInfo{}
			e.tableTriggerEventDispatcher.GetHeartBeatInfo(heartBeatInfo)
			eventServiceDispatcherHeartbeat.Append(event.NewDispatcherProgress(e.tableTriggerEventDispatcher.GetId(), heartBeatInfo.Watermark.CheckpointTs))
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).SendDispatcherHeartbeat(eventServiceDispatcherHeartbeat)
	}

	e.metricCheckpointTs.Set(float64(message.Watermark.CheckpointTs))
	e.metricResolvedTs.Set(float64(message.Watermark.ResolvedTs))

	phyCheckpointTs := oracle.ExtractPhysical(message.Watermark.CheckpointTs)
	phyResolvedTs := oracle.ExtractPhysical(message.Watermark.ResolvedTs)

	pdTime := e.pdClock.CurrentTime()
	e.metricCheckpointTsLag.Set(float64(oracle.GetPhysical(pdTime)-phyCheckpointTs) / 1e3)
	e.metricResolvedTsLag.Set(float64(oracle.GetPhysical(pdTime)-phyResolvedTs) / 1e3)
	return &message
}

// ==== remove and clean related functions ====

func (e *EventDispatcherManager) TryClose(removeChangefeed bool) bool {
	if e.closed.Load() {
		return true
	}
	if e.closing.Load() {
		return e.closed.Load()
	}
	e.cleanMetrics()
	e.closing.Store(true)
	go e.close(removeChangefeed)
	return false
}

func (e *EventDispatcherManager) close(removeChangefeed bool) {
	log.Info("closing event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID))

	defer e.closing.Store(false)
	e.closeAllDispatchers()

	err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveEventDispatcherManager(e)
	if err != nil {
		log.Error("remove event dispatcher manager from heartbeat collector failed",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Error(err),
		)
		return
	}

	// heartbeatTask only will be generated when create new dispatchers.
	// We check heartBeatTask after we remove the stream in heartbeat collector,
	// so we won't get add dispatcher messages to create heartbeatTask.
	// Thus there will not data race when we check heartBeatTask.
	if e.heartBeatTask != nil {
		e.heartBeatTask.Cancel()
	}

	e.sink.Close(removeChangefeed)
	e.cancel()
	e.wg.Wait()

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())

	e.closed.Store(true)
	log.Info("event dispatcher manager closed",
		zap.Stringer("changefeedID", e.changefeedID))
}

// closeAllDispatchers is called when the event dispatcher manager is closing
func (e *EventDispatcherManager) closeAllDispatchers() {
	leftToCloseDispatchers := make([]*dispatcher.Dispatcher, 0)
	e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.Dispatcher) {
		// Remove dispatcher from eventService
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcher)

		if dispatcher.IsTableTriggerEventDispatcher() && e.sink.SinkType() != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(e.changefeedID)
			if err != nil {
				log.Error("remove checkpointTs message failed",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Error(err),
				)
			}
		}

		dispatcher.Remove()

		_, ok := dispatcher.TryClose()
		if !ok {
			leftToCloseDispatchers = append(leftToCloseDispatchers, dispatcher)
		}
	})
	// wait all dispatchers finish syncing the data to sink
	for _, dispatcher := range leftToCloseDispatchers {
		log.Info("closing dispatcher",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", dispatcher.GetId()),
			zap.Any("tableSpan", common.FormatTableSpan(dispatcher.GetTableSpan())),
		)
		ok := false
		count := 0
		for !ok {
			_, ok = dispatcher.TryClose()
			time.Sleep(10 * time.Millisecond)
			count += 1
			if count%100 == 0 {
				log.Info("waiting for dispatcher to close",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Stringer("dispatcherID", dispatcher.GetId()),
					zap.Any("tableSpan", common.FormatTableSpan(dispatcher.GetTableSpan())),
					zap.Int("count", count),
				)
			}
		}
	}
}

// removeDispatcher is called when the dispatcher is scheduled
func (e *EventDispatcherManager) removeDispatcher(id common.DispatcherID) {
	dispatcherItem, ok := e.dispatcherMap.Get(id)
	if ok {
		if dispatcherItem.GetRemovingStatus() {
			return
		}
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).RemoveDispatcher(dispatcherItem)

		// for non-mysql class sink, only the event dispatcher manager with table trigger event dispatcher need to receive the checkpointTs message.
		if dispatcherItem.IsTableTriggerEventDispatcher() && e.sink.SinkType() != common.MysqlSinkType {
			err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveCheckpointTsMessage(e.changefeedID)
			log.Error("remove checkpointTs message ds failed", zap.Error(err))
		}

		dispatcherItem.Remove()
	} else {
		e.statusesChan <- dispatcher.TableSpanStatusWithSeq{
			TableSpanStatus: &heartbeatpb.TableSpanStatus{
				ID:              id.ToPB(),
				ComponentStatus: heartbeatpb.ComponentState_Stopped,
			},
			Seq: e.dispatcherMap.GetSeq(),
		}
	}
}

// cleanDispatcher is called when the dispatcher is removed successfully.
func (e *EventDispatcherManager) cleanDispatcher(id common.DispatcherID, schemaID int64) {
	e.dispatcherMap.Delete(id)
	e.schemaIDToDispatchers.Delete(schemaID, id)
	if e.tableTriggerEventDispatcher != nil && e.tableTriggerEventDispatcher.GetId() == id {
		e.tableTriggerEventDispatcher = nil
		e.metricTableTriggerEventDispatcherCount.Dec()
	} else {
		e.metricEventDispatcherCount.Dec()
	}
	log.Info("table event dispatcher completely stopped, and delete it from event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", id),
	)
}

func (e *EventDispatcherManager) cleanMetrics() {
	metrics.DynamicStreamMemoryUsage.DeleteLabelValues(
		"event-collector",
		"max",
		e.changefeedID.String(),
	)

	metrics.DynamicStreamMemoryUsage.DeleteLabelValues(
		"event-collector",
		"used",
		e.changefeedID.String(),
	)

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerCheckpointTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
	metrics.EventDispatcherManagerResolvedTsLagGauge.DeleteLabelValues(e.changefeedID.Namespace(), e.changefeedID.Name())
}

// ==== remove and clean related functions END ====
