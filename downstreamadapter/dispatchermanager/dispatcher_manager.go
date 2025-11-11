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
	"github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/downstreamadapter/syncpoint"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

/*
DispatcherManager manages dispatchers for a changefeed instance with responsibilities including:

1. Initializing and managing the sink for the changefeed.
2. Communicating with the maintainer through the HeartBeatCollector by:
  - Collecting and batching messages from all dispatchers
  - Forwarding table status, block status, and heartbeat messages to the maintainer

3. Creating and removing dispatchers, including the table trigger event dispatcher
4. Collecting errors from all dispatchers and the sink module, reporting them to the maintainer

Architecture:
- Each changefeed in an instance has exactly one DispatcherManager
- Each DispatcherManager has exactly one backend sink
*/
type DispatcherManager struct {
	changefeedID common.ChangeFeedID
	keyspaceID   uint32

	// meta is used to store the meta info of the event dispatcher manager
	// it's used to avoid data race when we update the maintainerID and maintainerEpoch
	meta struct {
		sync.Mutex
		maintainerEpoch uint64
		maintainerID    node.ID
	}

	pdClock pdutil.Clock

	config *config.ChangefeedConfig

	// tableTriggerEventDispatcher is a special dispatcher, that is responsible for handling ddl and checkpoint events.
	tableTriggerEventDispatcher *dispatcher.EventDispatcher
	// redoTableTriggerEventDispatcher is a special redo dispatcher, that is responsible for handling ddl and checkpoint events.
	redoTableTriggerEventDispatcher *dispatcher.RedoDispatcher
	// dispatcherMap restore all the dispatchers in the DispatcherManager, including table trigger event dispatcher
	dispatcherMap *DispatcherMap[*dispatcher.EventDispatcher]
	// redoDispatcherMap restore all the redo dispatchers in the DispatcherManager, including redo table trigger event dispatcher
	redoDispatcherMap *DispatcherMap[*dispatcher.RedoDispatcher]
	// schemaIDToDispatchers is shared in the DispatcherManager,
	// it store all the infos about schemaID->Dispatchers
	// Dispatchers may change the schemaID when meets some special events, such as rename ddl
	// we use schemaIDToDispatchers to calculate the dispatchers that need to receive the dispatcher status
	schemaIDToDispatchers *dispatcher.SchemaIDToDispatchers
	// redoSchemaIDToDispatchers is store the schemaID info for all redo dispatchers.
	redoSchemaIDToDispatchers *dispatcher.SchemaIDToDispatchers
	// heartbeatRequestQueue is used to store the heartbeat request from all the dispatchers.
	// heartbeat collector will consume the heartbeat request from the queue and send the response to each dispatcher.
	heartbeatRequestQueue *HeartbeatRequestQueue

	// heartBeatTask is responsible for collecting the heartbeat info from all the dispatchers
	// and report to the maintainer periodicity.
	heartBeatTask *HeartBeatTask

	// blockStatusRequestQueue is used to store the block status request from all the dispatchers.
	// heartbeat collector will consume the block status request from the queue and report to the maintainer.
	blockStatusRequestQueue *BlockStatusRequestQueue

	// sink is used to send all the events to the downstream.
	sink sink.Sink
	// redo related
	RedoEnable bool
	redoSink   *redo.Sink
	// redoGlobalTs stores the resolved-ts of the redo metadata and blocks events in the common dispatcher where the commit-ts is greater than the resolved-ts.
	redoGlobalTs atomic.Uint64

	latestWatermark Watermark

	closing atomic.Bool
	closed  atomic.Bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup

	// removeTaskHandles stores the task handles for async dispatcher removal
	// map[common.DispatcherID]*threadpool.TaskHandle
	removeTaskHandles sync.Map

	sinkQuota uint64
	redoQuota uint64

	// Shared info for all dispatchers
	sharedInfo *dispatcher.SharedInfo

	metricTableTriggerEventDispatcherCount prometheus.Gauge
	metricEventDispatcherCount             prometheus.Gauge
	metricCreateDispatcherDuration         prometheus.Observer
	metricCheckpointTs                     prometheus.Gauge
	metricCheckpointTsLag                  prometheus.Gauge
	metricResolvedTs                       prometheus.Gauge
	metricResolvedTsLag                    prometheus.Gauge

	metricRedoTableTriggerEventDispatcherCount prometheus.Gauge
	metricRedoEventDispatcherCount             prometheus.Gauge
	metricRedoCreateDispatcherDuration         prometheus.Observer
}

// return actual startTs of the table trigger event dispatcher
// when the table trigger event dispatcher is in this event dispatcher manager
func NewDispatcherManager(
	keyspaceID uint32,
	changefeedID common.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	tableTriggerEventDispatcherID,
	redoTableTriggerEventDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	maintainerID node.ID,
	newChangefeed bool,
) (*DispatcherManager, uint64, error) {
	failpoint.Inject("NewDispatcherManagerDelay", nil)

	ctx, cancel := context.WithCancel(context.Background())
	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)

	filterCfg := &eventpb.FilterConfig{
		CaseSensitive:  cfConfig.CaseSensitive,
		ForceReplicate: cfConfig.ForceReplicate,
		FilterConfig:   toFilterConfigPB(cfConfig.Filter),
	}
	var integrityCfg *eventpb.IntegrityConfig
	if cfConfig.SinkConfig.Integrity != nil {
		integrityCfg = cfConfig.SinkConfig.Integrity.ToPB()
	}
	log.Info("New DispatcherManager",
		zap.Stringer("changefeedID", changefeedID),
		zap.String("config", cfConfig.String()),
		zap.String("filterConfig", filterCfg.String()),
	)
	manager := &DispatcherManager{
		dispatcherMap:         newDispatcherMap[*dispatcher.EventDispatcher](),
		changefeedID:          changefeedID,
		keyspaceID:            keyspaceID,
		pdClock:               pdClock,
		cancel:                cancel,
		config:                cfConfig,
		latestWatermark:       NewWatermark(0),
		schemaIDToDispatchers: dispatcher.NewSchemaIDToDispatchers(),
		sinkQuota:             cfConfig.MemoryQuota,

		metricTableTriggerEventDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricEventDispatcherCount:             metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricCreateDispatcherDuration:         metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricCheckpointTs:                     metrics.DispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricCheckpointTsLag:                  metrics.DispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTs:                       metrics.DispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTsLag:                    metrics.DispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),

		metricRedoTableTriggerEventDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
		metricRedoEventDispatcherCount:             metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
		metricRedoCreateDispatcherDuration:         metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
	}

	// Set the epoch and maintainerID of the event dispatcher manager
	manager.meta.maintainerEpoch = cfConfig.Epoch
	manager.meta.maintainerID = maintainerID

	// Set Sync Point Config
	var syncPointConfig *syncpoint.SyncPointConfig
	if cfConfig.EnableSyncPoint {
		// TODO: confirm that parameter validation is done at the setting location, so no need to check again here
		syncPointConfig = &syncpoint.SyncPointConfig{
			SyncPointInterval:  cfConfig.SyncPointInterval,
			SyncPointRetention: cfConfig.SyncPointRetention,
		}
	}

	var err error
	manager.sink, err = sink.New(ctx, manager.config, manager.changefeedID)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// Determine outputRawChangeEvent based on sink type
	var outputRawChangeEvent bool
	switch manager.sink.SinkType() {
	case common.CloudStorageSinkType:
		outputRawChangeEvent = manager.config.SinkConfig.CloudStorageConfig.GetOutputRawChangeEvent()
	case common.KafkaSinkType:
		outputRawChangeEvent = manager.config.SinkConfig.KafkaConfig.GetOutputRawChangeEvent()
	}

	// Create shared info for all dispatchers
	manager.sharedInfo = dispatcher.NewSharedInfo(
		manager.changefeedID,
		manager.config.TimeZone,
		manager.config.BDRMode,
		outputRawChangeEvent,
		integrityCfg,
		filterCfg,
		syncPointConfig,
		manager.config.EnableSplittableCheck,
		make(chan dispatcher.TableSpanStatusWithSeq, 8192),
		make(chan *heartbeatpb.TableSpanBlockStatus, 1024*1024),
		make(chan error, 1),
	)

	// Register Event Dispatcher Manager in HeartBeatCollector,
	// which is responsible for communication with the maintainer.
	err = appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterDispatcherManager(manager)
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
	err = initRedoComponet(ctx, manager, changefeedID, redoTableTriggerEventDispatcherID, startTs, newChangefeed)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	manager.wg.Add(1)
	go func() {
		defer manager.wg.Done()
		err := manager.sink.Run(ctx)
		manager.handleError(ctx, err)
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
		zap.Uint64("sinkQuota", manager.sinkQuota),
		zap.Uint64("redoQuota", manager.redoQuota),
		zap.Bool("redoEnable", manager.RedoEnable),
		zap.Bool("outputRawChangeEvent", manager.sharedInfo.IsOutputRawChangeEvent()),
	)
	return manager, tableTriggerStartTs, nil
}

func (e *DispatcherManager) NewTableTriggerEventDispatcher(id *heartbeatpb.DispatcherID, startTs uint64, newChangefeed bool) (uint64, error) {
	if e.tableTriggerEventDispatcher != nil {
		log.Error("table trigger event dispatcher existed!")
	}
	infos := map[common.DispatcherID]dispatcherCreateInfo{}
	dispatcherID := common.NewDispatcherIDFromPB(id)
	infos[dispatcherID] = dispatcherCreateInfo{
		Id:        dispatcherID,
		TableSpan: common.KeyspaceDDLSpan(e.keyspaceID),
		StartTs:   startTs,
		SchemaID:  0,
	}
	err := e.newEventDispatchers(infos, newChangefeed)
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

func (e *DispatcherManager) InitalizeTableTriggerEventDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
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

	// redo
	if e.RedoEnable {
		appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.redoTableTriggerEventDispatcher, e.redoQuota)
	}
	// table trigger event dispatcher can register to event collector to receive events after finish the initial table schema store from the maintainer.
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(e.tableTriggerEventDispatcher, e.sinkQuota)

	// when sink is not mysql-class, table trigger event dispatcher need to receive the checkpointTs message from maintainer.
	if e.sink.SinkType() != common.MysqlSinkType {
		appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterCheckpointTsMessageDs(e)
	}
	return nil
}

func (e *DispatcherManager) getTableRecoveryInfoFromMysqlSink(tableIds, startTsList []int64, removeDDLTs bool) ([]int64, []bool, []bool, error) {
	var (
		newStartTsList []int64
		err            error
	)
	skipSyncpointAtStartTsList := make([]bool, len(startTsList))
	skipDMLAsStartTsList := make([]bool, len(startTsList))
	if e.sink.SinkType() == common.MysqlSinkType {
		newStartTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, err = e.sink.(*mysql.Sink).GetTableRecoveryInfo(tableIds, startTsList, removeDDLTs)
		if err != nil {
			return nil, nil, nil, err
		}
		log.Info("get table recovery info for dispatchers",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Any("receiveStartTs", startTsList),
			zap.Any("realStartTs", newStartTsList),
			zap.Bool("removeDDLTs", removeDDLTs),
		)
	} else {
		newStartTsList = startTsList
	}
	return newStartTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, nil
}

// removeDDLTs means we don't need to check startTs from ddl_ts_table when sink is mysql-class,
// but we need to remove the ddl_ts item of this changefeed, to obtain a clean environment.
// removeDDLTs is true only when meet the following conditions:
// 1. newEventDispatchers is called by NewTableTriggerEventDispatcher(just means when creating table trigger event dispatcher)
// 2. changefeed is total new created, or resumed with overwriteCheckpointTs
func (e *DispatcherManager) newEventDispatchers(infos map[common.DispatcherID]dispatcherCreateInfo, removeDDLTs bool) error {
	start := time.Now()
	currentPdTs := e.pdClock.CurrentTS()

	dispatcherIds, tableIds, startTsList, tableSpans, schemaIds := prepareCreateDispatcher(infos, e.dispatcherMap)
	if len(dispatcherIds) == 0 {
		return nil
	}

	// When sink is mysql-class, we need to query DDL crash recovery information from the downstream.
	// This includes:
	// 1. The actual startTs to use (must be at least from the last DDL commitTs to avoid writing old data to new schema)
	// 2. Whether to skip syncpoint events at startTs (skipSyncpointAtStartTs)
	// 3. Whether to skip DML events at startTs+1 (skipDMLAsStartTs)
	//
	// For other sink types, they don't have the problem of writing old data to new schema,
	// so we just return the input startTs with all skip flags set to false.
	//
	// We batch the creation for the dispatchers to batch the recovery info query when sink is mysql-class,
	// which reduces the time cost.
	newStartTsList, skipSyncpointAtStartTsList, skipDMLAsStartTsList, err := e.getTableRecoveryInfoFromMysqlSink(tableIds, startTsList, removeDDLTs)
	if err != nil {
		return errors.Trace(err)
	}

	if e.latestWatermark.Get().CheckpointTs == 0 {
		// If the checkpointTs is 0, means there is no dispatchers before. So we need to init it with the smallest startTs of these dispatchers
		smallestStartTs := int64(math.MaxInt64)
		for _, startTs := range newStartTsList {
			if startTs < smallestStartTs {
				smallestStartTs = startTs
			}
		}
		e.latestWatermark.Set(&heartbeatpb.Watermark{
			CheckpointTs: uint64(smallestStartTs),
			ResolvedTs:   uint64(smallestStartTs),
		})
	}

	for idx, id := range dispatcherIds {
		d := dispatcher.NewEventDispatcher(
			id,
			tableSpans[idx],
			uint64(newStartTsList[idx]),
			schemaIds[idx],
			e.schemaIDToDispatchers,
			skipSyncpointAtStartTsList[idx],
			skipDMLAsStartTsList[idx],
			currentPdTs,
			e.sink,
			e.sharedInfo,
			e.RedoEnable,
			&e.redoGlobalTs,
		)
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
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(d, e.sinkQuota)
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

func (e *DispatcherManager) handleError(ctx context.Context, err error) {
	if err != nil && !errors.Is(errors.Cause(err), context.Canceled) {
		select {
		case <-ctx.Done():
			return
		case e.sharedInfo.GetErrCh() <- err:
		default:
			log.Error("error channel is full, discard error",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Error(err),
			)
		}
	}
}

// collectErrors collect the errors from the error channel and report to the maintainer.
func (e *DispatcherManager) collectErrors(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-e.sharedInfo.GetErrCh():
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
					Code:    string(errors.ErrorCode(err)),
					Message: err.Error(),
				}
				e.heartbeatRequestQueue.Enqueue(&HeartBeatRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})

				// resend message until the event dispatcher manager is closed
				// the first error is matter most, so we just need to resend it continue and ignore the other errors.
				ticker := time.NewTicker(time.Second * 5)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
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
func (e *DispatcherManager) collectBlockStatusRequest(ctx context.Context) {
	delay := time.NewTimer(0)
	defer delay.Stop()
	enqueueBlockStatus := func(blockStatusMessage []*heartbeatpb.TableSpanBlockStatus, mode int64) {
		var message heartbeatpb.BlockStatusRequest
		message.ChangefeedID = e.changefeedID.ToPB()
		message.BlockStatuses = blockStatusMessage
		message.Mode = mode
		e.blockStatusRequestQueue.Enqueue(&BlockStatusRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
	}
	for {
		blockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		redoBlockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		select {
		case <-ctx.Done():
			return
		case blockStatus := <-e.sharedInfo.GetBlockStatusesChan():
			if common.IsDefaultMode(blockStatus.Mode) {
				blockStatusMessage = append(blockStatusMessage, blockStatus)
			} else {
				redoBlockStatusMessage = append(redoBlockStatusMessage, blockStatus)
			}
			delay.Reset(10 * time.Millisecond)
		loop:
			for {
				select {
				case blockStatus := <-e.sharedInfo.GetBlockStatusesChan():
					if common.IsDefaultMode(blockStatus.Mode) {
						blockStatusMessage = append(blockStatusMessage, blockStatus)
					} else {
						redoBlockStatusMessage = append(redoBlockStatusMessage, blockStatus)
					}
				case <-delay.C:
					break loop
				}
			}

			if len(blockStatusMessage) != 0 {
				enqueueBlockStatus(blockStatusMessage, common.DefaultMode)
			}
			if len(redoBlockStatusMessage) != 0 {
				enqueueBlockStatus(redoBlockStatusMessage, common.RedoMode)
			}
		}
	}
}

// collectComponentStatusWhenStatesChanged collect the component status info when the dispatchers states changed,
// such as --> working; --> stopped; --> stopping
// we will do a batch for the status, then send to heartbeatRequestQueue
func (e *DispatcherManager) collectComponentStatusWhenChanged(ctx context.Context) {
	delay := time.NewTimer(0)
	defer delay.Stop()
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
		case tableSpanStatus := <-e.sharedInfo.GetStatusesChan():
			statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
			if common.IsDefaultMode(tableSpanStatus.Mode) {
				newWatermark.Seq = tableSpanStatus.Seq
				if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newWatermark.CheckpointTs {
					newWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
				}
			}
			delay.Reset(10 * time.Millisecond)
		loop:
			for {
				select {
				case tableSpanStatus := <-e.sharedInfo.GetStatusesChan():
					statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
					if common.IsDefaultMode(tableSpanStatus.Mode) {
						if newWatermark.Seq < tableSpanStatus.Seq {
							newWatermark.Seq = tableSpanStatus.Seq
						}
						if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newWatermark.CheckpointTs {
							newWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
						}
					}
				case <-delay.C:
					break loop
				}
			}
			var message heartbeatpb.HeartBeatRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.Statuses = statusMessage
			message.Watermark = newWatermark
			// FIXME: need to send redo watermark?
			// message.RedoWatermark = newRedoWatermark
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
func (e *DispatcherManager) aggregateDispatcherHeartbeats(needCompleteStatus bool) *heartbeatpb.HeartBeatRequest {
	message := heartbeatpb.HeartBeatRequest{
		ChangefeedID:    e.changefeedID.ToPB(),
		CompeleteStatus: needCompleteStatus,
		Watermark:       heartbeatpb.NewMaxWatermark(),
		RedoWatermark:   heartbeatpb.NewMaxWatermark(),
	}

	toCleanMap := make([]*cleanMap, 0)
	dispatcherCount := 0

	if e.RedoEnable {
		redoSeq := e.redoDispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem *dispatcher.RedoDispatcher) {
			dispatcherCount++
			status, cleanMap, watermark := getDispatcherStatus(id, dispatcherItem, needCompleteStatus)
			if status != nil {
				message.Statuses = append(message.Statuses, status)
			}
			if cleanMap != nil {
				toCleanMap = append(toCleanMap, cleanMap)
			}
			if watermark != nil {
				message.RedoWatermark.UpdateMin(*watermark)
			}
		})
		message.RedoWatermark.Seq = redoSeq
	}
	seq := e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcherItem *dispatcher.EventDispatcher) {
		dispatcherCount++
		status, cleanMap, watermark := getDispatcherStatus(id, dispatcherItem, needCompleteStatus)
		if status != nil {
			message.Statuses = append(message.Statuses, status)
		}
		if cleanMap != nil {
			toCleanMap = append(toCleanMap, cleanMap)
		}
		if watermark != nil {
			message.Watermark.Update(*watermark)
		}
	})
	message.Watermark.Seq = seq
	e.latestWatermark.Set(message.Watermark)

	// if the event dispatcher manager is closing, we don't to remove the stopped dispatchers.
	if !e.closing.Load() {
		for _, m := range toCleanMap {
			dispatcherCount--
			// Cancel the corresponding remove task if exists
			if handle, ok := e.removeTaskHandles.LoadAndDelete(m.id); ok {
				handle.(*threadpool.TaskHandle).Cancel()
				log.Debug("cancelled remove task for dispatcher",
					zap.Stringer("dispatcherID", m.id))
			}
			if common.IsRedoMode(m.mode) {
				e.cleanRedoDispatcher(m.id, m.schemaID)
			} else {
				e.cleanEventDispatcher(m.id, m.schemaID)
			}
		}
	}

	// If needCompleteStatus is true, we need to send the dispatcher heartbeat to the event service.
	if needCompleteStatus {
		eventServiceDispatcherHeartbeat := &event.DispatcherHeartbeat{
			Version:              event.DispatcherHeartbeatVersion1,
			DispatcherCount:      0,
			DispatcherProgresses: make([]event.DispatcherProgress, 0, dispatcherCount),
		}
		if e.RedoEnable {
			e.redoDispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.RedoDispatcher) {
				eventServiceDispatcherHeartbeat.Append(event.NewDispatcherProgress(id, message.Watermark.CheckpointTs))
			})
		}
		e.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.EventDispatcher) {
			eventServiceDispatcherHeartbeat.Append(event.NewDispatcherProgress(id, message.Watermark.CheckpointTs))
		})
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

func (e *DispatcherManager) MergeDispatcher(dispatcherIDs []common.DispatcherID, mergedDispatcherID common.DispatcherID, mode int64) *MergeCheckTask {
	if common.IsRedoMode(mode) {
		return e.mergeRedoDispatcher(dispatcherIDs, mergedDispatcherID)
	}
	return e.mergeEventDispatcher(dispatcherIDs, mergedDispatcherID)
}

// mergeEventDispatcher merges the mulitple event dispatchers belonging to the same table with consecutive ranges.
func (e *DispatcherManager) mergeEventDispatcher(dispatcherIDs []common.DispatcherID, mergedDispatcherID common.DispatcherID) *MergeCheckTask {
	// Step 1: check the dispatcherIDs and mergedDispatcherID are valid:
	//         1. whether the mergedDispatcherID is not exist in the dispatcherMap
	//         2. whether the dispatcherIDs exist in the dispatcherMap
	//         3. whether the dispatcherIDs belong to the same table
	//         4. whether the dispatcherIDs have consecutive ranges
	//         5. whether the dispatcher in working status.

	ok := prepareMergeDispatcher(e.changefeedID, dispatcherIDs, e.dispatcherMap, mergedDispatcherID, e.sharedInfo.GetStatusesChan())
	if !ok {
		return nil
	}

	mergedSpan, fakeStartTs, schemaID := createMergedSpan(e.changefeedID, dispatcherIDs, e.dispatcherMap)
	if mergedSpan == nil {
		return nil
	}

	mergedDispatcher := dispatcher.NewEventDispatcher(
		mergedDispatcherID,
		mergedSpan,
		fakeStartTs, // real startTs will be calculated later.
		schemaID,
		e.schemaIDToDispatchers,
		false, // skipSyncpointAtStartTs
		false, // skipDMLAsStartTs will be set later after calculating real startTs
		0,     // currentPDTs will be calculated later.
		e.sink,
		e.sharedInfo,
		e.RedoEnable,
		&e.redoGlobalTs,
	)

	log.Info("new dispatcher created(merge dispatcher)",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", mergedDispatcherID),
		zap.String("tableSpan", common.FormatTableSpan(mergedSpan)))

	registerMergeDispatcher(e.changefeedID, dispatcherIDs, e.dispatcherMap, mergedDispatcherID, mergedDispatcher, e.schemaIDToDispatchers, e.metricEventDispatcherCount, e.sinkQuota)
	return newMergeCheckTask(e, mergedDispatcher, dispatcherIDs)
}

// ==== remove and clean related functions ====

func (e *DispatcherManager) TryClose(removeChangefeed bool) bool {
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

func (e *DispatcherManager) close(removeChangefeed bool) {
	log.Info("closing event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID))

	defer e.closing.Store(false)
	if e.RedoEnable {
		closeAllDispatchers(e.changefeedID, e.redoDispatcherMap, e.redoSink.SinkType())
		log.Info("closed all redo dispatchers",
			zap.Stringer("changefeedID", e.changefeedID))
	}

	closeAllDispatchers(e.changefeedID, e.dispatcherMap, e.sink.SinkType())
	log.Info("closed all event dispatchers",
		zap.Stringer("changefeedID", e.changefeedID))

	err := appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RemoveDispatcherManager(e.changefeedID)
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

	if e.RedoEnable {
		e.redoSink.Close(removeChangefeed)
	}
	e.sink.Close(removeChangefeed)
	e.cancel()
	e.wg.Wait()

	e.removeTaskHandles.Range(func(key, value interface{}) bool {
		handle := value.(*threadpool.TaskHandle)
		handle.Cancel()
		return true
	})

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.DispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerCheckpointTsLagGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerResolvedTsLagGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")

	e.closed.Store(true)
	log.Info("event dispatcher manager closed",
		zap.Stringer("changefeedID", e.changefeedID))
}

// cleanEventDispatcher is called when the event dispatcher is removed successfully.
func (e *DispatcherManager) cleanEventDispatcher(id common.DispatcherID, schemaID int64) {
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

func (e *DispatcherManager) cleanMetrics() {
	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "eventDispatcher")
	metrics.DispatcherManagerCheckpointTsGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerResolvedTsGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerCheckpointTsLagGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())
	metrics.DispatcherManagerResolvedTsLagGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
}

// ==== remove and clean related functions END ====
