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
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/routing"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/threadpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

const (
	maxBlockStatusesPerRequest = 2048
	// The local buffer only needs to cover the short gap before dispatcher
	// manager batching drains it. Longer retries are absorbed by the request
	// queue dedupe, so keeping this queue modest avoids preallocating a large
	// value-retention window in front of the manager.
	blockStatusBufferSize = 16 * 1024
)

// IsWritePathClosedError reports whether err means the local write path has
// already been fenced. Callers should stop the in-flight local request instead
// of treating it as a successful dispatcher creation.
func IsWritePathClosedError(err error) bool {
	if err == nil {
		return false
	}
	code, ok := errors.RFCCode(err)
	return ok && code == errors.ErrDispatcherManagerWritePathClosed.RFCCode()
}

func newWritePathClosedError() error {
	return errors.ErrDispatcherManagerWritePathClosed.FastGenByArgs()
}

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
	ctx          context.Context
	changefeedID common.ChangeFeedID
	keyspaceID   uint32
	// mu is only used for table trigger dispatcher
	mu sync.Mutex

	// meta is used to store the meta info of the event dispatcher manager
	// it's used to avoid data race when we update the maintainerID and maintainerEpoch
	meta struct {
		sync.Mutex
		maintainerEpoch uint64
		maintainerID    node.ID
	}
	// MaintainerFenceMu serializes maintainer owner/epoch changes with request
	// fence checks and scheduler side effects.
	MaintainerFenceMu sync.Mutex

	pdClock pdutil.Clock

	config *config.ChangefeedConfig

	// tableTriggerEventDispatcher is a special dispatcher, that is responsible for handling ddl and checkpoint events.
	tableTriggerEventDispatcher *dispatcher.EventDispatcher
	// tableTriggerRedoDispatcher is a special redo dispatcher, that is responsible for handling ddl and checkpoint events.
	tableTriggerRedoDispatcher *dispatcher.RedoDispatcher
	// dispatcherMap restore all the dispatchers in the DispatcherManager, including table trigger event dispatcher
	dispatcherMap *DispatcherMap[*dispatcher.EventDispatcher]
	// redoDispatcherMap restore all the redo dispatchers in the DispatcherManager, including table trigger redo dispatcher
	redoDispatcherMap *DispatcherMap[*dispatcher.RedoDispatcher]
	// currentOperatorMap stores one in-flight scheduling request per dispatcherID.
	//
	// The value carries sender and maintainer epoch so bootstrap recovery can
	// return only current-epoch operators, and precheck can replace stale entries.
	// Entries must be deleted on completion, otherwise future requests for the
	// same dispatcherID will be ignored.
	currentOperatorMap sync.Map // map[common.DispatcherID]SchedulerDispatcherRequest (in dispatcher manager, not heartbeatpb)
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
	// redoEnabled is immutable and set to true if enabled.
	redoEnabled bool
	// redoReady set to true after the redo components are fully initialized and safe for concurrent access.
	redoReady atomic.Bool
	redoSink  *redo.Sink
	// redoGlobalTs stores the resolved-ts of the redo metadata and blocks events in the common dispatcher where the commit-ts is greater than the resolved-ts.
	redoGlobalTs atomic.Uint64

	latestWatermark     Watermark
	latestRedoWatermark Watermark

	closing         atomic.Bool
	closed          atomic.Bool
	writePathMu     sync.Mutex
	writePathClosed atomic.Bool
	// removeChangefeedRequested is sticky once any close request asks for removed=true.
	// A later removed=false request must not downgrade the final cleanup semantics.
	removeChangefeedRequested atomic.Bool
	// removeChangefeedCleaned records whether the best-effort remove-only cleanup has finished.
	// It is intentionally not part of the TryClose success condition so the close contract
	// stays compatible with the historical behavior.
	removeChangefeedCleaned atomic.Bool
	// removeChangefeedCleanupRunning prevents duplicate background cleanup runs
	// while late remove requests or retries keep asking for remove semantics after
	// the base close path ends.
	removeChangefeedCleanupRunning atomic.Bool
	cancel                         context.CancelFunc
	wg                             sync.WaitGroup

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
	metricBlockStatusesChanLen             prometheus.Gauge

	metricTableTriggerRedoDispatcherCount prometheus.Gauge
	metricRedoEventDispatcherCount        prometheus.Gauge
	metricRedoCreateDispatcherDuration    prometheus.Observer
}

// return actual startTs of the table trigger event dispatcher
// when the table trigger event dispatcher is in this event dispatcher manager
func NewDispatcherManager(
	keyspaceID uint32,
	changefeedID common.ChangeFeedID,
	cfConfig *config.ChangefeedConfig,
	tableTriggerEventDispatcherID,
	tableTriggerRedoDispatcherID *heartbeatpb.DispatcherID,
	startTs uint64,
	maintainerID node.ID,
	maintainerEpoch uint64,
	newChangefeed bool,
	registerInitializing func(*DispatcherManager) bool,
) (manager *DispatcherManager, err error) {
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

	manager = &DispatcherManager{
		ctx:                   ctx,
		dispatcherMap:         newDispatcherMap[*dispatcher.EventDispatcher](),
		currentOperatorMap:    sync.Map{},
		changefeedID:          changefeedID,
		keyspaceID:            keyspaceID,
		pdClock:               pdClock,
		cancel:                cancel,
		config:                cfConfig,
		latestWatermark:       NewWatermark(startTs),
		latestRedoWatermark:   NewWatermark(startTs),
		schemaIDToDispatchers: dispatcher.NewSchemaIDToDispatchers(),
		sinkQuota:             cfConfig.MemoryQuota,
		redoEnabled:           isRedoConfigEnabled(cfConfig),

		metricTableTriggerEventDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricEventDispatcherCount:             metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricCreateDispatcherDuration:         metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "eventDispatcher"),
		metricCheckpointTs:                     metrics.DispatcherManagerCheckpointTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricCheckpointTsLag:                  metrics.DispatcherManagerCheckpointTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTs:                       metrics.DispatcherManagerResolvedTsGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricResolvedTsLag:                    metrics.DispatcherManagerResolvedTsLagGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricBlockStatusesChanLen:             metrics.DispatcherManagerBlockStatusesChanLenGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),

		metricTableTriggerRedoDispatcherCount: metrics.TableTriggerEventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
		metricRedoEventDispatcherCount:        metrics.EventDispatcherGauge.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
		metricRedoCreateDispatcherDuration:    metrics.CreateDispatcherDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name(), "redoDispatcher"),
	}

	// Trust only the explicit request maintainer epoch for receiver fencing. The
	// config epoch may be newer than an old rolling-upgrade request and must not
	// turn epoch 0 compatibility traffic into strict-mode traffic.
	manager.meta.maintainerEpoch = maintainerEpoch
	manager.meta.maintainerID = maintainerID
	cleanupManager := manager
	defer func() {
		if err != nil && cleanupManager != nil {
			cleanupManager.LocalFence()
			manager = nil
		}
	}()
	// The manager must be fenceable before any write-capable resource is initialized.
	if registerInitializing != nil && !registerInitializing(manager) {
		return nil, newWritePathClosedError()
	}

	// Set Sync Point Config
	var syncPointConfig *syncpoint.SyncPointConfig
	if cfConfig.EnableSyncPoint {
		// TODO: confirm that parameter validation is done at the setting location, so no need to check again here
		syncPointConfig = &syncpoint.SyncPointConfig{
			SyncPointInterval:  cfConfig.SyncPointInterval,
			SyncPointRetention: cfConfig.SyncPointRetention,
		}
	}

	createdSink, err := sink.New(ctx, manager.config, manager.changefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manager.writePathMu.Lock()
	if manager.writePathClosed.Load() {
		manager.writePathMu.Unlock()
		createdSink.Close()
		return nil, newWritePathClosedError()
	}
	manager.sink = createdSink
	manager.writePathMu.Unlock()

	sinkType := manager.sink.SinkType()
	if sinkType != common.KafkaSinkType {
		ignoreUpdateOnlyColumnsRuleCount := countIgnoreUpdateOnlyColumnsRules(cfConfig.Filter)
		if ignoreUpdateOnlyColumnsRuleCount > 0 {
			log.Warn("ignore update only columns is configured but does not take effect for this sink",
				zap.Stringer("changefeedID", changefeedID),
				zap.String("sinkType", metrics.DownstreamTypeFromSinkURI(manager.config.SinkURI)),
				zap.Int("eventFilterRuleCount", ignoreUpdateOnlyColumnsRuleCount))
		}
	}

	// Determine outputRawChangeEvent based on sink type
	var outputRawChangeEvent bool
	switch sinkType {
	case common.CloudStorageSinkType:
		outputRawChangeEvent = manager.config.SinkConfig.CloudStorageConfig.GetOutputRawChangeEvent()
	case common.KafkaSinkType:
		outputRawChangeEvent = manager.config.SinkConfig.KafkaConfig.GetOutputRawChangeEvent()
	}

	router, err := routing.NewRouter(
		manager.changefeedID,
		util.GetOrZero(manager.config.SinkConfig.CaseSensitive),
		manager.config.SinkConfig.DispatchRules,
	)
	if err != nil {
		return nil, err
	}

	batchCounts, batchBytes := manager.getEventCollectorBatchCountAndBytes(manager.sink)
	// Create shared info for all dispatchers
	sharedInfo := dispatcher.NewSharedInfo(
		manager.changefeedID,
		manager.config.TimeZone,
		manager.config.BDRMode,
		manager.config.EnableActiveActive,
		outputRawChangeEvent,
		integrityCfg,
		filterCfg,
		syncPointConfig,
		manager.config.SinkConfig.TxnAtomicity,
		manager.config.EnableSplittableCheck,
		router,
		batchCounts,
		batchBytes,
		make(chan dispatcher.TableSpanStatusWithSeq, 8192),
		blockStatusBufferSize,
		make(chan error, 1),
	)
	manager.writePathMu.Lock()
	if manager.writePathClosed.Load() {
		manager.writePathMu.Unlock()
		sharedInfo.Close()
		return nil, newWritePathClosedError()
	}
	manager.sharedInfo = sharedInfo
	manager.writePathMu.Unlock()

	// Register Event Dispatcher Manager in HeartBeatCollector,
	// which is responsible for communication with the maintainer.
	manager.writePathMu.Lock()
	if manager.writePathClosed.Load() {
		manager.writePathMu.Unlock()
		return nil, newWritePathClosedError()
	}
	err = appcontext.GetService[*HeartBeatCollector](appcontext.HeartbeatCollector).RegisterDispatcherManager(manager)
	manager.writePathMu.Unlock()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// init table trigger event dispatcher when tableTriggerEventDispatcherID is not nil
	if tableTriggerEventDispatcherID != nil {
		err = manager.NewTableTriggerEventDispatcher(tableTriggerEventDispatcherID, startTs, newChangefeed)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	err = initRedoComponet(ctx, manager, changefeedID, tableTriggerRedoDispatcherID, startTs, newChangefeed)
	if err != nil {
		return nil, errors.Trace(err)
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

	log.Info("dispatcher manager initialized",
		zap.Stringer("changefeedID", changefeedID),
		zap.Stringer("maintainerID", maintainerID),
		zap.Uint64("startTs", startTs),
		zap.Uint64("sinkQuota", manager.sinkQuota),
		zap.Uint64("redoQuota", manager.redoQuota),
		zap.Bool("redoEnable", manager.IsRedoEnabled()),
		zap.Bool("outputRawChangeEvent", manager.sharedInfo.IsOutputRawChangeEvent()),
		zap.String("filterConfig", filterCfg.String()),
	)
	return manager, nil
}

func countIgnoreUpdateOnlyColumnsRules(filter *config.FilterConfig) int {
	if filter == nil {
		return 0
	}
	count := 0
	for _, rule := range filter.EventFilters {
		if rule != nil && len(rule.IgnoreUpdateOnlyColumns) > 0 {
			count++
		}
	}
	return count
}

func (e *DispatcherManager) getEventCollectorBatchCountAndBytes(s sink.Sink) (int, int) {
	var (
		batchCount = s.BatchCount()
		batchBytes = s.BatchBytes()
	)
	if e.config.EventCollectorBatchCount != nil {
		batchCount = *e.config.EventCollectorBatchCount
	}
	if e.config.EventCollectorBatchBytes != nil {
		batchBytes = *e.config.EventCollectorBatchBytes
	}
	return batchCount, batchBytes
}

func (e *DispatcherManager) NewTableTriggerEventDispatcher(id *heartbeatpb.DispatcherID, startTs uint64, newChangefeed bool) error {
	if e.GetTableTriggerEventDispatcher() != nil {
		return errors.ErrChangefeedInitTableTriggerDispatcherFailed.FastGenByArgs("table trigger event dispatcher existed!")
	}
	infos := map[common.DispatcherID]dispatcherCreateInfo{}
	dispatcherID := common.NewDispatcherIDFromPB(id)
	infos[dispatcherID] = dispatcherCreateInfo{
		ID:        dispatcherID,
		TableSpan: common.KeyspaceDDLSpan(e.keyspaceID),
		StartTs:   startTs,
		SchemaID:  0,
	}
	err := e.newEventDispatchers(infos, newChangefeed)
	if err != nil {
		return errors.Trace(err)
	}
	tableTriggerDispatcher := e.GetTableTriggerEventDispatcher()
	if tableTriggerDispatcher == nil {
		if e.writePathClosed.Load() {
			return newWritePathClosedError()
		}
		return errors.ErrChangefeedInitTableTriggerDispatcherFailed.
			FastGenByArgs("table trigger event dispatcher was not created")
	}
	log.Info("table trigger event dispatcher created",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", tableTriggerDispatcher.GetId()),
		zap.Uint64("startTs", tableTriggerDispatcher.GetStartTs()),
	)
	return nil
}

func (e *DispatcherManager) InitalizeTableTriggerEventDispatcher(schemaInfo []*heartbeatpb.SchemaInfo) error {
	e.writePathMu.Lock()
	if e.writePathClosed.Load() {
		e.writePathMu.Unlock()
		return newWritePathClosedError()
	}
	tableTriggerDispatcher := e.GetTableTriggerEventDispatcher()
	if tableTriggerDispatcher == nil {
		e.writePathMu.Unlock()
		return nil
	}
	needAddDispatcher, err := tableTriggerDispatcher.InitializeTableSchemaStore(schemaInfo)
	if err != nil {
		e.writePathMu.Unlock()
		return errors.Trace(err)
	}
	e.writePathMu.Unlock()
	if !needAddDispatcher {
		return nil
	}
	if e.writePathClosed.Load() {
		return newWritePathClosedError()
	}
	// before bootstrap finished, cannot send any event.
	success := tableTriggerDispatcher.EmitBootstrap(e.writePathClosed.Load)
	if e.writePathClosed.Load() {
		return newWritePathClosedError()
	}
	if !success {
		return errors.ErrDispatcherFailed.GenWithStackByArgs()
	}

	e.writePathMu.Lock()
	defer e.writePathMu.Unlock()
	if e.writePathClosed.Load() {
		return newWritePathClosedError()
	}
	// table trigger event dispatcher can register to event collector to receive events after finish the initial table schema store from the maintainer.
	appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(tableTriggerDispatcher, e.sinkQuota)

	// The table trigger event dispatcher needs changefeed-level checkpoint updates only
	// when downstream components must maintain table names (for non-MySQL sinks), or
	// when MySQL/TiDB sink runs in enable-active-active mode to update the progress table.
	needCheckpointUpdates := commonEvent.NeedTableNameStoreAndCheckpointTs(e.sink.SinkType() == common.MysqlSinkType, e.sharedInfo.EnableActiveActive())
	if needCheckpointUpdates {
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
	if e.writePathClosed.Load() {
		return newWritePathClosedError()
	}
	start := time.Now()
	currentPdTs := e.pdClock.CurrentTS()

	dispatcherIds, tableIds, startTsList, tableSpans, schemaIds, scheduleSkipDMLAsStartTsList := prepareCreateDispatcher(infos, e.dispatcherMap)
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
		skipDMLAsStartTs := resolveSkipDMLAsStartTs(
			newStartTsList[idx],
			startTsList[idx],
			scheduleSkipDMLAsStartTsList[idx],
			skipDMLAsStartTsList[idx],
		)
		d := dispatcher.NewEventDispatcher(
			id,
			tableSpans[idx],
			uint64(newStartTsList[idx]),
			schemaIds[idx],
			e.schemaIDToDispatchers,
			skipSyncpointAtStartTsList[idx],
			skipDMLAsStartTs,
			currentPdTs,
			e.sink,
			e.sharedInfo,
			e.IsRedoEnabled(),
			&e.redoGlobalTs,
		)
		if e.heartBeatTask == nil {
			e.heartBeatTask = newHeartBeatTask(e)
		}

		e.writePathMu.Lock()
		if e.writePathClosed.Load() {
			e.writePathMu.Unlock()
			d.Remove()
			return newWritePathClosedError()
		}
		if d.IsTableTriggerDispatcher() {
			if util.GetOrZero(e.config.SinkConfig.SendAllBootstrapAtStart) {
				d.BootstrapState = dispatcher.BootstrapNotStarted
			}
			e.SetTableTriggerEventDispatcher(d)
		} else {
			e.schemaIDToDispatchers.Set(schemaIds[idx], id)
			// we don't register table trigger event dispatcher in event collector, when created.
			// Table trigger event dispatcher is a special dispatcher,
			// it need to wait get the initial table schema store from the maintainer, then will register to event collector to receive events.
			appcontext.GetService[*eventcollector.EventCollector](appcontext.EventCollector).AddDispatcher(d, e.sinkQuota)
		}

		seq := e.dispatcherMap.Set(id, d)
		d.SetSeq(seq)
		e.writePathMu.Unlock()

		if d.IsTableTriggerDispatcher() {
			e.metricTableTriggerEventDispatcherCount.Inc()
		} else {
			e.metricEventDispatcherCount.Inc()
		}

		log.Info("new dispatcher created",
			zap.Stringer("changefeedID", e.changefeedID),
			zap.Stringer("dispatcherID", id),
			zap.String("tableSpan", common.FormatTableSpan(tableSpans[idx])),
			zap.Int64("startTs", newStartTsList[idx]),
			zap.Bool("skipDMLAsStartTs", skipDMLAsStartTs))
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
	enqueueBlockStatus := func(blockStatusMessage []*heartbeatpb.TableSpanBlockStatus, mode int64) {
		// Split oversized batches so one protobuf message does not monopolize
		// serialization, transport, and maintainer-side processing.
		for start := 0; start < len(blockStatusMessage); start += maxBlockStatusesPerRequest {
			end := min(start+maxBlockStatusesPerRequest, len(blockStatusMessage))
			// Copy each chunk so queue-side in-place filtering owns the backing
			// array and cannot mutate another batch's slice accidentally.
			chunk := make([]*heartbeatpb.TableSpanBlockStatus, end-start)
			copy(chunk, blockStatusMessage[start:end])
			var message heartbeatpb.BlockStatusRequest
			message.ChangefeedID = e.changefeedID.ToPB()
			message.BlockStatuses = chunk
			message.Mode = mode
			e.blockStatusRequestQueue.Enqueue(&BlockStatusRequestWithTargetID{TargetID: e.GetMaintainerID(), Request: &message})
		}
	}
	for {
		blockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		redoBlockStatusMessage := make([]*heartbeatpb.TableSpanBlockStatus, 0)
		blockStatus := e.sharedInfo.TakeBlockStatus(ctx)
		if blockStatus == nil {
			return
		}
		if common.IsDefaultMode(blockStatus.Mode) {
			blockStatusMessage = append(blockStatusMessage, blockStatus)
		} else {
			redoBlockStatusMessage = append(redoBlockStatusMessage, blockStatus)
		}

		// Batch from the first observed status for up to 10ms. We drain ready
		// entries first, then keep waiting until the same deadline so late arrivals
		// still join the current request instead of being delayed to the next batch.
		deadline := time.Now().Add(10 * time.Millisecond)
	loop:
		for {
			var ok bool
			blockStatus, ok = e.sharedInfo.TryTakeBlockStatus()
			if !ok {
				// Once the local queue is drained, keep waiting until the batch
				// deadline so late arrivals can still join the current request.
				waitCtx, cancel := context.WithDeadline(ctx, deadline)
				blockStatus = e.sharedInfo.TakeBlockStatus(waitCtx)
				cancel()
				if blockStatus == nil {
					if ctx.Err() != nil {
						return
					}
					break loop
				}
			}
			if common.IsDefaultMode(blockStatus.Mode) {
				blockStatusMessage = append(blockStatusMessage, blockStatus)
			} else {
				redoBlockStatusMessage = append(redoBlockStatusMessage, blockStatus)
			}
		}

		e.metricBlockStatusesChanLen.Set(float64(e.sharedInfo.BlockStatusLen()))
		if len(blockStatusMessage) != 0 {
			enqueueBlockStatus(blockStatusMessage, common.DefaultMode)
		}
		if len(redoBlockStatusMessage) != 0 {
			enqueueBlockStatus(redoBlockStatusMessage, common.RedoMode)
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
		redoWatermark := e.latestRedoWatermark.Get()
		newRedoWatermark := &heartbeatpb.Watermark{
			CheckpointTs: redoWatermark.CheckpointTs,
			ResolvedTs:   redoWatermark.ResolvedTs,
			Seq:          redoWatermark.Seq,
		}
		select {
		case <-ctx.Done():
			return
		case tableSpanStatus := <-e.sharedInfo.GetStatusesChan():
			statusMessage = append(statusMessage, tableSpanStatus.TableSpanStatus)
			if common.IsDefaultMode(tableSpanStatus.Mode) {
				// Note: tableSpanStatus.Seq is the seq assigned when that dispatcher was created.
				// status messages can arrive out-of-order and Seq has no ordering relationship with
				// per-dispatcher checkpoint/resolved ts.
				//
				// - Keep Watermark.Seq monotonic to avoid maintainer dropping the watermark as stale
				//   while still applying status updates (statuses are handled regardless of watermark Seq).
				// - Still update CheckpointTs with min() regardless of Seq ordering; the periodic
				//   aggregateDispatcherHeartbeats() is responsible for advancing the watermark.
				if newWatermark.Seq < tableSpanStatus.Seq {
					newWatermark.Seq = tableSpanStatus.Seq
				}
				if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newWatermark.CheckpointTs {
					newWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
				}
			} else {
				// Same rule applies to redo watermark.
				if newRedoWatermark.Seq < tableSpanStatus.Seq {
					newRedoWatermark.Seq = tableSpanStatus.Seq
				}
				if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newRedoWatermark.CheckpointTs {
					newRedoWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
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
					} else {
						if newRedoWatermark.Seq < tableSpanStatus.Seq {
							newRedoWatermark.Seq = tableSpanStatus.Seq
						}
						if tableSpanStatus.CheckpointTs != 0 && tableSpanStatus.CheckpointTs < newRedoWatermark.CheckpointTs {
							newRedoWatermark.CheckpointTs = tableSpanStatus.CheckpointTs
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
			message.RedoWatermark = newRedoWatermark
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

	if e.IsRedoReady() {
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
		e.latestRedoWatermark.Set(message.RedoWatermark)
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

// mergeEventDispatcher merges the multiple event dispatchers belonging to the same table with consecutive ranges.
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
		e.IsRedoEnabled(),
		&e.redoGlobalTs,
	)

	log.Info("new dispatcher created(merge dispatcher)",
		zap.Stringer("changefeedID", e.changefeedID),
		zap.Stringer("dispatcherID", mergedDispatcherID),
		zap.String("tableSpan", common.FormatTableSpan(mergedSpan)))

	e.writePathMu.Lock()
	if e.writePathClosed.Load() {
		e.writePathMu.Unlock()
		mergedDispatcher.Remove()
		return nil
	}
	registerMergeDispatcher(e.changefeedID, dispatcherIDs, e.dispatcherMap, mergedDispatcherID, mergedDispatcher, e.schemaIDToDispatchers, e.metricEventDispatcherCount, e.sinkQuota)
	e.writePathMu.Unlock()
	return newMergeCheckTask(e, mergedDispatcher, dispatcherIDs)
}

// ==== remove and clean related functions ====

func (e *DispatcherManager) TryClose(removeChangefeed bool) bool {
	if removeChangefeed {
		e.removeChangefeedRequested.Store(true)
	}
	if e.closed.Load() {
		e.tryScheduleRemoveChangefeedCleanup()
		return true
	}
	if !e.closing.CompareAndSwap(false, true) {
		return e.closed.Load()
	}

	go e.close()
	return false
}

// LocalFence stops the local write path immediately without waiting for
// dispatcher progress to drain. The remaining cleanup continues asynchronously.
func (e *DispatcherManager) LocalFence() {
	if e.closed.Load() {
		return
	}
	startClose := e.closing.CompareAndSwap(false, true)
	e.stopWritePath(true)
	if startClose {
		go e.finishClose()
	}
}

func (e *DispatcherManager) close() {
	log.Info("closing event dispatcher manager",
		zap.Stringer("changefeedID", e.changefeedID))

	e.stopWritePath(false)
	e.finishClose()
}

func (e *DispatcherManager) stopWritePath(cancelFirst bool) {
	e.writePathMu.Lock()
	if e.writePathClosed.Load() {
		e.writePathMu.Unlock()
		return
	}
	e.writePathClosed.Store(true)
	e.writePathMu.Unlock()

	log.Info("stopping dispatcher manager write path",
		zap.Stringer("changefeedID", e.changefeedID))

	if cancelFirst && e.cancel != nil {
		e.cancel()
	}

	if e.IsRedoEnabled() && e.redoSink != nil {
		closeAllDispatchers(e.changefeedID, e.redoDispatcherMap, e.redoSink.SinkType())
		log.Info("closed all redo dispatchers",
			zap.Stringer("changefeedID", e.changefeedID))
		if heartbeatCollector, ok := appcontext.TryGetService[*HeartBeatCollector](appcontext.HeartbeatCollector); ok {
			err := heartbeatCollector.RemoveRedoMessage(e.changefeedID)
			if err != nil {
				log.Error("remove redo message failed",
					zap.Stringer("changefeedID", e.changefeedID),
					zap.Error(err),
				)
			}
		} else {
			log.Warn("heartbeat collector is not available when stopping redo write path",
				zap.Stringer("changefeedID", e.changefeedID),
			)
		}
	}

	if e.sink != nil {
		closeAllDispatchers(e.changefeedID, e.dispatcherMap, e.sink.SinkType())
	}
	log.Info("closed all event dispatchers",
		zap.Stringer("changefeedID", e.changefeedID))

	if heartbeatCollector, ok := appcontext.TryGetService[*HeartBeatCollector](appcontext.HeartbeatCollector); ok {
		err := heartbeatCollector.RemoveDispatcherManager(e.changefeedID)
		if err != nil {
			log.Error("remove dispatcher manager from heartbeat collector failed",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Error(err),
			)
		}
	} else {
		log.Warn("heartbeat collector is not available when stopping dispatcher manager write path",
			zap.Stringer("changefeedID", e.changefeedID),
		)
	}

	// heartbeatTask only will be generated when create new dispatchers.
	// We check heartBeatTask after we remove the stream in heartbeat collector,
	// so we won't get add dispatcher messages to create heartbeatTask.
	// Thus there will not data race when we check heartBeatTask.
	if e.heartBeatTask != nil {
		e.heartBeatTask.Cancel()
	}

	if !cancelFirst && e.cancel != nil {
		e.cancel()
	}

	if e.sharedInfo != nil {
		e.sharedInfo.Close()
	}

	log.Info("shared info closed", zap.Stringer("changefeedID", e.changefeedID))

	if e.IsRedoEnabled() && e.redoSink != nil {
		e.redoSink.Close()
	}
	if e.sink != nil {
		e.sink.Close()
	}
	log.Info("sink closed", zap.Stringer("changefeedID", e.changefeedID))
}

func (e *DispatcherManager) addCheckpointTs(checkpointTs uint64) {
	if e.writePathClosed.Load() {
		return
	}
	if e.GetTableTriggerEventDispatcher() == nil || e.sink == nil {
		return
	}
	if e.writePathClosed.Load() {
		return
	}
	e.sink.AddCheckpointTs(checkpointTs)
}

func (e *DispatcherManager) finishClose() {
	defer e.closing.Store(false)
	e.wg.Wait()
	if !e.closed.CompareAndSwap(false, true) {
		return
	}

	e.removeTaskHandles.Range(func(key, value interface{}) bool {
		handle := value.(*threadpool.TaskHandle)
		handle.Cancel()
		return true
	})

	e.cleanMetrics()

	e.tryScheduleRemoveChangefeedCleanup()
	log.Info("event dispatcher manager closed",
		zap.Stringer("changefeedID", e.changefeedID))
}

func (e *DispatcherManager) tryScheduleRemoveChangefeedCleanup() {
	if !e.removeChangefeedRequested.Load() {
		return
	}
	if e.removeChangefeedCleaned.Load() {
		return
	}
	if !e.removeChangefeedCleanupRunning.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer e.removeChangefeedCleanupRunning.Store(false)

		if err := e.runRemoveChangefeedCleanup(); err != nil {
			log.Warn("failed to cleanup removed changefeed",
				zap.Stringer("changefeedID", e.changefeedID),
				zap.Error(err))
			return
		}
		e.removeChangefeedCleaned.Store(true)
	}()
}

func (e *DispatcherManager) runRemoveChangefeedCleanup() error {
	if !e.removeChangefeedRequested.Load() || e.removeChangefeedCleaned.Load() {
		return nil
	}

	if e.IsRedoEnabled() {
		// Redo meta cleanup is the remove-only step for redo mode. It is safe to retry
		// because removeChangefeedCleaned is only set after all remove-only cleanup succeeds.
		if err := e.closeRedoMeta(true); err != nil {
			return errors.Trace(err)
		}
	}

	if mysqlSink, ok := e.sink.(*mysql.Sink); ok {
		// MySQL sink may still need ddl_ts cleanup after the base close path has already
		// released the long-lived DB connection, so keep the retryable remove step here.
		if err := mysqlSink.CleanupRemovedChangefeed(); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// cleanEventDispatcher is called when the event dispatcher is removed successfully.
func (e *DispatcherManager) cleanEventDispatcher(id common.DispatcherID, schemaID int64) {
	e.dispatcherMap.Delete(id)
	e.schemaIDToDispatchers.Delete(schemaID, id)
	e.currentOperatorMap.Delete(id)
	log.Debug("delete current working remove operator",
		zap.String("changefeedID", e.changefeedID.String()),
		zap.String("dispatcherID", id.String()),
	)
	tableTriggerEventDispatcher := e.GetTableTriggerEventDispatcher()
	if tableTriggerEventDispatcher != nil && tableTriggerEventDispatcher.GetId() == id {
		e.SetTableTriggerEventDispatcher(nil)
		e.metricTableTriggerEventDispatcherCount.Dec()
	} else {
		e.metricEventDispatcherCount.Dec()
	}
	log.Info("event dispatcher completely stopped, and delete it from event dispatcher manager",
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
	metrics.DispatcherManagerBlockStatusesChanLenGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name())

	metrics.TableTriggerEventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.EventDispatcherGauge.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
	metrics.CreateDispatcherDuration.DeleteLabelValues(e.changefeedID.Keyspace(), e.changefeedID.Name(), "redoDispatcher")
}

// ==== remove and clean related functions END ====
