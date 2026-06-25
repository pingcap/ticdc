// Copyright 2024 PingCAP, Inc.
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

package dispatchermanager

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

type DispatcherMap[T dispatcher.Dispatcher] struct {
	m sync.Map
	// sequence number is increasing when dispatcher is added.
	//
	// Seq is a generation marker for heartbeat reordering/deduplication only. It is NOT correlated with
	// checkpointTs/resolvedTs progress: a larger Seq does not imply a larger checkpointTs.
	//
	// Seq is used to prevent the fallback of changefeed's checkpointTs.
	// When some new dispatcher(table) is being added, the maintainer will block the forward of changefeed's checkpointTs
	// until the maintainer receive the message that the new dispatcher's component status change to working.
	//
	// Besides, heartbeat messages and table status messages have no strict order.
	// After dispatcher A is created, the event dispatcher manager may first send
	// a table status message showing the new dispatcher is working, and then send
	// a heartbeat with the current watermark calculated without the new dispatcher.
	// If that watermark checkpointTs is larger than the new dispatcher's startTs,
	// the next heartbeat calculated with the new dispatcher can be lower and cause
	// the changefeed checkpointTs to fall back.
	// To avoid fallback, each heartbeat carries a seq number collected from
	// collectComponentStatusWhenChanged and aggregateDispatcherHeartbeats. When a
	// table is added, the seq number increases, so the maintainer can ignore
	// outdated heartbeat messages.
	// In this way, even the above case happens, the changefeed's checkpointTs will not fallback.
	//
	// Here we don't need to make seq changes always atomic with the map changes.
	// Our target is only that the seq from ForEach is smaller than the seq from
	// Set when ForEach does not access the newly added dispatcher.
	// So we add seq after the dispatcher is add in the m for Set, and get the seq before do range for ForRange.
	seq atomic.Uint64
}

func newDispatcherMap[T dispatcher.Dispatcher]() *DispatcherMap[T] {
	dispatcherMap := &DispatcherMap[T]{
		m: sync.Map{},
	}
	dispatcherMap.seq.Store(0)
	return dispatcherMap
}

func (d *DispatcherMap[T]) Len() int {
	len := 0
	d.m.Range(func(_, _ interface{}) bool {
		len++
		return true
	})
	return len
}

func (d *DispatcherMap[T]) Get(id common.DispatcherID) (dispatcher T, exist bool) {
	dispatcherItem, ok := d.m.Load(id)
	if ok {
		return dispatcherItem.(T), ok
	}
	return
}

func (d *DispatcherMap[T]) GetSeq() uint64 {
	return d.seq.Load()
}

func (d *DispatcherMap[T]) Delete(id common.DispatcherID) {
	d.m.Delete(id)
}

func (d *DispatcherMap[T]) Set(id common.DispatcherID, dispatcher T) uint64 {
	d.m.Store(id, dispatcher)
	d.seq.Add(1)
	return d.seq.Load()
}

func (d *DispatcherMap[T]) ForEach(fn func(id common.DispatcherID, dispatcher T)) uint64 {
	seq := d.seq.Load()
	d.m.Range(func(key, value interface{}) bool {
		fn(key.(common.DispatcherID), value.(T))
		return true
	})
	return seq
}

func toFilterConfigPB(filter *config.FilterConfig) *eventpb.InnerFilterConfig {
	filterConfig := &eventpb.InnerFilterConfig{
		Rules:            filter.Rules,
		IgnoreTxnStartTs: filter.IgnoreTxnStartTs,
		EventFilters:     make([]*eventpb.EventFilterRule, 0),
	}

	for _, eventFilterRule := range filter.EventFilters {
		filterConfig.EventFilters = append(filterConfig.EventFilters, toEventFilterRulePB(eventFilterRule))
	}

	return filterConfig
}

func toEventFilterRulePB(rule *config.EventFilterRule) *eventpb.EventFilterRule {
	eventFilterPB := &eventpb.EventFilterRule{
		IgnoreInsertValueExpr:    util.GetOrZero(rule.IgnoreInsertValueExpr),
		IgnoreUpdateNewValueExpr: util.GetOrZero(rule.IgnoreUpdateNewValueExpr),
		IgnoreUpdateOldValueExpr: util.GetOrZero(rule.IgnoreUpdateOldValueExpr),
		IgnoreDeleteValueExpr:    util.GetOrZero(rule.IgnoreDeleteValueExpr),
	}
	eventFilterPB.IgnoreUpdateOnlyColumns = append(eventFilterPB.IgnoreUpdateOnlyColumns, rule.IgnoreUpdateOnlyColumns...)

	eventFilterPB.Matcher = append(eventFilterPB.Matcher, rule.Matcher...)

	for _, ignoreEvent := range rule.IgnoreEvent {
		eventFilterPB.IgnoreEvent = append(eventFilterPB.IgnoreEvent, string(ignoreEvent))
	}

	eventFilterPB.IgnoreSql = append(eventFilterPB.IgnoreSql, rule.IgnoreSQL...)

	return eventFilterPB
}

type Watermark struct {
	mutex sync.Mutex
	*heartbeatpb.Watermark
}

func NewWatermark(ts uint64) Watermark {
	return Watermark{
		Watermark: &heartbeatpb.Watermark{
			CheckpointTs: ts,
			ResolvedTs:   ts,
		},
	}
}

func (w *Watermark) Get() *heartbeatpb.Watermark {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.Watermark
}

func (w *Watermark) Set(watermark *heartbeatpb.Watermark) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.Watermark = watermark
}

func newSchedulerDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.GID, SchedulerDispatcherRequest, *DispatcherManager, *SchedulerDispatcherRequestHandler] {
	option := dynstream.NewOption()
	option.BatchCount = 1024
	ds := dynstream.NewParallelDynamicStream("scheduler-dispatcher-request",
		&SchedulerDispatcherRequestHandler{}, option)
	ds.Start()
	return ds
}

type SchedulerDispatcherRequest struct {
	From node.ID
	*heartbeatpb.ScheduleDispatcherRequest
}

// NewSchedulerDispatcherRequest carries the sender node with the schedule
// request so dispatcher-manager admission can fence stale maintainers.
func NewSchedulerDispatcherRequest(from node.ID, req *heartbeatpb.ScheduleDispatcherRequest) SchedulerDispatcherRequest {
	return SchedulerDispatcherRequest{From: from, ScheduleDispatcherRequest: req}
}

type SchedulerDispatcherRequestHandler struct{}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest SchedulerDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(scheduleDispatcherRequest.ChangefeedID)
}

// Handle handles the SchedulerDispatcherRequest events, which are operators from the maintainer to add or remove dispatchers.
//
// We persist each request in `dispatcherManager.currentOperatorMap` before executing it. This map acts as a small
// "bootstrap journal": if the maintainer fails over, dispatcher manager will include unfinished requests in its
// bootstrap response, so the new maintainer can reconstruct in-flight operators and keep the system converging.
//
// Lifecycle expectations:
//   - Create: store -> create dispatcher -> delete entry after dispatcher is created.
//   - Remove: store -> remove dispatcher -> delete entry when dispatcher is fully cleaned up.
//
// Some requests are intentionally dropped (see preCheckForSchedulerHandler / handleScheduleRemove) to avoid
// leaking operator entries in cases where we have no cleanup callback (e.g. remove a non-existent dispatcher).
func (h *SchedulerDispatcherRequestHandler) Handle(dispatcherManager *DispatcherManager, reqs ...SchedulerDispatcherRequest) bool {
	dispatcherManager.MaintainerFenceMu.Lock()
	defer dispatcherManager.MaintainerFenceMu.Unlock()

	// `dynstream` guarantees per-path serialization: for a given changefeed (Path),
	// SchedulerDispatcherRequestHandler.Handle will not be executed concurrently. This matters for reasoning:
	// a Remove cannot "interrupt" an in-progress Create within the same dispatcher manager; it will only be
	// processed after the current Handle invocation returns.
	//
	// We still iterate through reqs because Create requests are batchable, and keeping the loop makes the
	// handler robust even if batching rules change later.
	infos := map[common.DispatcherID]dispatcherCreateInfo{}
	redoInfos := map[common.DispatcherID]dispatcherCreateInfo{}
	for _, req := range reqs {
		dispatcherID, ok := preCheckForSchedulerHandler(req, dispatcherManager)
		if !ok {
			continue
		}
		switch req.ScheduleAction {
		case heartbeatpb.ScheduleAction_Create:
			// Store the add operator and create an info for later create dispatcher.
			handleScheduleCreate(dispatcherManager, req, dispatcherID, infos, redoInfos)
		case heartbeatpb.ScheduleAction_Remove:
			// Remove is non-batchable (see GetType), so reqs should contain exactly one request.
			if len(reqs) != 1 {
				log.Error("invalid remove dispatcher request count in one batch", zap.Int("count", len(reqs)))
			}
			// Store the remove operator (when applicable) and remove the dispatcher directly.
			// The remove operator will be deleted after the dispatcher is removed from dispatcherMap.
			handleScheduleRemove(dispatcherManager, req, dispatcherID)
		default:
			log.Panic("unknown schedule action", zap.Int("action", int(req.ScheduleAction)))
		}
	}

	// Use the infos to create dispatchers, and delete the current operators after created (mark the operator finished).
	if len(infos) > 0 || len(redoInfos) > 0 {
		// Used by unit tests to force an interleaving where the operator is stored but dispatcher creation is blocked.
		failpoint.Inject("BlockCreateDispatcher", nil)
	}
	createDispatcherByInfo(dispatcherManager, infos, redoInfos)
	return false
}

// preCheckForSchedulerHandler validates a scheduling request and decides whether it should be applied.
//
// It returns the dispatcherID used as currentOperatorMap key. The precheck filters out:
//   - invalid requests (nil request/config/dispatcherID),
//   - redo requests when redo is disabled,
//   - stale maintainer requests and duplicate Create requests,
//   - Create requests for an already-existing dispatcher (idempotent no-op).
//
// Note: Remove requests are allowed to proceed even if the dispatcher doesn't exist (we still want to emit a
// terminal status to the maintainer), but we must be careful not to store such requests (see handleScheduleRemove),
// otherwise the operator entry would never be cleaned up.
func preCheckForSchedulerHandler(req SchedulerDispatcherRequest, dispatcherManager *DispatcherManager) (common.DispatcherID, bool) {
	if req.ScheduleDispatcherRequest == nil {
		log.Warn("scheduleDispatcherRequest is nil, skip")
		return common.DispatcherID{}, false
	}
	if req.Config == nil {
		log.Warn("scheduleDispatcherRequest config is nil, skip")
		return common.DispatcherID{}, false
	}
	dispatcherID := common.NewDispatcherIDFromPB(req.Config.DispatcherID)
	if dispatcherID.IsZero() {
		log.Warn("scheduleDispatcherRequest has no valid operator key, skip")
		return common.DispatcherID{}, false
	}
	if !dispatcherManager.IsMaintainerRequestAllowed(req.From, req.MaintainerEpoch) {
		log.Warn("drop stale schedule dispatcher request",
			zap.String("changefeedID", req.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.String("from", req.From.String()),
			zap.Uint64("requestMaintainerEpoch", req.MaintainerEpoch),
			zap.Uint64("currentMaintainerEpoch", dispatcherManager.GetMaintainerEpoch()),
			zap.String("currentMaintainer", dispatcherManager.GetMaintainerID().String()))
		return common.DispatcherID{}, false
	}
	isRedo := common.IsRedoMode(req.Config.Mode)
	if isRedo && !dispatcherManager.IsRedoReady() {
		return common.DispatcherID{}, false
	}
	if existing, operatorExists := dispatcherManager.currentOperatorMap.Load(dispatcherID); operatorExists {
		existingReq := existing.(SchedulerDispatcherRequest)
		if !dispatcherManager.IsMaintainerRequestAllowed(existingReq.From, existingReq.MaintainerEpoch) {
			dispatcherManager.currentOperatorMap.Delete(dispatcherID)
		} else {
			// Create requests must be serialized per dispatcherID; otherwise we can end up creating multiple
			// dispatchers for the same span/dispatcherID.
			if req.ScheduleAction == heartbeatpb.ScheduleAction_Create {
				return common.DispatcherID{}, false
			}
			// Remove requests are allowed to proceed: removeDispatcher is idempotent and the incoming request
			// may carry a newer OperatorType for maintainer bootstrap/failover reconstruction.
		}
	}

	// Check whether the dispatcher exists locally. This is used to treat Create as idempotent.
	var dispatcherExists bool
	if isRedo {
		_, dispatcherExists = dispatcherManager.redoDispatcherMap.Get(dispatcherID)
	} else {
		_, dispatcherExists = dispatcherManager.dispatcherMap.Get(dispatcherID)
	}

	// Action-aware precheck:
	// - Create: allow only if dispatcher does not exist (otherwise it's already created).
	// - Remove: allow even if dispatcher does not exist, removeDispatcher will emit a Stopped status.
	switch req.ScheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		if dispatcherExists {
			return common.DispatcherID{}, false
		}
	case heartbeatpb.ScheduleAction_Remove:
	}

	return dispatcherID, true
}

func handleScheduleCreate(
	dispatcherManager *DispatcherManager,
	req SchedulerDispatcherRequest,
	dispatcherID common.DispatcherID,
	infos map[common.DispatcherID]dispatcherCreateInfo,
	redoInfos map[common.DispatcherID]dispatcherCreateInfo,
) {
	config := req.Config
	info := dispatcherCreateInfo{
		ID:               dispatcherID,
		TableSpan:        config.Span,
		StartTs:          config.StartTs,
		SchemaID:         config.SchemaID,
		SkipDMLAsStartTs: config.SkipDMLAsStartTs,
	}
	if common.IsRedoMode(config.Mode) {
		dispatcherManager.currentOperatorMap.Store(dispatcherID, req)
		log.Debug("store current working add operator for redo dispatcher",
			zap.String("changefeedID", req.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.Any("operator", req),
		)
		redoInfos[dispatcherID] = info
	} else {
		dispatcherManager.currentOperatorMap.Store(dispatcherID, req)
		log.Debug("store current working add operator",
			zap.String("changefeedID", req.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.Any("operator", req),
		)
		infos[dispatcherID] = info
	}
}

func handleScheduleRemove(
	dispatcherManager *DispatcherManager,
	req SchedulerDispatcherRequest,
	dispatcherID common.DispatcherID,
) {
	config := req.Config
	if common.IsRedoMode(config.Mode) {
		// If redo is disabled or the dispatcher does not exist, do not store the remove operator.
		// Otherwise, the operator may never be cleaned up because cleanRedoDispatcher won't be called.
		if dispatcherManager.redoDispatcherMap == nil {
			return
		}
		if _, exists := dispatcherManager.redoDispatcherMap.Get(dispatcherID); exists {
			dispatcherManager.currentOperatorMap.Store(dispatcherID, req)
			log.Debug("store current working remove operator for redo dispatcher",
				zap.String("changefeedID", req.ChangefeedID.String()),
				zap.String("dispatcherID", dispatcherID.String()),
				zap.Any("operator", req),
			)
		} else {
			log.Debug("redo dispatcher not found, skip remove operator store",
				zap.String("changefeedID", req.ChangefeedID.String()),
				zap.String("dispatcherID", dispatcherID.String()),
				zap.Any("operator", req),
			)
		}
		// Even if the dispatcher doesn't exist, we still call removeDispatcher so the dispatcher manager
		// can emit a terminal status back to the maintainer and help it converge.
		removeDispatcher(dispatcherManager, dispatcherID, dispatcherManager.redoDispatcherMap, dispatcherManager.redoSink.SinkType())
	} else {
		// If the dispatcher does not exist, do not store the remove operator.
		// Otherwise, the operator may never be cleaned up because cleanEventDispatcher won't be called.
		if _, exists := dispatcherManager.dispatcherMap.Get(dispatcherID); exists {
			dispatcherManager.currentOperatorMap.Store(dispatcherID, req)
			log.Debug("store current working remove operator",
				zap.String("changefeedID", req.ChangefeedID.String()),
				zap.String("dispatcherID", dispatcherID.String()),
				zap.Any("operator", req),
			)
		} else {
			log.Debug("dispatcher not found, skip remove operator store",
				zap.String("changefeedID", req.ChangefeedID.String()),
				zap.String("dispatcherID", dispatcherID.String()),
				zap.Any("operator", req),
			)
		}
		// Even if the dispatcher doesn't exist, we still call removeDispatcher so the dispatcher manager
		// can emit a terminal status back to the maintainer and help it converge.
		removeDispatcher(dispatcherManager, dispatcherID, dispatcherManager.dispatcherMap, dispatcherManager.sink.SinkType())
	}
}

func createDispatcherByInfo(
	dispatcherManager *DispatcherManager,
	infos map[common.DispatcherID]dispatcherCreateInfo,
	redoInfos map[common.DispatcherID]dispatcherCreateInfo,
) {
	if len(redoInfos) > 0 {
		err := dispatcherManager.newRedoDispatchers(redoInfos, false)
		if err != nil {
			if IsWritePathClosedError(err) {
				log.Info("dispatcher manager write path closed, keep add operators for redo dispatchers",
					zap.String("changefeedID", dispatcherManager.changefeedID.String()),
					zap.Int("count", len(redoInfos)),
					zap.Error(err),
				)
				return
			}
			dispatcherManager.handleError(context.Background(), err)
		}
		deleteCreatedOperators(dispatcherManager, redoInfos, dispatcherManager.redoDispatcherMap, "redo dispatcher")
	}
	if len(infos) > 0 {
		err := dispatcherManager.newEventDispatchers(infos, false)
		if err != nil {
			if IsWritePathClosedError(err) {
				log.Info("dispatcher manager write path closed, keep add operators",
					zap.String("changefeedID", dispatcherManager.changefeedID.String()),
					zap.Int("count", len(infos)),
					zap.Error(err),
				)
				return
			}
			dispatcherManager.handleError(context.Background(), err)
		}
		deleteCreatedOperators(dispatcherManager, infos, dispatcherManager.dispatcherMap, "dispatcher")
	}
}

func deleteCreatedOperators[T dispatcher.Dispatcher](
	dispatcherManager *DispatcherManager,
	infos map[common.DispatcherID]dispatcherCreateInfo,
	dispatcherMap *DispatcherMap[T],
	dispatcherKind string,
) {
	for _, info := range infos {
		if _, exists := dispatcherMap.Get(info.ID); !exists {
			continue
		}
		// Create requests are stored in currentOperatorMap before creation and
		// should be deleted only after the dispatcher is actually created.
		if v, ok := dispatcherManager.currentOperatorMap.Load(info.ID); ok {
			req := v.(SchedulerDispatcherRequest)
			if req.ScheduleAction == heartbeatpb.ScheduleAction_Create {
				log.Debug("delete current working add operator",
					zap.String("changefeedID", dispatcherManager.changefeedID.String()),
					zap.String("dispatcherID", info.ID.String()),
					zap.String("dispatcherKind", dispatcherKind),
					zap.Any("operator", req),
				)
				dispatcherManager.currentOperatorMap.Delete(info.ID)
			}
		}
	}
}

func (h *SchedulerDispatcherRequestHandler) GetSize(_ SchedulerDispatcherRequest) int { return 0 }
func (h *SchedulerDispatcherRequestHandler) IsPaused(_ SchedulerDispatcherRequest) bool {
	return false
}

func (h *SchedulerDispatcherRequestHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *SchedulerDispatcherRequestHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *SchedulerDispatcherRequestHandler) GetTimestamp(_ SchedulerDispatcherRequest) dynstream.Timestamp {
	return 0
}

func (h *SchedulerDispatcherRequestHandler) GetType(event SchedulerDispatcherRequest) dynstream.EventType {
	// we do batch for create dispatcher now.
	switch event.ScheduleAction {
	case heartbeatpb.ScheduleAction_Create:
		return dynstream.EventType{DataGroup: 1, Property: dynstream.BatchableData}
	case heartbeatpb.ScheduleAction_Remove:
		return dynstream.EventType{DataGroup: 2, Property: dynstream.NonBatchable}
	default:
		log.Panic("unknown schedule action", zap.Int("action", int(event.ScheduleAction)))
	}
	return dynstream.DefaultEventType
}

func (h *SchedulerDispatcherRequestHandler) OnDrop(event SchedulerDispatcherRequest) interface{} {
	return nil
}

func newHeartBeatResponseDynamicStream(dds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]) dynstream.DynamicStream[int, common.GID, HeartBeatResponse, *DispatcherManager, *HeartBeatResponseHandler] {
	ds := dynstream.NewParallelDynamicStream("heartbeat-response",
		newHeartBeatResponseHandler(dds))
	ds.Start()
	return ds
}

type HeartBeatResponse struct {
	From node.ID
	*heartbeatpb.HeartBeatResponse
}

// NewHeartBeatResponse carries the sender node with the heartbeat so stale
// maintainer responses cannot update dispatcher state.
func NewHeartBeatResponse(from node.ID, resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{From: from, HeartBeatResponse: resp}
}

type HeartBeatResponseHandler struct {
	dispatcherStatusDynamicStream dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]
}

func newHeartBeatResponseHandler(dds dynstream.DynamicStream[common.GID, common.DispatcherID, dispatcher.DispatcherStatusWithID, dispatcher.Dispatcher, *dispatcher.DispatcherStatusHandler]) *HeartBeatResponseHandler {
	return &HeartBeatResponseHandler{dispatcherStatusDynamicStream: dds}
}

func (h *HeartBeatResponseHandler) Path(HeartbeatResponse HeartBeatResponse) common.GID {
	return common.NewChangefeedGIDFromPB(HeartbeatResponse.ChangefeedID)
}

func (h *HeartBeatResponseHandler) Handle(dispatcherManager *DispatcherManager, resps ...HeartBeatResponse) bool {
	if len(resps) != 1 {
		// TODO: Support batch
		panic("invalid response count")
	}
	heartbeatResponse := resps[0]
	dispatcherManager.MaintainerFenceMu.Lock()
	defer dispatcherManager.MaintainerFenceMu.Unlock()
	if !isHeartBeatResponseAllowed(dispatcherManager, heartbeatResponse) {
		return false
	}
	dispatcherStatuses := heartbeatResponse.GetDispatcherStatuses()
	for _, dispatcherStatus := range dispatcherStatuses {
		influencedDispatchersType := dispatcherStatus.InfluencedDispatchers.InfluenceType
		switch influencedDispatchersType {
		case heartbeatpb.InfluenceType_Normal:
			for _, dispatcherID := range dispatcherStatus.InfluencedDispatchers.DispatcherIDs {
				dispId := common.NewDispatcherIDFromPB(dispatcherID)
				h.dispatcherStatusDynamicStream.Push(
					dispId,
					dispatcher.NewDispatcherStatusWithID(dispatcherStatus, dispId))
			}
		case heartbeatpb.InfluenceType_DB:
			schemaID := dispatcherStatus.InfluencedDispatchers.SchemaID
			var dispatcherIds []common.DispatcherID
			if common.IsRedoMode(heartbeatResponse.Mode) {
				dispatcherIds = dispatcherManager.GetAllRedoDispatchers(schemaID)
			} else {
				dispatcherIds = dispatcherManager.GetAllDispatchers(schemaID)
			}
			for _, id := range dispatcherIds {
				h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
			}
		case heartbeatpb.InfluenceType_All:
			if common.IsRedoMode(heartbeatResponse.Mode) {
				dispatcherManager.GetRedoDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.RedoDispatcher) {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				})
			} else {
				dispatcherManager.GetDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.EventDispatcher) {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				})
			}
		}
	}
	return false
}

// isHeartBeatResponseAllowed drops dispatcher heartbeats from stale maintainers
// before they can update table state or complete scheduler operators.
func isHeartBeatResponseAllowed(dispatcherManager *DispatcherManager, heartbeatResponse HeartBeatResponse) bool {
	return isMaintainerControlMessageAllowed(
		dispatcherManager,
		"drop stale heartbeat response",
		"responseMaintainerEpoch",
		heartbeatResponse.ChangefeedID,
		heartbeatResponse.From,
		heartbeatResponse.MaintainerEpoch,
	)
}

func (h *HeartBeatResponseHandler) GetSize(event HeartBeatResponse) int   { return 0 }
func (h *HeartBeatResponseHandler) IsPaused(event HeartBeatResponse) bool { return false }
func (h *HeartBeatResponseHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *HeartBeatResponseHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *HeartBeatResponseHandler) GetTimestamp(event HeartBeatResponse) dynstream.Timestamp {
	return 0
}

func (h *HeartBeatResponseHandler) GetType(event HeartBeatResponse) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *HeartBeatResponseHandler) OnDrop(event HeartBeatResponse) interface{} {
	return nil
}

// checkpointTsMessageDynamicStream is responsible for push checkpointTsMessage to the corresponding table trigger event dispatcher.
func newCheckpointTsMessageDynamicStream() dynstream.DynamicStream[int, common.GID, CheckpointTsMessage, *DispatcherManager, *CheckpointTsMessageHandler] {
	ds := dynstream.NewParallelDynamicStream("checkpoint-ts",
		&CheckpointTsMessageHandler{})
	ds.Start()
	return ds
}

type CheckpointTsMessage struct {
	*heartbeatpb.CheckpointTsMessage
}

func NewCheckpointTsMessage(msg *heartbeatpb.CheckpointTsMessage) CheckpointTsMessage {
	return CheckpointTsMessage{msg}
}

type CheckpointTsMessageHandler struct{}

func (h *CheckpointTsMessageHandler) Path(checkpointTsMessage CheckpointTsMessage) common.GID {
	return common.NewChangefeedGIDFromPB(checkpointTsMessage.ChangefeedID)
}

func (h *CheckpointTsMessageHandler) Handle(dispatcherManager *DispatcherManager, messages ...CheckpointTsMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	if dispatcherManager.GetTableTriggerEventDispatcher() != nil {
		checkpointTsMessage := messages[0]
		dispatcherManager.addCheckpointTs(checkpointTsMessage.CheckpointTs)
	}
	return false
}

func (h *CheckpointTsMessageHandler) GetSize(event CheckpointTsMessage) int   { return 0 }
func (h *CheckpointTsMessageHandler) IsPaused(event CheckpointTsMessage) bool { return false }
func (h *CheckpointTsMessageHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *CheckpointTsMessageHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *CheckpointTsMessageHandler) GetTimestamp(event CheckpointTsMessage) dynstream.Timestamp {
	return 0
}

func (h *CheckpointTsMessageHandler) GetType(event CheckpointTsMessage) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *CheckpointTsMessageHandler) OnDrop(event CheckpointTsMessage) interface{} {
	return nil
}

// RedoResolvedTsForwardMessageDynamicStream is responsible for push RedoResolvedTsForwardMessage to the corresponding table trigger event dispatcher.
func newRedoResolvedTsForwardMessageDynamicStream() dynstream.DynamicStream[int, common.GID, RedoResolvedTsForwardMessage, *DispatcherManager, *RedoResolvedTsForwardMessageHandler] {
	ds := dynstream.NewParallelDynamicStream("redo-resolved-ts",
		&RedoResolvedTsForwardMessageHandler{})
	ds.Start()
	return ds
}

type RedoResolvedTsForwardMessage struct {
	From node.ID
	*heartbeatpb.RedoResolvedTsForwardMessage
}

// NewRedoResolvedTsForwardMessage carries the sender node with redo
// resolved-ts updates so stale maintainers cannot unblock redo dispatchers.
func NewRedoResolvedTsForwardMessage(from node.ID, msg *heartbeatpb.RedoResolvedTsForwardMessage) RedoResolvedTsForwardMessage {
	return RedoResolvedTsForwardMessage{From: from, RedoResolvedTsForwardMessage: msg}
}

type RedoResolvedTsForwardMessageHandler struct{}

func NewRedoResolvedTsForwardMessageHandler() RedoResolvedTsForwardMessageHandler {
	return RedoResolvedTsForwardMessageHandler{}
}

func (h *RedoResolvedTsForwardMessageHandler) Path(RedoResolvedTsForwardMessage RedoResolvedTsForwardMessage) common.GID {
	return common.NewChangefeedGIDFromPB(RedoResolvedTsForwardMessage.ChangefeedID)
}

func (h *RedoResolvedTsForwardMessageHandler) Handle(dispatcherManager *DispatcherManager, messages ...RedoResolvedTsForwardMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	msg := messages[0]
	dispatcherManager.MaintainerFenceMu.Lock()
	if !isRedoResolvedTsForwardMessageAllowed(dispatcherManager, msg) {
		dispatcherManager.MaintainerFenceMu.Unlock()
		return false
	}
	ok := dispatcherManager.SetRedoResolvedTs(msg.ResolvedTs)
	var dispatchers []*dispatcher.EventDispatcher
	if ok {
		dispatcherManager.dispatcherMap.ForEach(func(_ common.DispatcherID, dispatcher *dispatcher.EventDispatcher) {
			dispatchers = append(dispatchers, dispatcher)
		})
	}
	dispatcherManager.MaintainerFenceMu.Unlock()
	for _, dispatcher := range dispatchers {
		dispatcher.HandleCacheEvents()
	}
	return false
}

// isRedoResolvedTsForwardMessageAllowed drops redo resolved-ts updates from
// stale maintainers before they can unblock cached events.
func isRedoResolvedTsForwardMessageAllowed(dispatcherManager *DispatcherManager, msg RedoResolvedTsForwardMessage) bool {
	return isMaintainerControlMessageAllowed(
		dispatcherManager,
		"drop stale redo resolved ts forward message",
		"requestMaintainerEpoch",
		msg.ChangefeedID,
		msg.From,
		msg.MaintainerEpoch,
	)
}

func (h *RedoResolvedTsForwardMessageHandler) GetSize(event RedoResolvedTsForwardMessage) int {
	return 0
}

func (h *RedoResolvedTsForwardMessageHandler) IsPaused(event RedoResolvedTsForwardMessage) bool {
	return false
}

func (h *RedoResolvedTsForwardMessageHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *RedoResolvedTsForwardMessageHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *RedoResolvedTsForwardMessageHandler) GetTimestamp(event RedoResolvedTsForwardMessage) dynstream.Timestamp {
	return 0
}

func (h *RedoResolvedTsForwardMessageHandler) GetType(event RedoResolvedTsForwardMessage) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *RedoResolvedTsForwardMessageHandler) OnDrop(event RedoResolvedTsForwardMessage) interface{} {
	return nil
}

// newRedoMetaMessageDynamicStream is responsible for push RedoMetaMessage to the corresponding table trigger dispatcher.
func newRedoMetaMessageDynamicStream() dynstream.DynamicStream[int, common.GID, RedoMetaMessage, *DispatcherManager, *RedoMetaMessageHandler] {
	ds := dynstream.NewParallelDynamicStream("redo-meta",
		&RedoMetaMessageHandler{})
	ds.Start()
	return ds
}

type RedoMetaMessage struct {
	From node.ID
	*heartbeatpb.RedoMetaMessage
}

// NewRedoMetaMessage carries the sender node with redo meta updates so redo
// metadata cannot be advanced by stale maintainers after ownership changes.
func NewRedoMetaMessage(from node.ID, msg *heartbeatpb.RedoMetaMessage) RedoMetaMessage {
	return RedoMetaMessage{From: from, RedoMetaMessage: msg}
}

type RedoMetaMessageHandler struct{}

func NewRedoMetaMessageHandler() RedoMetaMessageHandler {
	return RedoMetaMessageHandler{}
}

func (h *RedoMetaMessageHandler) Path(redoMetaMessage RedoMetaMessage) common.GID {
	return common.NewChangefeedGIDFromPB(redoMetaMessage.ChangefeedID)
}

func (h *RedoMetaMessageHandler) Handle(dispatcherManager *DispatcherManager, messages ...RedoMetaMessage) bool {
	if len(messages) != 1 {
		// TODO: Support batch
		panic("invalid message count")
	}
	msg := messages[0]
	dispatcherManager.MaintainerFenceMu.Lock()
	defer dispatcherManager.MaintainerFenceMu.Unlock()
	if !isRedoMetaMessageAllowed(dispatcherManager, msg) {
		return false
	}
	if dispatcherManager.GetTableTriggerRedoDispatcher() != nil {
		dispatcherManager.UpdateRedoMeta(msg.CheckpointTs, msg.ResolvedTs)
	}
	return false
}

// isRedoMetaMessageAllowed drops redo meta updates from stale maintainers
// before they can advance redo recovery boundaries.
func isRedoMetaMessageAllowed(dispatcherManager *DispatcherManager, msg RedoMetaMessage) bool {
	return isMaintainerControlMessageAllowed(
		dispatcherManager,
		"drop stale redo meta message",
		"requestMaintainerEpoch",
		msg.ChangefeedID,
		msg.From,
		msg.MaintainerEpoch,
	)
}

func isMaintainerControlMessageAllowed(
	dispatcherManager *DispatcherManager,
	logMessage string,
	epochField string,
	changefeedID *heartbeatpb.ChangefeedID,
	from node.ID,
	maintainerEpoch uint64,
) bool {
	if dispatcherManager.IsMaintainerRequestAllowed(from, maintainerEpoch) {
		return true
	}
	log.Warn(logMessage,
		zap.String("changefeedID", changefeedID.String()),
		zap.String("from", from.String()),
		zap.Uint64(epochField, maintainerEpoch),
		zap.Uint64("currentMaintainerEpoch", dispatcherManager.GetMaintainerEpoch()),
		zap.String("currentMaintainer", dispatcherManager.GetMaintainerID().String()))
	return false
}

func (h *RedoMetaMessageHandler) GetSize(event RedoMetaMessage) int   { return 0 }
func (h *RedoMetaMessageHandler) IsPaused(event RedoMetaMessage) bool { return false }
func (h *RedoMetaMessageHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *RedoMetaMessageHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *RedoMetaMessageHandler) GetTimestamp(event RedoMetaMessage) dynstream.Timestamp {
	return 0
}

func (h *RedoMetaMessageHandler) GetType(event RedoMetaMessage) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *RedoMetaMessageHandler) OnDrop(event RedoMetaMessage) interface{} {
	return nil
}

func newMergeDispatcherRequestDynamicStream() dynstream.DynamicStream[int, common.GID, MergeDispatcherRequest, *DispatcherManager, *MergeDispatcherRequestHandler] {
	ds := dynstream.NewParallelDynamicStream("merge-dispatcher-request",
		&MergeDispatcherRequestHandler{})
	ds.Start()
	return ds
}

type MergeDispatcherRequest struct {
	From node.ID
	*heartbeatpb.MergeDispatcherRequest
}

// NewMergeDispatcherRequest carries the sender node together with the request
// so the handler can apply the dispatcher-manager maintainer fence.
func NewMergeDispatcherRequest(from node.ID, req *heartbeatpb.MergeDispatcherRequest) MergeDispatcherRequest {
	return MergeDispatcherRequest{From: from, MergeDispatcherRequest: req}
}

type MergeDispatcherRequestHandler struct{}

func (h *MergeDispatcherRequestHandler) Path(mergeDispatcherRequest MergeDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(mergeDispatcherRequest.ChangefeedID)
}

func (h *MergeDispatcherRequestHandler) Handle(dispatcherManager *DispatcherManager, reqs ...MergeDispatcherRequest) bool {
	if len(reqs) != 1 {
		panic("invalid request count")
	}

	mergeDispatcherRequest := reqs[0]
	dispatcherManager.MaintainerFenceMu.Lock()
	defer dispatcherManager.MaintainerFenceMu.Unlock()
	if !isMaintainerControlMessageAllowed(
		dispatcherManager,
		"drop stale merge dispatcher request",
		"requestMaintainerEpoch",
		mergeDispatcherRequest.ChangefeedID,
		mergeDispatcherRequest.From,
		mergeDispatcherRequest.MaintainerEpoch,
	) {
		return false
	}
	dispatcherIDs := make([]common.DispatcherID, 0, len(mergeDispatcherRequest.DispatcherIDs))
	for _, id := range mergeDispatcherRequest.DispatcherIDs {
		dispatcherIDs = append(dispatcherIDs, common.NewDispatcherIDFromPB(id))
	}
	dispatcherManager.MergeDispatcher(dispatcherIDs, common.NewDispatcherIDFromPB(mergeDispatcherRequest.MergedDispatcherID), mergeDispatcherRequest.Mode)
	return false
}

func (h *MergeDispatcherRequestHandler) GetSize(event MergeDispatcherRequest) int   { return 0 }
func (h *MergeDispatcherRequestHandler) IsPaused(event MergeDispatcherRequest) bool { return false }
func (h *MergeDispatcherRequestHandler) GetArea(_ common.GID, _ *DispatcherManager) int {
	return 0
}

func (h *MergeDispatcherRequestHandler) GetMetricLabel(dest *DispatcherManager) string {
	return dest.changefeedID.String()
}

func (h *MergeDispatcherRequestHandler) GetTimestamp(event MergeDispatcherRequest) dynstream.Timestamp {
	return 0
}

func (h *MergeDispatcherRequestHandler) GetType(event MergeDispatcherRequest) dynstream.EventType {
	return dynstream.DefaultEventType
}

func (h *MergeDispatcherRequestHandler) OnDrop(event MergeDispatcherRequest) interface{} {
	return nil
}
