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
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/dynstream"
	"go.uber.org/zap"
)

type DispatcherMap[T dispatcher.Dispatcher] struct {
	m sync.Map
	// sequence number is increasing when dispatcher is added.
	//
	// Seq is used to prevent the fallback of changefeed's checkpointTs.
	// When some new dispatcher(table) is being added, the maintainer will block the forward of changefeed's checkpointTs
	// until the maintainer receive the message that the new dispatcher's component status change to working.
	//
	// Besides, there is no strict order of the heartbeat message and the table status messages, which is means
	// it can happen that when dispatcher A is created, event dispatcher manager may first send a table status message
	// to show the new dispatcher is working, and then send a heartbeat message of the current watermark,
	// which is calculated without the new disaptcher.
	// When the checkpointTs of the watermark is large than the startTs of the new dispatcher,
	// the watermark of next heartbeat, which calculated with the new dispatcher can be less than the previous watermark.
	// Then it can cause the fallback of changefeed's checkpointTs.
	// To avoid fallback, we add a seq number in each heartbeat message(both collect from collectComponentStatusWhenChanged and aggregateDispatcherHeartbeats)
	// When a table is added the seq number will be increase,
	// and when the maintainer receive the outdate seq, it will know the heartbeat message is outdate and ignore it.
	// In this way, even the above case happens, the changefeed's checkpointTs will not fallback.
	//
	// Here we don't need to make seq changes always atmoic with the m changed.
	// Our target is just :
	// The seq get from ForEach should be smaller than the seq get from Set
	// when ForEach is not access the new dispatcher just Set.
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
	ds := dynstream.NewParallelDynamicStream(
		&SchedulerDispatcherRequestHandler{}, option)
	ds.Start()
	return ds
}

type SchedulerDispatcherRequest struct {
	*heartbeatpb.ScheduleDispatcherRequest
}

func NewSchedulerDispatcherRequest(req *heartbeatpb.ScheduleDispatcherRequest) SchedulerDispatcherRequest {
	return SchedulerDispatcherRequest{req}
}

type SchedulerDispatcherRequestHandler struct{}

func (h *SchedulerDispatcherRequestHandler) Path(scheduleDispatcherRequest SchedulerDispatcherRequest) common.GID {
	return common.NewChangefeedGIDFromPB(scheduleDispatcherRequest.ChangefeedID)
}

// Handle handles the SchedulerDispatcherRequest events, which are operators from the maintainer to add or remove dispatchers.
// It will record the operators first, it's used to store the information that there is a not finished operator for a dispatcher.
// The current working operators will be deleted after the operator is finished, i.e. the dispatcher is created or removed.
func (h *SchedulerDispatcherRequestHandler) Handle(dispatcherManager *DispatcherManager, reqs ...SchedulerDispatcherRequest) bool {
	// If req is about remove dispatcher, then there will only be one request in reqs.
	infos := map[common.DispatcherID]dispatcherCreateInfo{}
	redoInfos := map[common.DispatcherID]dispatcherCreateInfo{}
	for _, req := range reqs {
		operatorKey, ok := preCheckForSchedulerHandler(req, dispatcherManager)
		if !ok {
			continue
		}
		switch req.ScheduleAction {
		case heartbeatpb.ScheduleAction_Create:
			// store the add operator and create a info for later create dispatcher
			handleScheduleCreate(dispatcherManager, req, operatorKey, infos, redoInfos)
		case heartbeatpb.ScheduleAction_Remove:
			if len(reqs) != 1 {
				log.Error("invalid remove dispatcher request count in one batch", zap.Int("count", len(reqs)))
			}
			// store the remove operator and remove the dispatcher directly
			// the remove operator will be deleted after the dispatcher is removed from dispatcherMap
			handleScheduleRemove(dispatcherManager, req, operatorKey)
		}
	}

	// use the infos to create dispatchers, and delete the current operators after created, just indicate the operator is finished
	if len(infos) > 0 || len(redoInfos) > 0 {
		failpoint.Inject("BlockCreateDispatcher", nil)
	}
	createDispatcherByInfo(dispatcherManager, infos, redoInfos)
	return false
}

func preCheckForSchedulerHandler(req SchedulerDispatcherRequest, dispatcherManager *DispatcherManager) (common.DispatcherID, bool) {
	if req.ScheduleDispatcherRequest == nil {
		log.Warn("scheduleDispatcherRequest is nil, skip")
		return common.DispatcherID{}, false
	}
	if req.Config == nil {
		log.Warn("scheduleDispatcherRequest config is nil, skip")
		return common.DispatcherID{}, false
	}
	operatorKey := common.NewDispatcherIDFromPB(req.Config.DispatcherID)
	if operatorKey.IsZero() {
		log.Warn("scheduleDispatcherRequest has no valid operator key, skip")
		return common.DispatcherID{}, false
	}

	isRedo := common.IsRedoMode(req.Config.Mode)
	if isRedo && (!dispatcherManager.RedoEnable || dispatcherManager.redoDispatcherMap == nil) {
		return common.DispatcherID{}, false
	}
	if _, operatorExists := dispatcherManager.currentOperatorMap.Load(operatorKey); operatorExists {
		return common.DispatcherID{}, false
	}

	// If there is already an operator for the dispatcher, skip this request.
	var dispatcherExists bool
	if isRedo {
		_, dispatcherExists = dispatcherManager.redoDispatcherMap.Get(operatorKey)
	} else {
		_, dispatcherExists = dispatcherManager.dispatcherMap.Get(operatorKey)
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

	return operatorKey, true
}

func handleScheduleCreate(
	dispatcherManager *DispatcherManager,
	req SchedulerDispatcherRequest,
	operatorKey common.DispatcherID,
	infos map[common.DispatcherID]dispatcherCreateInfo,
	redoInfos map[common.DispatcherID]dispatcherCreateInfo,
) {
	config := req.Config
	dispatcherID := common.NewDispatcherIDFromPB(config.DispatcherID)
	info := dispatcherCreateInfo{
		Id:               dispatcherID,
		TableSpan:        config.Span,
		StartTs:          config.StartTs,
		SchemaID:         config.SchemaID,
		SkipDMLAsStartTs: config.SkipDMLAsStartTs,
	}
	if common.IsRedoMode(config.Mode) {
		dispatcherManager.currentOperatorMap.Store(operatorKey, req)
		log.Debug("store current working add operator for redo dispatcher",
			zap.String("changefeedID", req.ChangefeedID.String()),
			zap.String("dispatcherID", dispatcherID.String()),
			zap.Any("operator", req),
		)
		redoInfos[dispatcherID] = info
	} else {
		dispatcherManager.currentOperatorMap.Store(operatorKey, req)
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
	operatorKey common.DispatcherID,
) {
	config := req.Config
	dispatcherID := common.NewDispatcherIDFromPB(config.DispatcherID)
	if common.IsRedoMode(config.Mode) {
		// If redo is disabled or the dispatcher does not exist, do not store the remove operator.
		// Otherwise, the operator may never be cleaned up because cleanRedoDispatcher won't be called.
		if dispatcherManager.redoDispatcherMap == nil {
			return
		}
		if _, exists := dispatcherManager.redoDispatcherMap.Get(dispatcherID); exists {
			dispatcherManager.currentOperatorMap.Store(operatorKey, req)
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
		removeDispatcher(dispatcherManager, dispatcherID, dispatcherManager.redoDispatcherMap, dispatcherManager.redoSink.SinkType())
	} else {
		// If the dispatcher does not exist, do not store the remove operator.
		// Otherwise, the operator may never be cleaned up because cleanEventDispatcher won't be called.
		if _, exists := dispatcherManager.dispatcherMap.Get(dispatcherID); exists {
			dispatcherManager.currentOperatorMap.Store(operatorKey, req)
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
			dispatcherManager.handleError(context.Background(), err)
		}
		for _, info := range redoInfos {
			if v, ok := dispatcherManager.currentOperatorMap.Load(info.Id); ok {
				req := v.(SchedulerDispatcherRequest)
				log.Debug("delete current working add operator for redo dispatcher",
					zap.String("changefeedID", dispatcherManager.changefeedID.String()),
					zap.String("dispatcherID", info.Id.String()),
					zap.Any("operator", req),
				)
				dispatcherManager.currentOperatorMap.Delete(info.Id)
			} else {
				log.Error("current working operator not found for redo dispatcher, should not happen", zap.String("id", info.Id.String()))
			}
		}
	}
	if len(infos) > 0 {
		err := dispatcherManager.newEventDispatchers(infos, false)
		if err != nil {
			dispatcherManager.handleError(context.Background(), err)
		}
		for _, info := range infos {
			if v, ok := dispatcherManager.currentOperatorMap.Load(info.Id); ok {
				req := v.(SchedulerDispatcherRequest)
				log.Debug("delete current working add operator",
					zap.String("changefeedID", dispatcherManager.changefeedID.String()),
					zap.String("dispatcherID", info.Id.String()),
					zap.Any("operator", req),
				)
				dispatcherManager.currentOperatorMap.Delete(info.Id)
			} else {
				log.Error("current working operator not found, should not happen", zap.String("id", info.Id.String()))
			}
		}
	}
}

func (h *SchedulerDispatcherRequestHandler) GetSize(event SchedulerDispatcherRequest) int { return 0 }
func (h *SchedulerDispatcherRequestHandler) IsPaused(event SchedulerDispatcherRequest) bool {
	return false
}

func (h *SchedulerDispatcherRequestHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
}

func (h *SchedulerDispatcherRequestHandler) GetTimestamp(event SchedulerDispatcherRequest) dynstream.Timestamp {
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
	ds := dynstream.NewParallelDynamicStream(
		newHeartBeatResponseHandler(dds))
	ds.Start()
	return ds
}

type HeartBeatResponse struct {
	*heartbeatpb.HeartBeatResponse
}

func NewHeartBeatResponse(resp *heartbeatpb.HeartBeatResponse) HeartBeatResponse {
	return HeartBeatResponse{resp}
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
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			var dispatcherIds []common.DispatcherID
			if common.IsRedoMode(heartbeatResponse.Mode) {
				dispatcherIds = dispatcherManager.GetAllRedoDispatchers(schemaID)
			} else {
				dispatcherIds = dispatcherManager.GetAllDispatchers(schemaID)
			}
			for _, id := range dispatcherIds {
				if id != excludeDispatcherID {
					h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
				}
			}
		case heartbeatpb.InfluenceType_All:
			excludeDispatcherID := common.NewDispatcherIDFromPB(dispatcherStatus.InfluencedDispatchers.ExcludeDispatcherId)
			if common.IsRedoMode(heartbeatResponse.Mode) {
				dispatcherManager.GetRedoDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.RedoDispatcher) {
					if id != excludeDispatcherID {
						h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
					}
				})
			} else {
				dispatcherManager.GetDispatcherMap().ForEach(func(id common.DispatcherID, _ *dispatcher.EventDispatcher) {
					if id != excludeDispatcherID {
						h.dispatcherStatusDynamicStream.Push(id, dispatcher.NewDispatcherStatusWithID(dispatcherStatus, id))
					}
				})
			}
		}
	}
	return false
}

func (h *HeartBeatResponseHandler) GetSize(event HeartBeatResponse) int   { return 0 }
func (h *HeartBeatResponseHandler) IsPaused(event HeartBeatResponse) bool { return false }
func (h *HeartBeatResponseHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
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
	ds := dynstream.NewParallelDynamicStream(
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
		dispatcherManager.sink.AddCheckpointTs(checkpointTsMessage.CheckpointTs)
	}
	return false
}

func (h *CheckpointTsMessageHandler) GetSize(event CheckpointTsMessage) int   { return 0 }
func (h *CheckpointTsMessageHandler) IsPaused(event CheckpointTsMessage) bool { return false }
func (h *CheckpointTsMessageHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
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
	ds := dynstream.NewParallelDynamicStream(
		&RedoResolvedTsForwardMessageHandler{})
	ds.Start()
	return ds
}

type RedoResolvedTsForwardMessage struct {
	*heartbeatpb.RedoResolvedTsForwardMessage
}

func NewRedoResolvedTsForwardMessage(msg *heartbeatpb.RedoResolvedTsForwardMessage) RedoResolvedTsForwardMessage {
	return RedoResolvedTsForwardMessage{msg}
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
	ok := dispatcherManager.SetRedoResolvedTs(msg.ResolvedTs)
	if ok {
		dispatcherManager.dispatcherMap.ForEach(func(id common.DispatcherID, dispatcher *dispatcher.EventDispatcher) {
			dispatcher.HandleCacheEvents()
		})
	}
	return false
}

func (h *RedoResolvedTsForwardMessageHandler) GetSize(event RedoResolvedTsForwardMessage) int {
	return 0
}

func (h *RedoResolvedTsForwardMessageHandler) IsPaused(event RedoResolvedTsForwardMessage) bool {
	return false
}

func (h *RedoResolvedTsForwardMessageHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
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

// newRedoMetaMessageDynamicStream is responsible for push RedoMetaMessage to the corresponding table trigger event dispatcher.
func newRedoMetaMessageDynamicStream() dynstream.DynamicStream[int, common.GID, RedoMetaMessage, *DispatcherManager, *RedoMetaMessageHandler] {
	ds := dynstream.NewParallelDynamicStream(
		&RedoMetaMessageHandler{})
	ds.Start()
	return ds
}

type RedoMetaMessage struct {
	*heartbeatpb.RedoMetaMessage
}

func NewRedoMetaMessage(msg *heartbeatpb.RedoMetaMessage) RedoMetaMessage {
	return RedoMetaMessage{msg}
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
	dispatcherManager.UpdateRedoMeta(msg.CheckpointTs, msg.ResolvedTs)
	return false
}

func (h *RedoMetaMessageHandler) GetSize(event RedoMetaMessage) int   { return 0 }
func (h *RedoMetaMessageHandler) IsPaused(event RedoMetaMessage) bool { return false }
func (h *RedoMetaMessageHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
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
	ds := dynstream.NewParallelDynamicStream(
		&MergeDispatcherRequestHandler{})
	ds.Start()
	return ds
}

type MergeDispatcherRequest struct {
	*heartbeatpb.MergeDispatcherRequest
}

func NewMergeDispatcherRequest(req *heartbeatpb.MergeDispatcherRequest) MergeDispatcherRequest {
	return MergeDispatcherRequest{req}
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
	dispatcherIDs := make([]common.DispatcherID, 0, len(mergeDispatcherRequest.DispatcherIDs))
	for _, id := range mergeDispatcherRequest.DispatcherIDs {
		dispatcherIDs = append(dispatcherIDs, common.NewDispatcherIDFromPB(id))
	}
	dispatcherManager.MergeDispatcher(dispatcherIDs, common.NewDispatcherIDFromPB(mergeDispatcherRequest.MergedDispatcherID), mergeDispatcherRequest.Mode)
	return false
}

func (h *MergeDispatcherRequestHandler) GetSize(event MergeDispatcherRequest) int   { return 0 }
func (h *MergeDispatcherRequestHandler) IsPaused(event MergeDispatcherRequest) bool { return false }
func (h *MergeDispatcherRequestHandler) GetArea(path common.GID, dest *DispatcherManager) int {
	return 0
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
