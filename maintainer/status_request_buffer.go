package maintainer

import (
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
)

type bufferedStatusRequestBuffer struct {
	sync.Mutex

	heartbeats     map[node.ID]*bufferedHeartbeatRequest
	blockStatuses  map[bufferedBlockStatusSourceKey]*bufferedBlockStatusRequest
	redoResolvedTs map[node.ID]*heartbeatpb.RedoResolvedTsProgressMessage
}

type bufferedHeartbeatRequest struct {
	changefeedID   *heartbeatpb.ChangefeedID
	watermark      *heartbeatpb.Watermark
	redoWatermark  *heartbeatpb.Watermark
	statuses       map[heartbeatStatusKey]*heartbeatpb.TableSpanStatus
	order          []heartbeatStatusKey
	err            *heartbeatpb.RunningError
	completeStatus bool
}

type heartbeatStatusKey struct {
	dispatcherID common.DispatcherID
	mode         int64
}

type bufferedBlockStatusRequest struct {
	changefeedID *heartbeatpb.ChangefeedID
	from         node.ID
	mode         int64
	statuses     map[blockStatusKey]*heartbeatpb.TableSpanBlockStatus
	order        []blockStatusKey
}

type bufferedBlockStatusSourceKey struct {
	from node.ID
	mode int64
}

type blockStatusKey struct {
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

func isBufferableStatusRequest(msgType messaging.IOType) bool {
	switch msgType {
	case messaging.TypeHeartBeatRequest,
		messaging.TypeBlockStatusRequest,
		messaging.TypeRedoResolvedTsProgressMessage:
		return true
	default:
		return false
	}
}

func (m *Maintainer) tryBufferStatusRequestEvent(event *Event) (bool, bool) {
	if event == nil || event.eventType != EventMessage || event.message == nil {
		return false, false
	}
	if !isBufferableStatusRequest(event.message.Type) {
		return false, false
	}
	if !m.initialized.Load() {
		m.recordDroppedStatusRequest(event.message.Type, "maintainer not initialized")
		return true, false
	}
	if m.removing.Load() || (m.removed != nil && m.removed.Load()) {
		m.recordDroppedStatusRequest(event.message.Type, "maintainer is closing")
		return true, false
	}

	m.bufferStatusRequest(event.message)
	select {
	case m.statusRequestNotifyCh <- struct{}{}:
	default:
	}
	return true, true
}

func (m *Maintainer) bufferStatusRequest(msg *messaging.TargetMessage) {
	switch msg.Type {
	case messaging.TypeHeartBeatRequest:
		req := msg.Message[0].(*heartbeatpb.HeartBeatRequest)
		m.bufferedStatusRequests.mergeHeartbeat(msg.From, req)
	case messaging.TypeBlockStatusRequest:
		req := msg.Message[0].(*heartbeatpb.BlockStatusRequest)
		m.bufferedStatusRequests.mergeBlockStatus(msg.From, req)
	case messaging.TypeRedoResolvedTsProgressMessage:
		req := msg.Message[0].(*heartbeatpb.RedoResolvedTsProgressMessage)
		m.bufferedStatusRequests.mergeRedoResolvedTs(msg.From, req)
	}
}

func (m *Maintainer) handleBufferedStatusRequests() {
	for _, event := range m.takeBufferedStatusRequestEvents() {
		m.HandleEvent(event)
	}
}

func (m *Maintainer) takeBufferedStatusRequestEvents() []*Event {
	return m.bufferedStatusRequests.drain(m.changefeedID)
}

func (b *bufferedStatusRequestBuffer) mergeHeartbeat(from node.ID, req *heartbeatpb.HeartBeatRequest) {
	if req == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	if b.heartbeats == nil {
		b.heartbeats = make(map[node.ID]*bufferedHeartbeatRequest)
	}
	entry, ok := b.heartbeats[from]
	if !ok {
		entry = &bufferedHeartbeatRequest{
			changefeedID: req.ChangefeedID,
			statuses:     make(map[heartbeatStatusKey]*heartbeatpb.TableSpanStatus),
		}
		b.heartbeats[from] = entry
	}
	entry.changefeedID = req.ChangefeedID
	entry.completeStatus = entry.completeStatus || req.CompeleteStatus
	entry.err = pickRunningError(entry.err, req.Err)
	entry.watermark = pickWatermark(entry.watermark, req.Watermark)
	entry.redoWatermark = pickWatermark(entry.redoWatermark, req.RedoWatermark)
	for _, status := range req.Statuses {
		if status == nil || status.ID == nil {
			continue
		}
		key := heartbeatStatusKey{
			dispatcherID: common.NewDispatcherIDFromPB(status.ID),
			mode:         status.Mode,
		}
		if _, exists := entry.statuses[key]; !exists {
			entry.order = append(entry.order, key)
		}
		entry.statuses[key] = status
	}
}

func (b *bufferedStatusRequestBuffer) mergeBlockStatus(from node.ID, req *heartbeatpb.BlockStatusRequest) {
	if req == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	if b.blockStatuses == nil {
		b.blockStatuses = make(map[bufferedBlockStatusSourceKey]*bufferedBlockStatusRequest)
	}
	sourceKey := bufferedBlockStatusSourceKey{
		from: from,
		mode: req.Mode,
	}
	entry, ok := b.blockStatuses[sourceKey]
	if !ok {
		entry = &bufferedBlockStatusRequest{
			changefeedID: req.ChangefeedID,
			from:         from,
			mode:         req.Mode,
			statuses:     make(map[blockStatusKey]*heartbeatpb.TableSpanBlockStatus),
		}
		b.blockStatuses[sourceKey] = entry
	}
	entry.changefeedID = req.ChangefeedID
	entry.mode = req.Mode
	for _, status := range req.BlockStatuses {
		if status == nil || status.ID == nil || status.State == nil {
			continue
		}
		key := blockStatusKey{
			dispatcherID: common.NewDispatcherIDFromPB(status.ID),
			blockTs:      status.State.BlockTs,
			mode:         status.Mode,
			isSyncPoint:  status.State.IsSyncPoint,
			stage:        status.State.Stage,
		}
		if _, exists := entry.statuses[key]; !exists {
			entry.order = append(entry.order, key)
		}
		entry.statuses[key] = status
	}
}

func (b *bufferedStatusRequestBuffer) mergeRedoResolvedTs(from node.ID, req *heartbeatpb.RedoResolvedTsProgressMessage) {
	if req == nil {
		return
	}
	b.Lock()
	defer b.Unlock()
	if b.redoResolvedTs == nil {
		b.redoResolvedTs = make(map[node.ID]*heartbeatpb.RedoResolvedTsProgressMessage)
	}
	current, ok := b.redoResolvedTs[from]
	if !ok || req.ResolvedTs > current.ResolvedTs {
		b.redoResolvedTs[from] = req
	}
}

func (b *bufferedStatusRequestBuffer) drain(changefeedID common.ChangeFeedID) []*Event {
	var events []*Event
	b.Lock()
	defer b.Unlock()

	for _, entry := range b.blockStatuses {
		req := &heartbeatpb.BlockStatusRequest{
			ChangefeedID:  entry.changefeedID,
			Mode:          entry.mode,
			BlockStatuses: make([]*heartbeatpb.TableSpanBlockStatus, 0, len(entry.order)),
		}
		for _, key := range entry.order {
			if status, ok := entry.statuses[key]; ok {
				req.BlockStatuses = append(req.BlockStatuses, status)
			}
		}
		if len(req.BlockStatuses) > 0 {
			events = append(events, newBufferedStatusEvent(changefeedID, entry.from, messaging.TypeBlockStatusRequest, req))
		}
	}
	b.blockStatuses = nil

	for from, entry := range b.heartbeats {
		req := &heartbeatpb.HeartBeatRequest{
			ChangefeedID:    entry.changefeedID,
			Statuses:        make([]*heartbeatpb.TableSpanStatus, 0, len(entry.order)),
			Watermark:       entry.watermark,
			RedoWatermark:   entry.redoWatermark,
			Err:             entry.err,
			CompeleteStatus: entry.completeStatus,
		}
		for _, key := range entry.order {
			if status, ok := entry.statuses[key]; ok {
				req.Statuses = append(req.Statuses, status)
			}
		}
		if len(req.Statuses) > 0 || req.Watermark != nil || req.RedoWatermark != nil || req.Err != nil {
			events = append(events, newBufferedStatusEvent(changefeedID, from, messaging.TypeHeartBeatRequest, req))
		}
	}
	b.heartbeats = nil

	for from, req := range b.redoResolvedTs {
		events = append(events, newBufferedStatusEvent(changefeedID, from, messaging.TypeRedoResolvedTsProgressMessage, req))
	}
	b.redoResolvedTs = nil

	return events
}

func newBufferedStatusEvent(
	changefeedID common.ChangeFeedID,
	from node.ID,
	msgType messaging.IOType,
	msg messaging.IOTypeT,
) *Event {
	return &Event{
		changefeedID: changefeedID,
		eventType:    EventMessage,
		message: &messaging.TargetMessage{
			From:    from,
			To:      "",
			Type:    msgType,
			Message: []messaging.IOTypeT{msg},
		},
	}
}

func pickRunningError(current, candidate *heartbeatpb.RunningError) *heartbeatpb.RunningError {
	if candidate != nil {
		return candidate
	}
	return current
}

func pickWatermark(current, candidate *heartbeatpb.Watermark) *heartbeatpb.Watermark {
	if candidate == nil {
		return current
	}
	if current == nil {
		return candidate
	}
	if candidate.Seq > current.Seq || (candidate.Seq == current.Seq && candidate.CheckpointTs >= current.CheckpointTs) {
		current = candidate
	}
	if candidate.LastSyncedTs > current.LastSyncedTs {
		current.LastSyncedTs = candidate.LastSyncedTs
	}
	return current
}
