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
	"sync"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/node"
)

// HeartbeatRequestQueue is a channel for all event dispatcher managers to send heartbeat requests to HeartBeatCollector
type HeartBeatRequestWithTargetID struct {
	TargetID node.ID
	Request  *heartbeatpb.HeartBeatRequest
}

type HeartbeatRequestQueue struct {
	queue chan *HeartBeatRequestWithTargetID
}

func NewHeartbeatRequestQueue() *HeartbeatRequestQueue {
	return &HeartbeatRequestQueue{
		queue: make(chan *HeartBeatRequestWithTargetID, 100000),
	}
}

func (q *HeartbeatRequestQueue) Enqueue(request *HeartBeatRequestWithTargetID) {
	q.queue <- request
}

func (q *HeartbeatRequestQueue) Dequeue(ctx context.Context) *HeartBeatRequestWithTargetID {
	select {
	case <-ctx.Done():
		return nil
	case request := <-q.queue:
		return request
	}
}

func (q *HeartbeatRequestQueue) Close() {
	close(q.queue)
}

type BlockStatusRequestWithTargetID struct {
	TargetID node.ID
	Request  *heartbeatpb.BlockStatusRequest
}

// BlockStatusRequestQueue is a channel for all event dispatcher managers to send block status requests to HeartBeatCollector
type BlockStatusRequestQueue struct {
	queue chan *BlockStatusRequestWithTargetID

	mu                sync.Mutex
	requestStatusKeys map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey
	queuedStatuses    map[blockStatusRequestDedupeKey]struct{}
	inFlightStatuses  map[blockStatusRequestDedupeKey]struct{}
}

func NewBlockStatusRequestQueue() *BlockStatusRequestQueue {
	return &BlockStatusRequestQueue{
		queue:             make(chan *BlockStatusRequestWithTargetID, 10000),
		requestStatusKeys: make(map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey),
		queuedStatuses:    make(map[blockStatusRequestDedupeKey]struct{}),
		inFlightStatuses:  make(map[blockStatusRequestDedupeKey]struct{}),
	}
}

func (q *BlockStatusRequestQueue) Enqueue(request *BlockStatusRequestWithTargetID) {
	if request == nil || request.Request == nil {
		return
	}
	if !q.trackPendingStatuses(request) {
		metrics.HeartbeatCollectorBlockStatusRequestQueueLenGauge.Set(float64(len(q.queue)))
		return
	}
	q.queue <- request
	metrics.HeartbeatCollectorBlockStatusRequestQueueLenGauge.Set(float64(len(q.queue)))
}

func (q *BlockStatusRequestQueue) Dequeue(ctx context.Context) *BlockStatusRequestWithTargetID {
	select {
	case <-ctx.Done():
		return nil
	case request := <-q.queue:
		q.markInFlight(request)
		metrics.HeartbeatCollectorBlockStatusRequestQueueLenGauge.Set(float64(len(q.queue)))
		return request
	}
}

func (q *BlockStatusRequestQueue) OnSendComplete(request *BlockStatusRequestWithTargetID) {
	if request == nil {
		return
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range q.requestStatusKeys[request] {
		delete(q.inFlightStatuses, key)
	}
	delete(q.requestStatusKeys, request)
}

func (q *BlockStatusRequestQueue) Close() {
	close(q.queue)
}

type blockStatusRequestDedupeKey struct {
	targetID     node.ID
	dispatcherID common.DispatcherID
	blockTs      uint64
	mode         int64
	isSyncPoint  bool
	stage        heartbeatpb.BlockStage
}

func (q *BlockStatusRequestQueue) trackPendingStatuses(request *BlockStatusRequestWithTargetID) bool {
	statuses := request.Request.BlockStatuses
	filtered := statuses[:0]
	statusKeys := make([]blockStatusRequestDedupeKey, 0)

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, status := range statuses {
		if !shouldDeduplicateRequestStatus(status) {
			filtered = append(filtered, status)
			continue
		}

		key := blockStatusRequestDedupeKey{
			targetID:     request.TargetID,
			dispatcherID: common.NewDispatcherIDFromPB(status.ID),
			blockTs:      status.State.BlockTs,
			mode:         status.Mode,
			isSyncPoint:  status.State.IsSyncPoint,
			stage:        status.State.Stage,
		}
		if _, ok := q.queuedStatuses[key]; ok {
			continue
		}
		if _, ok := q.inFlightStatuses[key]; ok {
			continue
		}

		filtered = append(filtered, status)
		q.queuedStatuses[key] = struct{}{}
		statusKeys = append(statusKeys, key)
	}

	request.Request.BlockStatuses = filtered
	if len(statusKeys) > 0 {
		q.requestStatusKeys[request] = statusKeys
	}
	return len(filtered) > 0
}

func (q *BlockStatusRequestQueue) markInFlight(request *BlockStatusRequestWithTargetID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range q.requestStatusKeys[request] {
		delete(q.queuedStatuses, key)
		q.inFlightStatuses[key] = struct{}{}
	}
}

func isWaitingRequestStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_WAITING
}

func shouldDeduplicateRequestStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return isDoneRequestStatus(status) || isWaitingRequestStatus(status)
}

func isDoneRequestStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_DONE
}
