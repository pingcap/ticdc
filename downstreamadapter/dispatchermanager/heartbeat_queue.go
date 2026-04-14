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

	mu              sync.Mutex
	requestDoneKeys map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey
	queuedDone      map[blockStatusRequestDedupeKey]struct{}
	inFlightDone    map[blockStatusRequestDedupeKey]struct{}
}

func NewBlockStatusRequestQueue() *BlockStatusRequestQueue {
	return &BlockStatusRequestQueue{
		queue:           make(chan *BlockStatusRequestWithTargetID, 10000),
		requestDoneKeys: make(map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey),
		queuedDone:      make(map[blockStatusRequestDedupeKey]struct{}),
		inFlightDone:    make(map[blockStatusRequestDedupeKey]struct{}),
	}
}

func (q *BlockStatusRequestQueue) Enqueue(request *BlockStatusRequestWithTargetID) {
	if request == nil || request.Request == nil {
		return
	}
	if !q.trackPendingDone(request) {
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
	for _, key := range q.requestDoneKeys[request] {
		delete(q.inFlightDone, key)
	}
	delete(q.requestDoneKeys, request)
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
}

func (q *BlockStatusRequestQueue) trackPendingDone(request *BlockStatusRequestWithTargetID) bool {
	statuses := request.Request.BlockStatuses
	filtered := statuses[:0]
	doneKeys := make([]blockStatusRequestDedupeKey, 0)

	q.mu.Lock()
	defer q.mu.Unlock()

	for _, status := range statuses {
		if !isDoneRequestStatus(status) {
			filtered = append(filtered, status)
			continue
		}

		key := blockStatusRequestDedupeKey{
			targetID:     request.TargetID,
			dispatcherID: common.NewDispatcherIDFromPB(status.ID),
			blockTs:      status.State.BlockTs,
			mode:         status.Mode,
			isSyncPoint:  status.State.IsSyncPoint,
		}
		if _, ok := q.queuedDone[key]; ok {
			continue
		}
		if _, ok := q.inFlightDone[key]; ok {
			continue
		}

		filtered = append(filtered, status)
		q.queuedDone[key] = struct{}{}
		doneKeys = append(doneKeys, key)
	}

	request.Request.BlockStatuses = filtered
	if len(doneKeys) > 0 {
		q.requestDoneKeys[request] = doneKeys
	}
	return len(filtered) > 0
}

func (q *BlockStatusRequestQueue) markInFlight(request *BlockStatusRequestWithTargetID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range q.requestDoneKeys[request] {
		delete(q.queuedDone, key)
		q.inFlightDone[key] = struct{}{}
	}
}

func isDoneRequestStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		status.State.Stage == heartbeatpb.BlockStage_DONE
}
