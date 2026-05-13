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

// BlockStatusRequestQueue forwards already-batched block status requests to the
// heartbeat collector.
//
// Dispatcher-side dedupe removes local mailbox amplification before batching,
// while this queue keeps a single reservation map from enqueue until send
// attempt completion so collector backlog cannot re-introduce duplicate WAITING
// or DONE requests.
type BlockStatusRequestQueue struct {
	queue chan *BlockStatusRequestWithTargetID

	mu          sync.Mutex
	pending     map[blockStatusRequestDedupeKey]struct{}
	requestKeys map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey
}

func NewBlockStatusRequestQueue() *BlockStatusRequestQueue {
	return &BlockStatusRequestQueue{
		queue:       make(chan *BlockStatusRequestWithTargetID, 10000),
		pending:     make(map[blockStatusRequestDedupeKey]struct{}),
		requestKeys: make(map[*BlockStatusRequestWithTargetID][]blockStatusRequestDedupeKey),
	}
}

func (q *BlockStatusRequestQueue) Enqueue(request *BlockStatusRequestWithTargetID) {
	if !q.reserveRequestStatuses(request) {
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
		metrics.HeartbeatCollectorBlockStatusRequestQueueLenGauge.Set(float64(len(q.queue)))
		return request
	}
}

// OnSendAttemptFinished releases the reservation for statuses carried by one
// queued request after the collector finishes its send attempt.
//
// The collector does not retry failed requests in place. Any later resend from
// the dispatcher will enqueue a fresh request and reserve the key again.
func (q *BlockStatusRequestQueue) OnSendAttemptFinished(request *BlockStatusRequestWithTargetID) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, key := range q.requestKeys[request] {
		delete(q.pending, key)
	}
	delete(q.requestKeys, request)
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

func (q *BlockStatusRequestQueue) reserveRequestStatuses(request *BlockStatusRequestWithTargetID) bool {
	if request == nil || request.Request == nil {
		return false
	}

	statuses := request.Request.BlockStatuses
	filtered := statuses[:0]
	keys := make([]blockStatusRequestDedupeKey, 0)

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
		if _, ok := q.pending[key]; ok {
			continue
		}

		filtered = append(filtered, status)
		q.pending[key] = struct{}{}
		keys = append(keys, key)
	}

	request.Request.BlockStatuses = filtered
	if len(keys) > 0 {
		q.requestKeys[request] = keys
	}
	return len(filtered) > 0
}

func shouldDeduplicateRequestStatus(status *heartbeatpb.TableSpanBlockStatus) bool {
	return status != nil &&
		status.State != nil &&
		status.State.IsBlocked &&
		(status.State.Stage == heartbeatpb.BlockStage_WAITING ||
			status.State.Stage == heartbeatpb.BlockStage_DONE)
}
