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

	"github.com/pingcap/ticdc/heartbeatpb"
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
}

func NewBlockStatusRequestQueue() *BlockStatusRequestQueue {
	return &BlockStatusRequestQueue{
		queue: make(chan *BlockStatusRequestWithTargetID, 10000),
	}
}

func (q *BlockStatusRequestQueue) Enqueue(request *BlockStatusRequestWithTargetID) {
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

func (q *BlockStatusRequestQueue) Close() {
	close(q.queue)
}

type RecoverDispatcherRequestWithTargetID struct {
	TargetID      node.ID
	Request       *heartbeatpb.RecoverDispatcherRequest
	FallbackErrCh chan<- error
}

// RecoverDispatcherRequestQueue is a channel for dispatcher managers to send dispatcher recovery requests
// to HeartBeatCollector.
type RecoverDispatcherRequestQueue struct {
	queue chan *RecoverDispatcherRequestWithTargetID
}

func NewRecoverDispatcherRequestQueue() *RecoverDispatcherRequestQueue {
	return &RecoverDispatcherRequestQueue{
		// Recover requests are expected to be sparse; keep a moderate buffer to
		// absorb short bursts without reserving excessive memory.
		queue: make(chan *RecoverDispatcherRequestWithTargetID, 1024),
	}
}

func (q *RecoverDispatcherRequestQueue) Enqueue(request *RecoverDispatcherRequestWithTargetID) {
	q.queue <- request
}

func (q *RecoverDispatcherRequestQueue) Dequeue(ctx context.Context) *RecoverDispatcherRequestWithTargetID {
	select {
	case <-ctx.Done():
		return nil
	case request := <-q.queue:
		return request
	}
}

func (q *RecoverDispatcherRequestQueue) Close() {
	close(q.queue)
}
