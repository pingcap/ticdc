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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
)

// HeartbeatRequestQueue is a channel for all event dispatcher managers to send heartbeat requests to HeartBeatCollector
type HeartBeatRequestWithTargetID struct {
	TargetID node.ID
	Request  *heartbeatpb.HeartBeatRequest
}

type HeartbeatRequestQueue struct {
	queue *chann.UnlimitedChannel[*HeartBeatRequestWithTargetID, any]
}

func NewHeartbeatRequestQueue() *HeartbeatRequestQueue {
	return &HeartbeatRequestQueue{
		queue: chann.NewUnlimitedChannel[*HeartBeatRequestWithTargetID, any](nil, nil),
	}
}

func (q *HeartbeatRequestQueue) Enqueue(request *HeartBeatRequestWithTargetID) {
	q.queue.Push(request)
}

func (q *HeartbeatRequestQueue) Dequeue() []*HeartBeatRequestWithTargetID {
	buffer := make([]*HeartBeatRequestWithTargetID, 0, 1024)
	requests, ok := q.queue.GetMultipleNoGroup(buffer)
	if !ok {
		log.Error("heartbeat request queue is closed")
		return nil
	}

	requestMap := make(map[node.ID]*HeartBeatRequestWithTargetID)
	spanStatusMap := make(map[node.ID]map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus)
	for _, request := range requests {
		ret, ok := requestMap[request.TargetID]
		if !ok {
			requestMap[request.TargetID] = request
		} else {
			// update watermark and error
			ret.Request.Watermark = request.Request.Watermark
			if ret.Request.Err == nil && request.Request.Err != nil {
				ret.Request.Err = request.Request.Err
			}
		}

		for _, status := range request.Request.Statuses {
			if _, ok := spanStatusMap[request.TargetID]; !ok {
				spanStatusMap[request.TargetID] = make(map[heartbeatpb.DispatcherID]*heartbeatpb.TableSpanStatus)
			}
			spanStatusMap[request.TargetID][*status.ID] = status
		}
	}

	result := make([]*HeartBeatRequestWithTargetID, 0, len(requestMap))
	for node, request := range requestMap {
		if spanStatusMap[node] == nil {
			request.Request.Statuses = make([]*heartbeatpb.TableSpanStatus, 0, len(spanStatusMap[node]))
			for _, status := range spanStatusMap[node] {
				request.Request.Statuses = append(request.Request.Statuses, status)
			}
			request.Request.CompeleteStatus = true
		} else {
			request.Request.CompeleteStatus = false
		}
		result = append(result, request)
	}
	log.Info("heartbeat request queue dequeue", zap.Int("result size", len(result)), zap.Any("len requests", len(requests)))
	return result
}

func (q *HeartbeatRequestQueue) Close() {
	q.queue.Close()
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
}

func (q *BlockStatusRequestQueue) Dequeue(ctx context.Context) *BlockStatusRequestWithTargetID {
	select {
	case <-ctx.Done():
		return nil
	case request := <-q.queue:
		return request
	}
}

func (q *BlockStatusRequestQueue) Close() {
	close(q.queue)
}
