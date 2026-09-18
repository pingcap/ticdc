// Copyright 2026 PingCAP, Inc.
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

package schemastore

import (
	"context"
	"encoding/json"
	"slices"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const (
	schemaStoreResponseMaxTries   = 10
	schemaStoreRequestWorkers     = 16
	schemaStoreMaxPendingRequests = 256
)

// schemaStoreMessageHandler owns request admission, queuing and execution.
// Only dispatch accesses the queue and the running request count.
type schemaStoreMessageHandler struct {
	store  *schemaStore
	mc     messaging.MessageCenter
	ctx    context.Context
	cancel context.CancelFunc

	submissions chan schemaRequestSubmission
	tasks       chan schemaRequest
	completed   chan struct{}
	stopped     chan struct{}
	workers     sync.WaitGroup
}

type schemaRequest struct {
	from      node.ID
	message   *messaging.SchemaStoreRequest
	expiresAt time.Time
}

type schemaRequestSubmission struct {
	request schemaRequest
	result  chan error
}

func newSchemaStoreMessageHandler(ctx context.Context, store *schemaStore, mc messaging.MessageCenter, workers int) *schemaStoreMessageHandler {
	ctx, cancel := context.WithCancel(ctx)
	h := &schemaStoreMessageHandler{
		store: store, mc: mc, ctx: ctx, cancel: cancel,
		submissions: make(chan schemaRequestSubmission),
		tasks:       make(chan schemaRequest),
		completed:   make(chan struct{}),
		stopped:     make(chan struct{}),
	}
	for range workers {
		h.workers.Go(h.runWorker)
	}
	go h.dispatch()
	mc.RegisterHandler(messaging.SchemaStoreTopic, h.handleMessage)
	return h
}

// stop releases queued requests without waiting for synchronous storage calls.
// The store must close its keyspaces before waiting for the workers to exit.
func (h *schemaStoreMessageHandler) stop() {
	h.mc.DeRegisterHandler(messaging.SchemaStoreTopic)
	h.cancel()
	<-h.stopped
}

func (h *schemaStoreMessageHandler) dispatch() {
	queue := make([]schemaRequest, 0, schemaStoreMaxPendingRequests)
	running := 0
	timer := time.NewTimer(messaging.SchemaStoreRequestTimeout)
	defer timer.Stop()
	defer close(h.stopped)
	defer close(h.tasks)
	defer func() { clear(queue) }()
	for {
		if h.ctx.Err() != nil {
			return
		}
		// Admission assigns expiration times in FIFO order. Expire requests even
		// when every worker is busy, releasing both their payloads and queue slots.
		now := time.Now()
		expired := 0
		for expired < len(queue) && !queue[expired].expiresAt.After(now) {
			expired++
		}
		queue = slices.Delete(queue, 0, expired)
		var output chan schemaRequest
		var next schemaRequest
		var expiration <-chan time.Time
		timer.Stop()
		if len(queue) > 0 {
			output, next = h.tasks, queue[0]
			timer.Reset(time.Until(next.expiresAt))
			expiration = timer.C
		}
		select {
		case <-h.ctx.Done():
			return
		case submission := <-h.submissions:
			if running+len(queue) >= schemaStoreMaxPendingRequests {
				submission.result <- errors.ErrSchemaStoreRequestFailed.GenWithStack("schema store request queue is full")
				continue
			}
			submission.request.expiresAt = time.Now().Add(messaging.SchemaStoreRequestTimeout)
			queue = append(queue, submission.request)
			submission.result <- nil
		case output <- next:
			running++
			queue = slices.Delete(queue, 0, 1)
		case <-h.completed:
			running--
		case <-expiration:
		}
	}
}

func (h *schemaStoreMessageHandler) runWorker() {
	for request := range h.tasks {
		// This deadline bounds response delivery; existing storage calls keep
		// their own waiting and lifetime semantics.
		ctx, cancel := context.WithDeadline(h.ctx, request.expiresAt)
		h.handleRequest(ctx, request.from, request.message)
		cancel()
		select {
		case h.completed <- struct{}{}:
		case <-h.ctx.Done():
			return
		}
	}
}

func (h *schemaStoreMessageHandler) sendResponse(ctx context.Context, to node.ID, resp messaging.IOTypeT) error {
	msg := messaging.NewSingleTargetMessage(to, messaging.SchemaStoreClientTopic, resp)
	return retry.Do(ctx, func() error {
		return h.mc.SendCommand(msg)
	}, retry.WithMaxTries(schemaStoreResponseMaxTries), retry.WithBackoffBaseDelay(10),
		retry.WithBackoffMaxDelay(100), retry.WithIsRetryableErr(func(err error) bool {
			var appErr interface{ GetType() errors.ErrorType }
			if !errors.As(err, &appErr) {
				return false
			}
			switch appErr.GetType() {
			case errors.ErrorTypeMessageCongested, errors.ErrorTypeConnectionNotFound, errors.ErrorTypeConnectionFailed:
				return true
			default:
				return false
			}
		}))
}

func (h *schemaStoreMessageHandler) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		req, ok := m.(*messaging.SchemaStoreRequest)
		if !ok || req == nil {
			continue
		}
		if err := h.submit(ctx, msg.From, req); err != nil {
			// Rejection must not wait or retry on the shared command router.
			resp := &messaging.SchemaStoreResponse{RequestID: req.RequestID, Error: err.Error()}
			code, _ := errors.RFCCode(err)
			resp.ErrorCode = string(code)
			if sendErr := h.mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic, resp)); sendErr != nil {
				return sendErr
			}
		}
	}
	return nil
}

func (h *schemaStoreMessageHandler) submit(ctx context.Context, from node.ID, req *messaging.SchemaStoreRequest) error {
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if err := h.ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if req.Operation == messaging.SchemaStoreGetTableInfos && (len(req.TableIDs) == 0 || len(req.TableIDs) > messaging.SchemaStoreTableBatchSize) {
		return errors.ErrSchemaStoreRequestFailed.GenWithStack("invalid schema store batch size %d", len(req.TableIDs))
	}
	// Dispatch only handles queue state, so admission never waits for storage.
	// The buffered result also lets dispatch finish if the submitter cancels.
	submission := schemaRequestSubmission{
		request: schemaRequest{from: from, message: req},
		result:  make(chan error, 1),
	}
	select {
	case h.submissions <- submission:
	case <-ctx.Done():
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
	case <-h.ctx.Done():
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, h.ctx.Err())
	}
	select {
	case err := <-submission.result:
		return err
	case <-ctx.Done():
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
	case <-h.ctx.Done():
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, h.ctx.Err())
	}
}

func (h *schemaStoreMessageHandler) handleRequest(ctx context.Context, from node.ID, req *messaging.SchemaStoreRequest) {
	if ctx.Err() != nil {
		return
	}
	resp := &messaging.SchemaStoreResponse{RequestID: req.RequestID}
	var err error
	switch req.Operation {
	case messaging.SchemaStoreRegisterKeyspace:
		err = h.store.RegisterKeyspace(h.ctx, req.Keyspace)
	case messaging.SchemaStoreGetAllPhysicalTables:
		var f filter.Filter
		f, err = filter.NewFilter(req.Filter, "", req.CaseSensitive, req.ForceReplicate)
		if err == nil {
			resp.Tables, err = h.store.GetAllPhysicalTables(req.Keyspace, req.Ts, f)
		}
	case messaging.SchemaStoreGetTableInfos:
		resp.TableInfos, resp.More, err = h.getTableInfosBatch(ctx, req)
	default:
		err = errors.ErrSchemaStoreRequestFailed.GenWithStack("unknown schema store operation: %d", req.Operation)
	}
	if err != nil {
		resp.Error = err.Error()
		code, _ := errors.RFCCode(err)
		resp.ErrorCode = string(code)
		resp.TableInfos = nil
	}
	if ctx.Err() != nil {
		return
	}
	if err := h.sendResponse(ctx, from, resp); err != nil && ctx.Err() == nil {
		log.Warn("send schema store response failed", zap.Uint32("keyspaceID", req.Keyspace.ID),
			zap.Uint64("requestID", req.RequestID), zap.Error(err))
	}
}

func (h *schemaStoreMessageHandler) getTableInfosBatch(ctx context.Context, req *messaging.SchemaStoreRequest) ([]messaging.SchemaStoreTableInfo, bool, error) {
	store, err := h.store.acquireKeyspaceSchemaStore(req.Keyspace)
	if err != nil {
		return nil, false, err
	}
	defer store.release()
	if !store.waitResolvedTs(0, req.Ts, 2*time.Second) {
		return nil, false, errors.ErrKeyspaceNotFound.FastGenByArgs(req.Keyspace.ID)
	}
	result := make([]messaging.SchemaStoreTableInfo, 0, len(req.TableIDs))
	// Reserve space for the response envelope; account for base64 and JSON
	// overhead in each table result, not just the raw schema bytes.
	size := 128
	for _, tableID := range req.TableIDs {
		if err := ctx.Err(); err != nil {
			return nil, false, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		item := messaging.SchemaStoreTableInfo{TableID: tableID}
		var info *common.TableInfo
		info, err = store.dataStorage.getTableInfoAtTs(tableID, req.Ts)
		if err == nil {
			if info == nil {
				return nil, false, errors.ErrSchemaStoreRequestFailed.GenWithStack("table info is nil for table %d", tableID)
			}
			item.TableInfo, err = info.Marshal()
			if err != nil {
				return nil, false, errors.WrapError(errors.ErrMarshalFailed, err)
			}
		} else {
			if ctx.Err() != nil {
				return nil, false, errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
			}
			item.Error = err.Error()
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			return nil, false, errors.WrapError(errors.ErrMarshalFailed, err)
		}
		if size+len(encoded)+1 > messaging.SchemaStoreTableBatchBytes {
			if len(result) == 0 {
				return nil, false, errors.ErrSchemaStoreRequestFailed.GenWithStack("schema for table %d exceeds the response size limit", tableID)
			}
			return result, true, nil
		}
		size += len(encoded) + 1
		result = append(result, item)
	}
	return result, false, nil
}
