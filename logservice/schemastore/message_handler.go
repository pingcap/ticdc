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
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/schemastore"
	"go.uber.org/zap"
)

const (
	schemaStoreResponseMaxTries = 10
	schemaStoreRequestWorkers   = 16
	schemaStoreRequestQueueSize = 256
)

type schemaStoreMessageHandler struct {
	ctx      context.Context
	cancel   context.CancelFunc
	store    *schemaStore
	mc       messaging.MessageCenter
	requests chan schemaRequest
	wg       sync.WaitGroup
}

type schemaRequest struct {
	from    node.ID
	message messaging.IOTypeT
}

func newSchemaStoreMessageHandler(ctx context.Context, store *schemaStore, mc messaging.MessageCenter, workers int) *schemaStoreMessageHandler {
	ctx, cancel := context.WithCancel(ctx)
	h := &schemaStoreMessageHandler{
		ctx: ctx, cancel: cancel, store: store, mc: mc,
		requests: make(chan schemaRequest, schemaStoreRequestQueueSize),
	}
	for range workers {
		h.wg.Go(h.runWorker)
	}
	mc.RegisterHandler(messaging.SchemaStoreTopic, h.handleMessage)
	return h
}

// stop discards pending requests before the store closes its keyspaces and waits
// for running workers. The channel stays open for concurrent submitters.
func (h *schemaStoreMessageHandler) stop() {
	h.mc.DeRegisterHandler(messaging.SchemaStoreTopic)
	h.cancel()
	h.discardPendingRequests()
}

func (h *schemaStoreMessageHandler) discardPendingRequests() {
	for {
		select {
		case <-h.requests:
		default:
			return
		}
	}
}

func (h *schemaStoreMessageHandler) submit(req schemaRequest) error {
	if err := h.ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	select {
	case h.requests <- req:
		// A sender can pass the first check just before stop drains the queue.
		// Release that late submission too, without closing the shared channel.
		if err := h.ctx.Err(); err != nil {
			h.discardPendingRequests()
			return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		return nil
	default:
		return errors.ErrSchemaStoreRequestFailed.GenWithStack("schema store request queue is full")
	}
}

func (h *schemaStoreMessageHandler) runWorker() {
	for {
		select {
		case <-h.ctx.Done():
			return
		case req := <-h.requests:
			// The timeout starts on dequeue. Existing synchronous storage calls
			// retain their own waiting and cancellation semantics.
			ctx, cancel := context.WithTimeout(h.ctx, schemastore.RequestTimeout)
			h.handleRequest(ctx, req.from, req.message)
			cancel()
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
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	for _, message := range msg.Message {
		var rejection messaging.IOTypeT
		switch req := message.(type) {
		case *schemastore.GetTableInfosRequest:
			if req == nil {
				continue
			}
			if err := h.submit(schemaRequest{from: msg.From, message: req}); err != nil {
				rejection = &schemastore.GetTableInfosResponse{RequestID: req.RequestID, Error: schemastore.NewError(err)}
			}
		case *schemastore.GetAllPhysicalTablesRequest:
			if req == nil {
				continue
			}
			if err := h.submit(schemaRequest{from: msg.From, message: req}); err != nil {
				rejection = &schemastore.GetAllPhysicalTablesResponse{RequestID: req.RequestID, Error: schemastore.NewError(err)}
			}
		default:
			continue
		}
		// Rejection must not wait or retry on the shared command router.
		if rejection != nil {
			if err := h.mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic, rejection)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *schemaStoreMessageHandler) handleRequest(ctx context.Context, from node.ID, message messaging.IOTypeT) {
	if ctx.Err() != nil {
		return
	}
	var response messaging.IOTypeT
	var requestID uint64
	var keyspaceID uint32
	switch req := message.(type) {
	case *schemastore.GetTableInfosRequest:
		requestID, keyspaceID = req.RequestID, req.Keyspace.ID
		infos, more, err := h.getTableInfosBatch(ctx, req)
		response = &schemastore.GetTableInfosResponse{RequestID: requestID, TableInfos: infos, More: more, Error: schemastore.NewError(err)}
	case *schemastore.GetAllPhysicalTablesRequest:
		requestID, keyspaceID = req.RequestID, req.Keyspace.ID
		resp := &schemastore.GetAllPhysicalTablesResponse{RequestID: requestID}
		response = resp
		if req.Filter == nil {
			resp.Error = schemastore.NewError(errors.ErrSchemaStoreRequestFailed.GenWithStack("schema store filter is missing"))
			break
		}
		f, err := filter.NewFilter(schemastore.FilterConfigFromProto(req.Filter), "", req.CaseSensitive, req.ForceReplicate)
		if err == nil {
			tables, readErr := h.store.GetAllPhysicalTables(req.Keyspace.ToCommon(), req.Ts, f)
			err = readErr
			resp.Tables = schemastore.NewPhysicalTables(tables)
		}
		resp.Error = schemastore.NewError(err)
	default:
		return
	}
	if ctx.Err() != nil {
		return
	}
	if err := h.sendResponse(ctx, from, response); err != nil && ctx.Err() == nil {
		log.Warn("send schema store response failed", zap.Uint32("keyspaceID", keyspaceID), zap.Uint64("requestID", requestID), zap.Error(err))
	}
}

func (h *schemaStoreMessageHandler) getTableInfosBatch(ctx context.Context, req *schemastore.GetTableInfosRequest) ([]schemastore.TableInfoResult, bool, error) {
	if len(req.TableIDs) == 0 || len(req.TableIDs) > schemastore.TableBatchSize {
		return nil, false, errors.ErrSchemaStoreRequestFailed.GenWithStack("invalid schema store batch size %d", len(req.TableIDs))
	}

	store, err := h.store.acquireKeyspaceSchemaStore(req.Keyspace.ToCommon())
	if err != nil {
		return nil, false, err
	}
	defer store.release()
	if !store.waitResolvedTs(0, req.Ts, 2*time.Second) {
		return nil, false, errors.ErrKeyspaceNotFound.FastGenByArgs(req.Keyspace.ID)
	}
	result := make([]schemastore.TableInfoResult, 0, len(req.TableIDs))
	// Reserve the response envelope, including the continuation flag.
	size := (&schemastore.GetTableInfosResponse{RequestID: req.RequestID, More: true}).Size()
	for _, tableID := range req.TableIDs {
		if err := ctx.Err(); err != nil {
			return nil, false, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		item := schemastore.TableInfoResult{TableID: tableID}
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
		itemSize := (&schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{item}}).Size()
		if size+itemSize > schemastore.TableBatchBytes {
			if len(result) == 0 {
				return nil, false, errors.ErrSchemaStoreRequestFailed.GenWithStack("schema for table %d exceeds the response size limit", tableID)
			}
			return result, true, nil
		}
		size += itemSize
		result = append(result, item)
	}
	return result, false, nil
}
