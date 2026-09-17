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

func (s *schemaStore) sendResponse(ctx context.Context, to node.ID, resp messaging.IOTypeT) error {
	msg := messaging.NewSingleTargetMessage(to, messaging.SchemaStoreClientTopic, resp)
	return retry.Do(ctx, func() error {
		return s.mc.SendCommand(msg)
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

// A request ID is scoped to its sending node.
type schemaRequestKey struct {
	from node.ID
	id   uint64
}

func (s *schemaStore) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		req, ok := m.(*messaging.SchemaStoreRequest)
		if !ok || req == nil {
			continue
		}
		key := schemaRequestKey{from: msg.From, id: req.RequestID}
		if req.Operation == messaging.SchemaStoreCancelRequest {
			s.requestMu.Lock()
			if cancel := s.activeRequests[key]; cancel != nil {
				cancel()
			}
			s.requestMu.Unlock()
			continue
		}
		if err := s.submitRequest(ctx, key, req); err != nil {
			// Rejection must not wait or retry on the shared command router.
			resp := &messaging.SchemaStoreResponse{RequestID: req.RequestID, Error: err.Error()}
			code, _ := errors.RFCCode(err)
			resp.ErrorCode = string(code)
			if sendErr := s.mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic, resp)); sendErr != nil {
				return sendErr
			}
		}
	}
	return nil
}

func (s *schemaStore) submitRequest(ctx context.Context, key schemaRequestKey, req *messaging.SchemaStoreRequest) error {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if err := ctx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if err := s.requestCtx.Err(); err != nil {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if len(s.activeRequests) >= schemaStoreMaxPendingRequests {
		return errors.ErrSchemaStoreRequestFailed.GenWithStack("schema store request queue is full")
	}
	if _, exists := s.activeRequests[key]; exists {
		return errors.ErrSchemaStoreRequestFailed.GenWithStack("duplicate schema store request %d", req.RequestID)
	}
	if req.Operation == messaging.SchemaStoreGetTableInfos && (len(req.TableIDs) == 0 || len(req.TableIDs) > messaging.SchemaStoreTableBatchSize) {
		return errors.ErrSchemaStoreRequestFailed.GenWithStack("invalid schema store batch size %d", len(req.TableIDs))
	}
	deadline := time.Unix(0, req.Deadline)
	if !deadline.After(time.Now()) {
		return errors.WrapError(errors.ErrSchemaStoreRequestFailed, context.DeadlineExceeded)
	}
	// Never let a peer extend the server's maximum request lifetime.
	if limit := time.Now().Add(messaging.SchemaStoreRequestTimeout); deadline.After(limit) {
		deadline = limit
	}
	requestCtx, cancel := context.WithDeadline(s.requestCtx, deadline)
	s.activeRequests[key] = cancel
	s.requestPool.SubmitFunc(func() time.Time {
		defer func() {
			cancel()
			s.requestMu.Lock()
			delete(s.activeRequests, key)
			s.requestMu.Unlock()
		}()
		// Canceled queued requests do no storage work. They retain their queue
		// slot until dequeued so repeated cancellation cannot grow the pool queue.
		if requestCtx.Err() == nil {
			s.handleRequest(requestCtx, key.from, req)
		}
		return time.Time{}
	}, time.Now())
	return nil
}

func (s *schemaStore) handleRequest(ctx context.Context, from node.ID, req *messaging.SchemaStoreRequest) {
	resp := &messaging.SchemaStoreResponse{RequestID: req.RequestID}
	var err error
	switch req.Operation {
	case messaging.SchemaStoreRegisterKeyspace:
		err = s.registerKeyspace(ctx, s.requestCtx, req.Keyspace)
	case messaging.SchemaStoreGetAllPhysicalTables:
		var f filter.Filter
		f, err = filter.NewFilter(req.Filter, "", req.CaseSensitive, req.ForceReplicate)
		if err == nil {
			var store *keyspaceSchemaStore
			store, err = s.acquireRequestStore(ctx, req)
			if err == nil {
				resp.Tables, err = store.dataStorage.getAllPhysicalTables(req.Ts, f)
				store.release()
			}
		}
	case messaging.SchemaStoreGetTableInfos:
		resp.TableInfos, resp.More, err = s.getTableInfosBatch(ctx, req)
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
	if err := s.sendResponse(ctx, from, resp); err != nil && ctx.Err() == nil {
		log.Warn("send schema store response failed", zap.Uint32("keyspaceID", req.Keyspace.ID),
			zap.Uint64("requestID", req.RequestID), zap.Error(err))
	}
}

// acquireRequestStore waits with the request context, independently of the
// keyspace lifetime, so a stalled resolved ts cannot retain a worker forever.
func (s *schemaStore) acquireRequestStore(ctx context.Context, req *messaging.SchemaStoreRequest) (*keyspaceSchemaStore, error) {
	store, err := s.acquireKeyspaceSchemaStoreWithContext(ctx, req.Keyspace)
	if err != nil {
		return nil, err
	}
	if !store.waitResolvedTs(ctx, 0, req.Ts, 10*time.Second) {
		store.release()
		if err := ctx.Err(); err != nil {
			return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		return nil, errors.ErrKeyspaceNotFound.GenWithStackByArgs(req.Keyspace.ID)
	}
	return store, nil
}

func (s *schemaStore) getTableInfosBatch(ctx context.Context, req *messaging.SchemaStoreRequest) ([]messaging.SchemaStoreTableInfo, bool, error) {
	store, err := s.acquireRequestStore(ctx, req)
	if err != nil {
		return nil, false, err
	}
	defer store.release()
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
		info, err = store.dataStorage.forceGetTableInfoWithContext(ctx, tableID, req.Ts)
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
