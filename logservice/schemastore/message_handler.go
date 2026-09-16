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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/retry"
	"go.uber.org/zap"
)

const schemaStoreResponseMaxTries = 10

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

func (s *schemaStore) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	switch msg.Type {
	case messaging.TypeSchemaStoreRequest:
		for _, m := range msg.Message {
			req, ok := m.(*messaging.SchemaStoreRequest)
			if !ok {
				continue
			}
			go s.handleRequest(ctx, msg.From, req)
		}
	case messaging.TypeSchemaStoreTableInfosRequest:
		for _, m := range msg.Message {
			req, ok := m.(*messaging.SchemaStoreTableInfosRequest)
			if !ok {
				log.Warn("invalid schema store request message, ignore it",
					zap.Any("msg", msg))
				continue
			}
			from := msg.From
			go s.handleTableInfosRequest(ctx, from, req)
		}
	default:
		log.Warn("unknown message type, ignore it",
			zap.String("type", msg.Type.String()),
			zap.Any("message", msg.Message))
	}
	return nil
}

func (s *schemaStore) handleTableInfosRequest(
	ctx context.Context,
	from node.ID,
	req *messaging.SchemaStoreTableInfosRequest,
) {
	if req == nil {
		return
	}

	completion := &messaging.SchemaStoreTableInfosResponse{RequestID: req.RequestID, Done: true}
	defer func() {
		if err := s.sendResponse(ctx, from, completion); err != nil {
			log.Warn("send schema store response failed",
				zap.Any("keyspaceID", req.KeyspaceID),
				zap.Uint64("requestID", req.RequestID),
				zap.Error(err))
		}
	}()

	keyspaceMeta := common.KeyspaceMeta{
		ID:   req.KeyspaceID,
		Name: req.KeyspaceName,
	}

	for _, tableID := range req.TableIDs {
		if err := ctx.Err(); err != nil {
			completion.Error = err.Error()
			return
		}

		resp := &messaging.SchemaStoreTableInfosResponse{RequestID: req.RequestID, TableID: tableID}
		err := s.RegisterTable(keyspaceMeta, tableID, req.Ts)
		if err == nil {
			var tableInfo *common.TableInfo
			tableInfo, err = s.GetTableInfo(keyspaceMeta, tableID, req.Ts)
			if err == nil {
				if tableInfo == nil {
					resp.Error = "table info is nil"
				} else {
					resp.TableInfo, err = tableInfo.Marshal()
					err = errors.WrapError(errors.ErrMarshalFailed, err)
				}
			}
		}
		if err != nil {
			resp.Error = err.Error()
		}
		if err := s.sendResponse(ctx, from, resp); err != nil {
			// A failed delivery must never be followed by a successful completion.
			completion.Error = err.Error()
			return
		}
	}
}

func (s *schemaStore) handleRequest(ctx context.Context, from node.ID, req *messaging.SchemaStoreRequest) {
	resp := &messaging.SchemaStoreResponse{RequestID: req.RequestID}
	var err error
	switch req.Operation {
	case messaging.SchemaStoreRegisterKeyspace:
		// The message handler's context has the service lifetime, independent of the caller.
		err = s.RegisterKeyspace(ctx, req.Keyspace)
	case messaging.SchemaStoreGetAllPhysicalTables:
		// Table discovery does not need a timezone.
		var f filter.Filter
		f, err = filter.NewFilter(req.Filter, "", req.CaseSensitive, req.ForceReplicate)
		if err == nil {
			resp.Tables, err = s.GetAllPhysicalTables(req.Keyspace, req.Ts, f)
		}
	default:
		err = errors.ErrSchemaStoreRequestFailed.GenWithStack("unknown schema store operation: %d", req.Operation)
	}
	if err != nil {
		resp.Error = err.Error()
		code, _ := errors.RFCCode(err)
		resp.ErrorCode = string(code)
	}
	if err := s.sendResponse(ctx, from, resp); err != nil {
		log.Warn("send schema store response failed",
			zap.Uint32("keyspaceID", req.Keyspace.ID),
			zap.Uint64("requestID", req.RequestID), zap.Error(err))
	}
}
