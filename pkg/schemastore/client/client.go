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

package client

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastore"
	"go.uber.org/zap"
)

type response interface {
	messaging.IOTypeT
	GetRequestID() uint64
	GetError() *schemastore.Error
}

type Client struct {
	mc            messaging.MessageCenter
	target        node.ID
	nextRequestID atomic.Uint64
	requests      sync.Map // uint64 request ID -> chan response
}

var (
	schemaStoreClient     *Client
	schemaStoreClientOnce sync.Once
)

// New registers the response handler. Create one client per message center.
func New(mc messaging.MessageCenter, target node.ID) *Client {
	c := &Client{mc: mc, target: target}
	mc.RegisterHandler(messaging.SchemaStoreClientTopic, c.handleMessage)
	return c
}

// GetSchemaStoreClient lazily creates the client for the local server.
func GetSchemaStoreClient() *Client {
	schemaStoreClientOnce.Do(func() {
		mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)
		schemaStoreClient = New(mc, node.ID(appcontext.GetID()))
	})
	return schemaStoreClient
}

// SetSchemaStoreClientForTest replaces the singleton and returns a restore function.
// Tests must run serially and stop singleton users before replacing or restoring it.
func SetSchemaStoreClientForTest(c *Client) func() {
	previous := schemaStoreClient
	schemaStoreClient = c
	schemaStoreClientOnce.Do(func() {})
	return func() {
		schemaStoreClient = previous
		if previous == nil {
			schemaStoreClientOnce = sync.Once{}
		}
	}
}

func (c *Client) handleMessage(_ context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		resp, ok := m.(response)
		if !ok || resp.GetRequestID() == 0 {
			continue
		}
		if value, ok := c.requests.LoadAndDelete(resp.GetRequestID()); ok {
			// Exactly one response claims the single buffered slot. Duplicates
			// and late responses cannot block the shared command router.
			value.(chan response) <- resp
		}
	}
	return nil
}

// GetTableInfos fetches bounded batches and rejects incomplete responses. No
// partial result is returned if any batch fails or the caller cancels.
func (c *Client) GetTableInfos(ctx context.Context, meta common.KeyspaceMeta, tableIDs []int64, ts uint64) ([]*common.TableInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, schemastore.RequestTimeout)
	defer cancel()
	result := make([]*common.TableInfo, 0, len(tableIDs))
	skipped := 0
	var firstSkipped schemastore.TableInfoResult
	for len(tableIDs) > 0 {
		batch := tableIDs[:min(len(tableIDs), schemastore.TableBatchSize)]
		req := &schemastore.GetTableInfosRequest{
			RequestID: c.nextRequestID.Add(1), Keyspace: schemastore.NewKeyspaceMeta(meta), TableIDs: batch, Ts: ts,
		}
		reply, err := c.request(ctx, req.RequestID, req)
		if err != nil {
			return nil, err
		}
		resp, ok := reply.(*schemastore.GetTableInfosResponse)
		if !ok {
			return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("unexpected response to table infos request: %T", reply)
		}

		count := len(resp.TableInfos)
		if count == 0 || count > len(batch) || (!resp.More && count != len(batch)) || (resp.More && count == len(batch)) {
			return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("incomplete schema store batch: requested %d, received %d, more %t", len(batch), count, resp.More)
		}
		for i, table := range resp.TableInfos {
			if err := ctx.Err(); err != nil {
				return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
			}
			if table.TableID != batch[i] {
				return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("unexpected table in schema store batch: expected %d, received %d", batch[i], table.TableID)
			}
			if table.Error != "" {
				if skipped == 0 {
					firstSkipped = table
				}
				skipped++
				continue
			}
			info, err := common.UnmarshalJSONToTableInfo(table.TableInfo)
			if err != nil {
				return nil, errors.WrapError(errors.ErrUnmarshalFailed, err)
			}
			result = append(result, info)
		}
		tableIDs = tableIDs[count:]
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if skipped > 0 {
		log.Warn("schema store skipped tables with explicit errors", zap.Any("keyspace", meta), zap.Uint64("ts", ts), zap.Int("tables", skipped),
			zap.Int64("firstTableID", firstSkipped.TableID), zap.String("firstError", firstSkipped.Error))
	}
	return result, nil
}

func (c *Client) GetAllPhysicalTables(ctx context.Context, meta common.KeyspaceMeta, ts uint64,
	filterConfig *config.FilterConfig, caseSensitive, forceReplicate bool,
) ([]commonEvent.Table, error) {
	req := &schemastore.GetAllPhysicalTablesRequest{
		RequestID: c.nextRequestID.Add(1), Keyspace: schemastore.NewKeyspaceMeta(meta), Ts: ts,
		Filter: schemastore.NewFilterConfig(filterConfig), CaseSensitive: caseSensitive, ForceReplicate: forceReplicate,
	}
	reply, err := c.request(ctx, req.RequestID, req)
	if err != nil {
		return nil, err
	}
	resp, ok := reply.(*schemastore.GetAllPhysicalTablesResponse)
	if !ok {
		return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("unexpected response to physical tables request: %T", reply)
	}
	return schemastore.PhysicalTablesFromProto(resp.Tables), nil
}

func (c *Client) request(ctx context.Context, requestID uint64, req messaging.IOTypeT) (response, error) {
	ctx, cancel := context.WithTimeout(ctx, schemastore.RequestTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if c.target.IsEmpty() {
		return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("server id is empty")
	}
	ch := make(chan response, 1)
	c.requests.Store(requestID, ch)
	defer c.requests.Delete(requestID)
	if err := c.mc.SendCommand(messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreTopic, req)); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
	case resp := <-ch:
		if err := ctx.Err(); err != nil {
			return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		if err := resp.GetError().ToError(); err != nil {
			return nil, err
		}
		return resp, nil
	}
}
