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
	"go.uber.org/zap"
)

type Client struct {
	mc            messaging.MessageCenter
	target        node.ID
	nextRequestID atomic.Uint64
	requests      sync.Map // uint64 request ID -> chan *messaging.SchemaStoreResponse
}

// New registers the response handler. Create one client per message center.
func New(mc messaging.MessageCenter, target node.ID) *Client {
	c := &Client{mc: mc, target: target}
	mc.RegisterHandler(messaging.SchemaStoreClientTopic, c.handleMessage)
	return c
}

func GetSchemaStoreClient() *Client {
	return appcontext.GetService[*Client](appcontext.SchemaStoreClient)
}

func (c *Client) handleMessage(_ context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		resp, ok := m.(*messaging.SchemaStoreResponse)
		if !ok || resp == nil {
			continue
		}
		if value, ok := c.requests.LoadAndDelete(resp.RequestID); ok {
			// Exactly one response claims the single buffered slot. Duplicates
			// and late responses cannot block the shared command router.
			value.(chan *messaging.SchemaStoreResponse) <- resp
		}
	}
	return nil
}

// GetTableInfos fetches bounded batches and rejects incomplete responses. No
// partial result is returned if any batch fails or the caller cancels.
func (c *Client) GetTableInfos(ctx context.Context, meta common.KeyspaceMeta, tableIDs []int64, ts uint64) ([]*common.TableInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, messaging.SchemaStoreRequestTimeout)
	defer cancel()
	result := make([]*common.TableInfo, 0, len(tableIDs))
	skipped := 0
	for len(tableIDs) > 0 {
		batch := tableIDs[:min(len(tableIDs), messaging.SchemaStoreTableBatchSize)]
		resp, err := c.request(ctx, &messaging.SchemaStoreRequest{
			Operation: messaging.SchemaStoreGetTableInfos, Keyspace: meta, TableIDs: batch, Ts: ts,
		})
		if err != nil {
			return nil, err
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
		log.Warn("schema store skipped tables with explicit errors", zap.Any("keyspace", meta), zap.Uint64("ts", ts), zap.Int("tables", skipped))
	}
	return result, nil
}

func (c *Client) RegisterKeyspace(ctx context.Context, meta common.KeyspaceMeta) error {
	_, err := c.request(ctx, &messaging.SchemaStoreRequest{Operation: messaging.SchemaStoreRegisterKeyspace, Keyspace: meta})
	return err
}

func (c *Client) GetAllPhysicalTables(ctx context.Context, meta common.KeyspaceMeta, ts uint64,
	filterConfig *config.FilterConfig, caseSensitive, forceReplicate bool,
) ([]commonEvent.Table, error) {
	resp, err := c.request(ctx, &messaging.SchemaStoreRequest{
		Operation: messaging.SchemaStoreGetAllPhysicalTables, Keyspace: meta, Ts: ts,
		Filter: filterConfig, CaseSensitive: caseSensitive, ForceReplicate: forceReplicate,
	})
	if err != nil {
		return nil, err
	}
	return resp.Tables, nil
}

func (c *Client) request(ctx context.Context, req *messaging.SchemaStoreRequest) (*messaging.SchemaStoreResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, messaging.SchemaStoreRequestTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	if c.target.IsEmpty() {
		return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("server id is empty")
	}
	deadline, _ := ctx.Deadline()
	req.Deadline = deadline.UnixNano()
	req.RequestID = c.nextRequestID.Add(1)
	ch := make(chan *messaging.SchemaStoreResponse, 1)
	c.requests.Store(req.RequestID, ch)
	defer c.requests.Delete(req.RequestID)
	if err := c.mc.SendCommand(messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreTopic, req)); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		// Cancellation bypasses the worker pool. If delivery fails, the deadline
		// carried by the original request still bounds server work.
		_ = c.mc.SendCommand(messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreTopic,
			&messaging.SchemaStoreRequest{RequestID: req.RequestID, Operation: messaging.SchemaStoreCancelRequest}))
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
	case resp := <-ch:
		if err := ctx.Err(); err != nil {
			return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
		}
		if resp.Error != "" {
			if resp.ErrorCode != "" {
				return nil, errors.Normalize(resp.Error, errors.RFCCodeText(resp.ErrorCode)).GenWithStackByArgs()
			}
			return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("%s", resp.Error)
		}
		return resp, nil
	}
}
