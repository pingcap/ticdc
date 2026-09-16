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

package schemastoreclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

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

const schemaStoreRequestTimeout = 10 * time.Minute

type tableInfosRequest struct {
	ctx       context.Context
	responses chan *messaging.SchemaStoreTableInfosResponse
}

type Client struct {
	mc messaging.MessageCenter

	nextRequestID atomic.Uint64

	pending  sync.Map // uint64 request ID -> *tableInfosRequest
	requests sync.Map // uint64 request ID -> chan *messaging.SchemaStoreResponse
}

var (
	schemaStoreClientMu sync.Mutex
	// schemaStoreClientInstance is a server-level singleton. It's created lazily,
	// because MessageCenter is wired by server bootstrap.
	schemaStoreClientInstance atomic.Pointer[Client]
)

// GetSchemaStoreClient returns the shared client for the current message center.
func GetSchemaStoreClient() *Client {
	mc := appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)

	if c := schemaStoreClientInstance.Load(); c != nil && c.mc == mc {
		return c
	}

	schemaStoreClientMu.Lock()
	defer schemaStoreClientMu.Unlock()

	// Allow tests to swap message center by rebuilding the client when mc changed.
	if c := schemaStoreClientInstance.Load(); c != nil && c.mc == mc {
		return c
	}

	c := &Client{mc: mc}
	c.mc.RegisterHandler(messaging.SchemaStoreClientTopic, c.handleMessage)
	schemaStoreClientInstance.Store(c)
	return c
}

func (c *Client) handleMessage(ctx context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		if resp, ok := m.(*messaging.SchemaStoreResponse); ok {
			if value, ok := c.requests.LoadAndDelete(resp.RequestID); ok {
				// Only one response can claim this channel. Its single buffered slot
				// stays open so delivery cannot block even if the caller has canceled.
				value.(chan *messaging.SchemaStoreResponse) <- resp
			}
			continue
		}
		resp, ok := m.(*messaging.SchemaStoreTableInfosResponse)
		if !ok {
			log.Warn("invalid schema store response message, ignore it",
				zap.String("type", msg.Type.String()),
				zap.Any("message", m))
			continue
		}

		value, ok := c.pending.Load(resp.RequestID)
		if !ok {
			log.Debug("schema store response received but request already removed",
				zap.Uint64("requestID", resp.RequestID),
				zap.Int64("tableID", resp.TableID),
				zap.Bool("done", resp.Done))
			continue
		}
		req := value.(*tableInfosRequest)

		// Apply backpressure while the caller decodes earlier responses. Cancellation
		// releases the router if the caller exits without draining the channel.
		select {
		case req.responses <- resp:
		case <-req.ctx.Done():
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// GetTableInfos waits for a result or explicit error for every requested table,
// followed by a successful completion. Cancellation discards any partial result.
func (c *Client) GetTableInfos(
	ctx context.Context,
	keyspaceMeta common.KeyspaceMeta,
	tableIDs []int64,
	ts uint64,
) ([]*common.TableInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaStoreRequestTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	reqID := c.nextRequestID.Add(1)

	bufferSize := len(tableIDs) + 1
	if bufferSize < 8 {
		bufferSize = 8
	}
	if bufferSize > 4096 {
		bufferSize = 4096
	}

	respCh := make(chan *messaging.SchemaStoreTableInfosResponse, bufferSize)
	c.pending.Store(reqID, &tableInfosRequest{ctx: ctx, responses: respCh})
	defer c.pending.Delete(reqID)

	target := node.ID(appcontext.GetID())
	if target.IsEmpty() {
		return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("server id is empty")
	}
	err := c.mc.SendCommand(messaging.NewSingleTargetMessage(target, messaging.SchemaStoreTopic, &messaging.SchemaStoreTableInfosRequest{
		RequestID:    reqID,
		KeyspaceID:   keyspaceMeta.ID,
		KeyspaceName: keyspaceMeta.Name,
		TableIDs:     tableIDs,
		Ts:           ts,
	}))
	if err != nil {
		return nil, err
	}

	tableInfosByID := make(map[int64]*common.TableInfo, len(tableIDs))
	for {
		select {
		case <-ctx.Done():
			return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
		case resp := <-respCh:
			if resp == nil {
				continue
			}
			if resp.Done {
				if resp.Error != "" {
					return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("%s", resp.Error)
				}
				result := make([]*common.TableInfo, 0, len(tableIDs))
				for _, tableID := range tableIDs {
					tableInfo, received := tableInfosByID[tableID]
					if !received {
						return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack(
							"incomplete schema store response: no result for table %d", tableID)
					}
					if tableInfo != nil {
						result = append(result, tableInfo)
					}
				}
				return result, nil
			}

			if resp.Error != "" {
				// An explicit table error accounts for this table; a missing response does not.
				tableInfosByID[resp.TableID] = nil
				log.Warn("get table info from schema store failed, ignore it",
					zap.Any("keyspace", keyspaceMeta),
					zap.Int64("tableID", resp.TableID),
					zap.Uint64("ts", ts),
					zap.String("error", resp.Error))
				continue
			}

			tableInfo, err := common.UnmarshalJSONToTableInfo(resp.TableInfo)
			if err != nil {
				return nil, errors.WrapError(errors.ErrUnmarshalFailed, err)
			}
			tableInfosByID[resp.TableID] = tableInfo
		}
	}
}

// RegisterKeyspace asks the schema store to register a keyspace. The context only
// controls waiting for the response; the store uses its own lifetime context.
func (c *Client) RegisterKeyspace(ctx context.Context, meta common.KeyspaceMeta) error {
	_, err := c.request(ctx, &messaging.SchemaStoreRequest{
		Operation: messaging.SchemaStoreRegisterKeyspace,
		Keyspace:  meta,
	})
	return err
}

// GetAllPhysicalTables fetches the filtered physical tables at the snapshot timestamp.
func (c *Client) GetAllPhysicalTables(ctx context.Context, meta common.KeyspaceMeta, ts uint64,
	filterConfig *config.FilterConfig, caseSensitive, forceReplicate bool,
) ([]commonEvent.Table, error) {
	resp, err := c.request(ctx, &messaging.SchemaStoreRequest{
		Operation:      messaging.SchemaStoreGetAllPhysicalTables,
		Keyspace:       meta,
		Ts:             ts,
		Filter:         filterConfig,
		CaseSensitive:  caseSensitive,
		ForceReplicate: forceReplicate,
	})
	if err != nil {
		return nil, err
	}
	return resp.Tables, nil
}

func (c *Client) request(ctx context.Context, req *messaging.SchemaStoreRequest) (*messaging.SchemaStoreResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, schemaStoreRequestTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, err)
	}
	target := node.ID(appcontext.GetID())
	if target.IsEmpty() {
		return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("server id is empty")
	}
	req.RequestID = c.nextRequestID.Add(1)
	ch := make(chan *messaging.SchemaStoreResponse, 1)
	c.requests.Store(req.RequestID, ch)
	defer c.requests.Delete(req.RequestID)
	if err := c.mc.SendCommand(messaging.NewSingleTargetMessage(target, messaging.SchemaStoreTopic, req)); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, errors.WrapError(errors.ErrSchemaStoreRequestFailed, ctx.Err())
	case resp := <-ch:
		if resp.Error != "" {
			if resp.ErrorCode != "" {
				// Reconstruct the original error code for API handling and retry classification.
				return nil, errors.Normalize(resp.Error, errors.RFCCodeText(resp.ErrorCode)).GenWithStackByArgs()
			}
			return nil, errors.ErrSchemaStoreRequestFailed.GenWithStack("%s", resp.Error)
		}
		return resp, nil
	}
}
