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

type Client struct {
	mc messaging.MessageCenter

	nextRequestID atomic.Uint64

	pendingMu sync.Mutex
	pending   map[uint64]chan *messaging.SchemaStoreTableInfosResponse
	requests  map[uint64]chan *messaging.SchemaStoreResponse
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

	c := &Client{
		mc:       mc,
		pending:  make(map[uint64]chan *messaging.SchemaStoreTableInfosResponse),
		requests: make(map[uint64]chan *messaging.SchemaStoreResponse),
	}
	c.mc.RegisterHandler(messaging.SchemaStoreClientTopic, c.handleMessage)
	schemaStoreClientInstance.Store(c)
	return c
}

func (c *Client) handleMessage(_ context.Context, msg *messaging.TargetMessage) error {
	for _, m := range msg.Message {
		if resp, ok := m.(*messaging.SchemaStoreResponse); ok {
			c.pendingMu.Lock()
			ch := c.requests[resp.RequestID]
			c.pendingMu.Unlock()
			if ch != nil {
				select {
				case ch <- resp:
				default:
				}
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

		c.pendingMu.Lock()
		ch, ok := c.pending[resp.RequestID]
		c.pendingMu.Unlock()
		if !ok {
			log.Debug("schema store response received but request already removed",
				zap.Uint64("requestID", resp.RequestID),
				zap.Int64("tableID", resp.TableID),
				zap.Bool("done", resp.Done))
			continue
		}

		select {
		case ch <- resp:
		default:
			log.Warn("schema store response channel is full, drop response",
				zap.Uint64("requestID", resp.RequestID),
				zap.Int64("tableID", resp.TableID),
				zap.Bool("done", resp.Done))
		}
	}
	return nil
}

func (c *Client) GetTableInfos(
	keyspaceMeta common.KeyspaceMeta,
	tableIDs []int64,
	ts uint64,
) ([]*common.TableInfo, error) {
	reqID := c.nextRequestID.Add(1)

	bufferSize := len(tableIDs) + 1
	if bufferSize < 8 {
		bufferSize = 8
	}
	if bufferSize > 4096 {
		bufferSize = 4096
	}

	respCh := make(chan *messaging.SchemaStoreTableInfosResponse, bufferSize)
	c.pendingMu.Lock()
	c.pending[reqID] = respCh
	c.pendingMu.Unlock()

	cleanup := func() {
		c.pendingMu.Lock()
		delete(c.pending, reqID)
		c.pendingMu.Unlock()
	}
	defer cleanup()

	target := node.ID(appcontext.GetID())
	if target.IsEmpty() {
		return nil, errors.New("server id is empty")
	}
	err := c.mc.SendCommand(messaging.NewSingleTargetMessage(target, messaging.SchemaStoreTopic, &messaging.SchemaStoreTableInfosRequest{
		RequestID:    reqID,
		KeyspaceID:   keyspaceMeta.ID,
		KeyspaceName: keyspaceMeta.Name,
		TableIDs:     tableIDs,
		Ts:           ts,
	}))
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), schemaStoreRequestTimeout)
	defer cancel()

	tableInfosByID := make(map[int64]*common.TableInfo, len(tableIDs))
	for {
		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case resp := <-respCh:
			if resp == nil {
				continue
			}
			if resp.Done {
				if resp.Error != "" {
					return nil, errors.New(resp.Error)
				}
				result := make([]*common.TableInfo, 0, len(tableIDs))
				for _, tableID := range tableIDs {
					if tableInfo, ok := tableInfosByID[tableID]; ok {
						result = append(result, tableInfo)
					}
				}
				return result, nil
			}

			if resp.Error != "" {
				log.Warn("get table info from schema store failed, ignore it",
					zap.Any("keyspace", keyspaceMeta),
					zap.Int64("tableID", resp.TableID),
					zap.Uint64("ts", ts),
					zap.String("error", resp.Error))
				continue
			}

			tableInfo, err := common.UnmarshalJSONToTableInfo(resp.TableInfo)
			if err != nil {
				return nil, errors.Trace(err)
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
	c.pendingMu.Lock()
	c.requests[req.RequestID] = ch
	c.pendingMu.Unlock()
	defer func() {
		c.pendingMu.Lock()
		delete(c.requests, req.RequestID)
		c.pendingMu.Unlock()
	}()
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
