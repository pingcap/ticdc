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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/mock"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/schemastore"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestGetSchemaStoreClient(t *testing.T) {
	mc := mock.NewMockMessageCenter(gomock.NewController(t))
	mc.EXPECT().RegisterHandler(messaging.SchemaStoreClientTopic, gomock.Any()).Times(1)
	previousMC, hasPreviousMC := appcontext.TryGetService[messaging.MessageCenter](appcontext.MessageCenter)
	previousID := appcontext.GetID()
	id := node.NewID()
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetID(id.String())
	t.Cleanup(func() {
		schemaStoreClient = nil
		schemaStoreClientOnce = sync.Once{}
		appcontext.SetID(previousID)
		if hasPreviousMC {
			appcontext.SetService(appcontext.MessageCenter, previousMC)
		}
	})

	const callers = 32
	clients := make(chan *Client, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() { clients <- GetSchemaStoreClient() })
	}
	wg.Wait()
	close(clients)
	c := GetSchemaStoreClient()
	require.Same(t, mc, c.mc)
	require.Equal(t, id, c.target)
	for got := range clients {
		require.Same(t, c, got)
	}
}

func newTestClient(t *testing.T) (*Client, messaging.MessageCenter) {
	t.Helper()
	id := node.NewID()
	mc := messaging.NewMessageCenter(t.Context(), id, config.NewDefaultMessageCenterConfig("127.0.0.1:0"), nil)
	mc.Run(t.Context())
	t.Cleanup(mc.Close)
	return New(mc, id), mc
}

func requireNoPendingRequests(t *testing.T, c *Client) {
	t.Helper()
	c.requests.Range(func(key, _ any) bool { t.Errorf("request %v was not cleaned up", key); return true })
}

func marshalTestTable(t *testing.T, id int64) []byte {
	t.Helper()
	info := common.WrapTableInfo("test", &model.TableInfo{ID: id, Name: ast.NewCIStr("t")})
	data, err := info.Marshal()
	require.NoError(t, err)
	return data
}

func TestSchemaStoreClientTableBatches(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "table limit", true: "byte limit"}[partial], func(t *testing.T) {
			mc := mock.NewMockMessageCenter(gomock.NewController(t))
			mc.EXPECT().RegisterHandler(messaging.SchemaStoreClientTopic, gomock.Any())
			c := New(mc, "test")
			const count = 5000
			ids := make([]int64, count)
			for i := range ids {
				ids[i] = int64(i + 1)
			}
			calls := 0
			mc.EXPECT().SendCommand(gomock.Any()).AnyTimes().DoAndReturn(func(msg *messaging.TargetMessage) error {
				req := msg.Message[0].(*schemastore.GetTableInfosRequest)
				require.LessOrEqual(t, len(req.TableIDs), schemastore.TableBatchSize)
				batch := req.TableIDs
				if partial {
					batch = batch[:min(len(batch), 31)]
				}
				resp := &schemastore.GetTableInfosResponse{RequestID: req.RequestID, More: len(batch) < len(req.TableIDs)}
				for _, id := range batch {
					resp.TableInfos = append(resp.TableInfos, schemastore.TableInfoResult{TableID: id, TableInfo: marshalTestTable(t, id)})
				}
				calls++
				return c.handleMessage(t.Context(), messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic, resp))
			})
			infos, err := c.GetTableInfos(t.Context(), common.DefaultKeyspace, ids, 100)
			require.NoError(t, err)
			require.Len(t, infos, count)
			for i, info := range infos {
				require.Equal(t, ids[i], info.TableName.TableID)
			}
			require.Greater(t, calls, 1)
			requireNoPendingRequests(t, c)
		})
	}
}

func TestSchemaStoreClientBatchErrors(t *testing.T) {
	first := schemastore.TableInfoResult{TableID: 1, TableInfo: marshalTestTable(t, 1)}
	second := schemastore.TableInfoResult{TableID: 2, TableInfo: marshalTestTable(t, 2)}
	tests := []struct {
		name      string
		response  *schemastore.GetTableInfosResponse
		wantError string
		wantCount int
	}{
		{name: "complete", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first, second}}, wantCount: 2},
		{name: "explicit table error", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first, {TableID: 2, Error: "table deleted"}}}, wantCount: 1},
		{name: "missing table", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first}}, wantError: "incomplete"},
		{name: "duplicate table", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first, first}}, wantError: "unexpected table"},
		{name: "empty continuation", response: &schemastore.GetTableInfosResponse{More: true}, wantError: "incomplete"},
		{name: "invalid continuation", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first, second}, More: true}, wantError: "incomplete"},
		{name: "decode error", response: &schemastore.GetTableInfosResponse{TableInfos: []schemastore.TableInfoResult{first, {TableID: 2, TableInfo: []byte("invalid")}}}, wantError: "Unmarshal"},
		{name: "server error", response: &schemastore.GetTableInfosResponse{Error: &schemastore.Error{Message: "delivery failed"}}, wantError: "delivery failed"},
		{name: "missing response", wantError: "deadline exceeded"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, mc := newTestClient(t)
			mc.RegisterHandler(messaging.SchemaStoreTopic, func(ctx context.Context, msg *messaging.TargetMessage) error {
				req := msg.Message[0].(*schemastore.GetTableInfosRequest)
				if tt.response == nil {
					return nil
				}
				resp := *tt.response
				resp.RequestID = req.RequestID
				return mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic, &resp))
			})
			ctx := t.Context()
			if tt.response == nil {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			infos, err := c.GetTableInfos(ctx, common.DefaultKeyspace, []int64{1, 2}, 100)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				require.Nil(t, infos)
			} else {
				require.NoError(t, err)
				require.Len(t, infos, tt.wantCount)
			}
			requireNoPendingRequests(t, c)
		})
	}
}

func TestSchemaStoreClientResponseDoesNotBlockRouter(t *testing.T) {
	c, mc := newTestClient(t)
	responses := make(chan response, 1)
	c.requests.Store(uint64(1), responses)
	otherDone := make(chan struct{})
	mc.RegisterHandler("unrelated-command", func(context.Context, *messaging.TargetMessage) error { close(otherDone); return nil })
	// Leave the first response unread. Neither a duplicate nor another topic
	// may wait for the caller to drain or decode it.
	msg := messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreClientTopic, &schemastore.GetTableInfosResponse{RequestID: 1})
	require.NoError(t, mc.SendCommand(msg))
	require.Eventually(t, func() bool { return len(responses) == 1 }, time.Second, time.Millisecond)
	require.NoError(t, mc.SendCommand(messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreClientTopic, &schemastore.GetTableInfosResponse{RequestID: 1})))
	require.NoError(t, mc.SendCommand(messaging.NewSingleTargetMessage(c.target, "unrelated-command", &schemastore.GetTableInfosResponse{RequestID: 2})))
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("schema response blocked the command router")
	}
	require.Len(t, responses, 1)
	requireNoPendingRequests(t, c)
}

func TestSchemaStoreClientConcurrentRequests(t *testing.T) {
	c, mc := newTestClient(t)
	cfg := config.NewDefaultFilterConfig()
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		req := msg.Message[0].(*schemastore.GetAllPhysicalTablesRequest)
		return mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic,
			&schemastore.GetAllPhysicalTablesResponse{RequestID: req.RequestID, Tables: []schemastore.PhysicalTable{{TableID: int64(req.Ts)}}}))
	})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			tables, err := c.GetAllPhysicalTables(t.Context(), common.DefaultKeyspace, uint64(i+1), cfg, true, true)
			require.NoError(t, err)
			require.Equal(t, []commonEvent.Table{{TableID: int64(i + 1)}}, tables)
		})
	}
	wg.Wait()
	requireNoPendingRequests(t, c)
	other, _ := newTestClient(t)
	require.NotSame(t, c, other)
	require.NotEqual(t, c.target, other.target)
}

func TestSchemaStoreClientRequestErrors(t *testing.T) {
	c, mc := newTestClient(t)
	remoteErr := errors.ErrKeyspaceNotFound.GenWithStackByArgs(123)
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		req := msg.Message[0].(*schemastore.GetTableInfosRequest)
		return mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic,
			&schemastore.GetTableInfosResponse{RequestID: req.RequestID, Error: schemastore.NewError(remoteErr)}))
	})
	_, err := c.GetTableInfos(t.Context(), common.DefaultKeyspace, []int64{1}, 100)
	require.True(t, errors.ErrKeyspaceNotFound.Equal(err))
	t.Run("cancellation only stops local wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		mc := mock.NewMockMessageCenter(gomock.NewController(t))
		mc.EXPECT().RegisterHandler(messaging.SchemaStoreClientTopic, gomock.Any())
		c := New(mc, "test")
		var requestID uint64
		mc.EXPECT().SendCommand(gomock.Any()).Times(1).DoAndReturn(func(msg *messaging.TargetMessage) error {
			req := msg.Message[0].(*schemastore.GetTableInfosRequest)
			requestID = req.RequestID
			cancel()
			return nil
		})
		_, err := c.GetTableInfos(ctx, common.DefaultKeyspace, []int64{1}, 100)
		require.ErrorIs(t, err, context.Canceled)
		requireNoPendingRequests(t, c)
		// A late response is discarded after the caller has stopped waiting.
		require.NoError(t, c.handleMessage(t.Context(), messaging.NewSingleTargetMessage(c.target, messaging.SchemaStoreClientTopic,
			&schemastore.GetTableInfosResponse{RequestID: requestID})))
		requireNoPendingRequests(t, c)
	})
	c.target = "missing-target"
	_, err = c.GetTableInfos(t.Context(), common.DefaultKeyspace, []int64{1}, 100)
	require.Error(t, err)
	requireNoPendingRequests(t, c)
}

func TestSchemaStoreClientUnexpectedResponse(t *testing.T) {
	for _, tableInfos := range []bool{true, false} {
		mc := mock.NewMockMessageCenter(gomock.NewController(t))
		mc.EXPECT().RegisterHandler(messaging.SchemaStoreClientTopic, gomock.Any())
		c := New(mc, "test")
		mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
			var resp messaging.IOTypeT
			if tableInfos {
				req := msg.Message[0].(*schemastore.GetTableInfosRequest)
				resp = &schemastore.GetAllPhysicalTablesResponse{RequestID: req.RequestID}
			} else {
				req := msg.Message[0].(*schemastore.GetAllPhysicalTablesRequest)
				resp = &schemastore.GetTableInfosResponse{RequestID: req.RequestID}
			}
			return c.handleMessage(t.Context(), messaging.NewSingleTargetMessage("test", messaging.SchemaStoreClientTopic, resp))
		})
		var err error
		if tableInfos {
			_, err = c.GetTableInfos(t.Context(), common.DefaultKeyspace, []int64{1}, 100)
		} else {
			_, err = c.GetAllPhysicalTables(t.Context(), common.DefaultKeyspace, 100, config.NewDefaultFilterConfig(), true, false)
		}
		require.ErrorContains(t, err, "unexpected response")
		requireNoPendingRequests(t, c)
	}
}
