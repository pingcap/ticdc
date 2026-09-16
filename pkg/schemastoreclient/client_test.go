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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestSchemaStoreClientFullResponseBuffer(t *testing.T) {
	for _, done := range []bool{false, true} {
		name := "table response"
		if done {
			name = "completion"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			responses := make(chan *messaging.SchemaStoreTableInfosResponse, 4096)
			for range cap(responses) {
				responses <- &messaging.SchemaStoreTableInfosResponse{RequestID: 1}
			}
			client := &Client{}
			client.pending.Store(uint64(1), &tableInfosRequest{ctx: ctx, responses: responses})
			extra := &messaging.SchemaStoreTableInfosResponse{RequestID: 1, TableID: 4097, Done: done}
			handled := make(chan error, 1)
			go func() {
				handled <- client.handleMessage(ctx, messaging.NewSingleTargetMessage("test",
					messaging.SchemaStoreClientTopic, extra))
			}()
			select {
			case err := <-handled:
				t.Fatalf("handler discarded a response from a full buffer: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			// Free one slot so the handler can deliver the response without dropping it.
			<-responses
			select {
			case err := <-handled:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("handler did not resume after the buffer was drained")
			}
			for range cap(responses) - 1 {
				<-responses
			}
			require.Same(t, extra, <-responses)
		})
	}

	t.Run("cancellation releases blocked handler", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		responses := make(chan *messaging.SchemaStoreTableInfosResponse, 1)
		responses <- &messaging.SchemaStoreTableInfosResponse{RequestID: 1}
		client := &Client{}
		client.pending.Store(uint64(1), &tableInfosRequest{ctx: ctx, responses: responses})
		handled := make(chan error, 1)
		go func() {
			handled <- client.handleMessage(context.Background(), messaging.NewSingleTargetMessage("test",
				messaging.SchemaStoreClientTopic, &messaging.SchemaStoreTableInfosResponse{RequestID: 1, Done: true}))
		}()
		cancel()
		select {
		case err := <-handled:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("handler remained blocked after the request was canceled")
		}
	})
}

func TestSchemaStoreClientTableInfosCompletion(t *testing.T) {
	previousID := appcontext.GetID()
	appcontext.SetID("test")
	t.Cleanup(func() { appcontext.SetID(previousID) })
	tableInfos := make(map[int64][]byte)
	for _, id := range []int64{1, 2} {
		info := common.WrapTableInfo("test", &model.TableInfo{ID: id, Name: ast.NewCIStr("t")})
		data, err := info.Marshal()
		require.NoError(t, err)
		tableInfos[id] = data
	}
	tests := []struct {
		name      string
		responses []*messaging.SchemaStoreTableInfosResponse
		wantIDs   []int64
		wantError string
		noDone    bool
	}{
		{
			name: "all tables in request order",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 2, TableInfo: tableInfos[2]}, {TableID: 1, TableInfo: tableInfos[1]}, {Done: true},
			},
			wantIDs: []int64{1, 2},
		},
		{
			name: "explicit table error accounts for a table",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 1, TableInfo: tableInfos[1]}, {TableID: 2, Error: "table dropped"}, {Done: true},
			},
			wantIDs: []int64{1},
		},
		{
			name: "missing table response",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 1, TableInfo: tableInfos[1]}, {Done: true},
			},
			wantError: "no result for table 2",
		},
		{
			name: "duplicate response cannot hide missing table",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 1, TableInfo: tableInfos[1]}, {TableID: 1, TableInfo: tableInfos[1]}, {Done: true},
			},
			wantError: "no result for table 2",
		},
		{
			name: "failed server delivery",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 1, TableInfo: tableInfos[1]}, {Done: true, Error: "response delivery failed"},
			},
			wantError: "response delivery failed",
		},
		{
			name: "missing completion",
			responses: []*messaging.SchemaStoreTableInfosResponse{
				{TableID: 1, TableInfo: tableInfos[1]}, {TableID: 2, TableInfo: tableInfos[2]},
			},
			noDone: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := mock.NewMockMessageCenter(gomock.NewController(t))
			client := &Client{mc: mc}
			mc.EXPECT().SendCommand(gomock.Any()).DoAndReturn(func(msg *messaging.TargetMessage) error {
				req := msg.Message[0].(*messaging.SchemaStoreTableInfosRequest)
				for _, resp := range tt.responses {
					resp.RequestID = req.RequestID
					require.NoError(t, client.handleMessage(context.Background(), messaging.NewSingleTargetMessage(
						msg.From, messaging.SchemaStoreClientTopic, resp)))
				}
				return nil
			})
			ctx := context.Background()
			if tt.noDone {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			infos, err := client.GetTableInfos(ctx, common.DefaultKeyspace, []int64{1, 2}, 100)
			switch {
			case tt.noDone:
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Nil(t, infos)
			case tt.wantError != "":
				require.ErrorContains(t, err, tt.wantError)
				require.True(t, errors.ErrSchemaStoreRequestFailed.Equal(err))
				require.Nil(t, infos)
			default:
				require.NoError(t, err)
				var ids []int64
				for _, info := range infos {
					ids = append(ids, info.TableName.TableID)
				}
				require.Equal(t, tt.wantIDs, ids)
			}
			requireNoPendingRequests(t, client)
		})
	}
}

func requireNoPendingRequests(t *testing.T, client *Client) {
	t.Helper()
	for _, requests := range []*sync.Map{&client.pending, &client.requests} {
		requests.Range(func(key, _ any) bool {
			t.Errorf("request %v was not cleaned up", key)
			return true
		})
	}
}

func TestSchemaStoreClientDuplicateResponse(t *testing.T) {
	client := &Client{}
	responses := make(chan *messaging.SchemaStoreResponse, 1)
	resp := &messaging.SchemaStoreResponse{RequestID: 1}
	client.requests.Store(resp.RequestID, responses)
	msg := messaging.NewSingleTargetMessage("test", messaging.SchemaStoreClientTopic, resp)
	require.NoError(t, client.handleMessage(context.Background(), msg))
	require.Same(t, resp, <-responses)

	// Even after the first response is consumed, duplicates must not be delivered
	// again while the caller is finishing the request.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			require.NoError(t, client.handleMessage(context.Background(), msg))
		})
	}
	wg.Wait()
	require.Empty(t, responses)
	requireNoPendingRequests(t, client)
}

func newTestClient(t *testing.T) (*Client, messaging.MessageCenter) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	id := node.NewID()
	previousID := appcontext.GetID()
	appcontext.SetID(id.String())
	t.Cleanup(func() { appcontext.SetID(previousID) })
	mc := messaging.NewMessageCenter(ctx, id, config.NewDefaultMessageCenterConfig("127.0.0.1:0"), nil)
	mc.Run(ctx)
	t.Cleanup(mc.Close)
	appcontext.SetService(appcontext.MessageCenter, mc)
	return GetSchemaStoreClient(), mc
}

func TestSchemaStoreClientConcurrentRequests(t *testing.T) {
	client, mc := newTestClient(t)
	cfg := config.NewDefaultFilterConfig()
	cfg.Rules = []string{"test.*"}
	requests := make(chan *messaging.SchemaStoreRequest, 16)
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		req := msg.Message[0].(*messaging.SchemaStoreRequest)
		requests <- req
		return mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic,
			&messaging.SchemaStoreResponse{RequestID: req.RequestID, Tables: []commonEvent.Table{{TableID: int64(req.Ts)}}}))
	})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			require.Same(t, client, GetSchemaStoreClient())
			tables, err := client.GetAllPhysicalTables(context.Background(), common.DefaultKeyspace, uint64(i+1), cfg, true, true)
			require.NoError(t, err)
			require.Equal(t, []commonEvent.Table{{TableID: int64(i + 1)}}, tables)
		})
	}
	wg.Wait()
	require.NoError(t, client.RegisterKeyspace(context.Background(), common.DefaultKeyspace))
	ids := make(map[uint64]bool)
	for range 9 {
		req := <-requests
		require.False(t, ids[req.RequestID])
		ids[req.RequestID] = true
		require.Equal(t, common.DefaultKeyspace, req.Keyspace)
		if req.Operation == messaging.SchemaStoreGetAllPhysicalTables {
			require.Equal(t, cfg, req.Filter)
			require.True(t, req.CaseSensitive)
			require.True(t, req.ForceReplicate)
		}
	}
	requireNoPendingRequests(t, client)
	next, _ := newTestClient(t)
	require.NotSame(t, client, next)
}

func TestSchemaStoreClientRequestErrors(t *testing.T) {
	client, mc := newTestClient(t)
	remoteErr := errors.ErrKeyspaceNotFound.GenWithStackByArgs(123)
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		req := msg.Message[0].(*messaging.SchemaStoreRequest)
		return mc.SendCommand(messaging.NewSingleTargetMessage(msg.From, messaging.SchemaStoreClientTopic,
			&messaging.SchemaStoreResponse{RequestID: req.RequestID, Error: remoteErr.Error(), ErrorCode: string(errors.ErrKeyspaceNotFound.RFCCode())}))
	})
	require.True(t, errors.ErrKeyspaceNotFound.Equal(client.RegisterKeyspace(context.Background(), common.DefaultKeyspace)))
	requireNoPendingRequests(t, client)

	ctx, cancel := context.WithCancel(context.Background())
	requestSeen := make(chan uint64, 1)
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		requestSeen <- msg.Message[0].(*messaging.SchemaStoreRequest).RequestID
		cancel()
		return nil
	})
	require.ErrorIs(t, client.RegisterKeyspace(ctx, common.DefaultKeyspace), context.Canceled)
	requireNoPendingRequests(t, client)
	// A response arriving after cancellation must not block the message handler.
	require.NoError(t, client.handleMessage(context.Background(), messaging.NewSingleTargetMessage(
		node.ID(appcontext.GetID()), messaging.SchemaStoreClientTopic,
		&messaging.SchemaStoreResponse{RequestID: <-requestSeen})))

	expired, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	require.ErrorIs(t, client.RegisterKeyspace(expired, common.DefaultKeyspace), context.DeadlineExceeded)
	require.Empty(t, requestSeen)
	appcontext.SetID("missing-target")
	require.Error(t, client.RegisterKeyspace(context.Background(), common.DefaultKeyspace))
	requireNoPendingRequests(t, client)
}
