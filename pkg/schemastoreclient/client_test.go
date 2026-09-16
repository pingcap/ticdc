// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

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
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Same(t, client, GetSchemaStoreClient())
			tables, err := client.GetAllPhysicalTables(context.Background(), common.DefaultKeyspace, uint64(i+1), cfg, true, true)
			require.NoError(t, err)
			require.Equal(t, []commonEvent.Table{{TableID: int64(i + 1)}}, tables)
		}()
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
	require.Empty(t, client.requests)
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
	require.Empty(t, client.requests)

	ctx, cancel := context.WithCancel(context.Background())
	requestSeen := make(chan uint64, 1)
	mc.RegisterHandler(messaging.SchemaStoreTopic, func(_ context.Context, msg *messaging.TargetMessage) error {
		requestSeen <- msg.Message[0].(*messaging.SchemaStoreRequest).RequestID
		cancel()
		return nil
	})
	require.ErrorIs(t, client.RegisterKeyspace(ctx, common.DefaultKeyspace), context.Canceled)
	require.Empty(t, client.requests)
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
	require.Empty(t, client.requests)
}
