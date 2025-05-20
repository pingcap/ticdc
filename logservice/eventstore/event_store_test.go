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

package eventstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
)

type mockSubscriptionStat struct {
	span    heartbeatpb.TableSpan
	startTs uint64
}

type mockSubscriptionClient struct {
	nextID        uint64
	subscriptions map[logpuller.SubscriptionID]*mockSubscriptionStat
}

func NewMockSubscriptionClient() logpuller.SubscriptionClient {
	return &mockSubscriptionClient{
		nextID:        0,
		subscriptions: make(map[logpuller.SubscriptionID]*mockSubscriptionStat),
	}
}

func (s *mockSubscriptionClient) Name() string {
	return "mockSubscriptionClient"
}

func (s *mockSubscriptionClient) Run(ctx context.Context) error {
	return nil
}

func (s *mockSubscriptionClient) Close(ctx context.Context) error {
	return nil
}

func (s *mockSubscriptionClient) AllocSubscriptionID() logpuller.SubscriptionID {
	s.nextID += 1
	return logpuller.SubscriptionID(s.nextID)
}

func (s *mockSubscriptionClient) Subscribe(
	subID logpuller.SubscriptionID,
	span heartbeatpb.TableSpan,
	startTs uint64,
	consumeKVEvents func(raw []common.RawKVEntry, wakeCallback func()) bool,
	advanceResolvedTs func(ts uint64),
	advanceInterval int64,
	bdrMode bool,
) {
	s.subscriptions[subID] = &mockSubscriptionStat{
		span:    span,
		startTs: startTs,
	}
}

func (s *mockSubscriptionClient) Unsubscribe(subID logpuller.SubscriptionID) {
	delete(s.subscriptions, subID)
}

func TestEventStoreRegisterDispatcher(t *testing.T) {
	ctx := context.Background()
	subClient := NewMockSubscriptionClient()
	eventStore := New(ctx, fmt.Sprintf("/tmp/%s", t.Name()), subClient, pdutil.NewClock4Test())

	// register a dispatcher
	{
		id := common.NewDispatcherID()
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("e"),
		}
		ok, err := eventStore.RegisterDispatcher(id, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
		require.Nil(t, err)
	}
	// register another dispatcher with the same span
	{
		id := common.NewDispatcherID()
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("e"),
		}
		ok, err := eventStore.RegisterDispatcher(id, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
		require.Nil(t, err)
	}
	// check two dispatchers can reuse the same subscription
	{
		require.Equal(t, 1, len(subClient.(*mockSubscriptionClient).subscriptions))
	}
	// register another dispatcher
	{
		id := common.NewDispatcherID()
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("b"),
		}
		ok, err := eventStore.RegisterDispatcher(id, span, 100, func(watermark uint64, latestCommitTs uint64) {}, false, false)
		require.True(t, ok)
		require.Nil(t, err)
	}
	// check a new subscription is created
	{
		require.Equal(t, 2, len(subClient.(*mockSubscriptionClient).subscriptions))
	}
	// register another dispatcher with onlyReuse set to true
	{
		id := common.NewDispatcherID()
		span := &heartbeatpb.TableSpan{
			TableID:  1,
			StartKey: []byte("a"),
			EndKey:   []byte("c"),
		}
		ok, err := eventStore.RegisterDispatcher(id, span, 100, func(watermark uint64, latestCommitTs uint64) {}, true, false)
		require.True(t, ok)
		require.Nil(t, err)
	}
	// check no new subscription is created
	{
		require.Equal(t, 2, len(subClient.(*mockSubscriptionClient).subscriptions))
	}
}
