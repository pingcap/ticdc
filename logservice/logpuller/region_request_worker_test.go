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

package logpuller

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"google.golang.org/grpc/metadata"
)

func TestRegionStatesOperation(t *testing.T) {
	worker := &regionRequestWorker{}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	require.Nil(t, worker.getRegionState(1, 2))
	require.Nil(t, worker.takeRegionState(1, 2))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))

	worker.addRegionState(1, 2, &regionFeedState{})
	require.NotNil(t, worker.getRegionState(1, 2))
	require.NotNil(t, worker.takeRegionState(1, 2))
	require.Nil(t, worker.getRegionState(1, 2))
	require.Equal(t, 0, len(worker.requestedRegions.subscriptions))
}

type fakeEventFeedV2Client struct {
	sendErr  error
	sendHook func(*cdcpb.ChangeDataRequest)
	ctx      context.Context
}

func (c *fakeEventFeedV2Client) Send(req *cdcpb.ChangeDataRequest) error {
	if c.sendHook != nil {
		c.sendHook(req)
	}
	return c.sendErr
}

func (c *fakeEventFeedV2Client) Recv() (*cdcpb.ChangeDataEvent, error) { return nil, io.EOF }

func (c *fakeEventFeedV2Client) Header() (metadata.MD, error) { return nil, nil }
func (c *fakeEventFeedV2Client) Trailer() metadata.MD         { return nil }
func (c *fakeEventFeedV2Client) CloseSend() error             { return nil }
func (c *fakeEventFeedV2Client) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}
func (c *fakeEventFeedV2Client) SendMsg(m any) error { return nil }
func (c *fakeEventFeedV2Client) RecvMsg(m any) error { return nil }

func TestRegionRequestWorkerSendErrorDoesNotLeakPendingCount(t *testing.T) {
	t.Parallel()

	subSpan := &subscribedSpan{subID: 1}
	region := newRegionInfo(
		tikv.NewRegionVerID(100, 1, 1),
		heartbeatpb.TableSpan{StartKey: []byte("a"), EndKey: []byte("b")},
		&tikv.RPCContext{
			Addr: "store-1",
			Meta: &metapb.Region{RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1}},
		},
		subSpan,
		false,
	)
	region.lockedRangeState = &regionlock.LockedRangeState{}

	worker := &regionRequestWorker{
		workerID:              1,
		client:                &subscriptionClient{clusterID: 1},
		store:                 &requestedStore{storeAddr: "store-1"},
		preFetchForConnecting: &region,
		requestCache:          newRequestCache(16),
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	worker.requestCache.pendingCount.Store(1)

	conn := &ConnAndClient{
		Client: &fakeEventFeedV2Client{sendErr: errors.New("send failed")},
	}

	err := worker.processRegionSendTask(context.Background(), conn)
	require.Error(t, err)
	require.Equal(t, int64(0), worker.requestCache.pendingCount.Load())
}

func TestRegionRequestWorkerStopTaskDoesNotLeakPendingCount(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendCalled := make(chan struct{})
	conn := &ConnAndClient{
		Client: &fakeEventFeedV2Client{
			sendHook: func(*cdcpb.ChangeDataRequest) {
				select {
				case <-sendCalled:
				default:
					close(sendCalled)
				}
				cancel()
			},
		},
	}

	stopRegion := regionInfo{
		subscribedSpan:   &subscribedSpan{subID: 1},
		lockedRangeState: nil,
	}

	worker := &regionRequestWorker{
		workerID:              1,
		client:                &subscriptionClient{clusterID: 1},
		store:                 &requestedStore{storeAddr: "store-1"},
		preFetchForConnecting: &stopRegion,
		requestCache:          newRequestCache(16),
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	worker.requestCache.pendingCount.Store(1)

	errCh := make(chan error, 1)
	go func() {
		errCh <- worker.processRegionSendTask(ctx, conn)
	}()

	select {
	case <-sendCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("send is not called in time")
	}

	err := <-errCh
	require.Error(t, err)
	require.Equal(t, int64(0), worker.requestCache.pendingCount.Load())
}
