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

package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	appctx "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type testLocalFencer struct {
	count atomic.Int32
}

func (f *testLocalFencer) LocalFence() {
	f.count.Add(1)
}

func TestSessionWatchdogFencesOnSessionDone(t *testing.T) {
	fencer := &testLocalFencer{}
	appctx.SetService(appctx.DispatcherOrchestrator, fencer)

	c := &server{}
	sessionDone := make(chan struct{})
	close(sessionDone)

	err := c.watchEtcdSession(context.Background(), sessionDone, 1, time.Hour)

	require.True(t, errors.ErrCaptureSuicide.Equal(err), err)
	require.Equal(t, int32(1), fencer.count.Load())
	require.Equal(t, liveness.CaptureStopping, c.liveness.Load())
}

func TestSessionWatchdogFencesOnExpiredLease(t *testing.T) {
	fencer := &testLocalFencer{}
	appctx.SetService(appctx.DispatcherOrchestrator, fencer)

	ctrl := gomock.NewController(t)
	cdcEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	rawEtcdClient := etcd.NewMockClient(ctrl)
	cdcEtcdClient.EXPECT().GetEtcdClient().Return(rawEtcdClient).AnyTimes()
	rawEtcdClient.EXPECT().
		TimeToLive(gomock.Any(), clientv3.LeaseID(100)).
		Return(&clientv3.LeaseTimeToLiveResponse{TTL: -1}, nil)

	c := &server{EtcdClient: cdcEtcdClient}
	sessionDone := make(chan struct{})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.watchEtcdSession(ctx, sessionDone, 100, time.Millisecond)

	require.True(t, errors.ErrCaptureSuicide.Equal(err), err)
	require.Equal(t, int32(1), fencer.count.Load())
	require.Equal(t, liveness.CaptureStopping, c.liveness.Load())
}

func TestSessionWatchdogRenewsEtcdWriteProof(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdcEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	rawEtcdClient := etcd.NewMockClient(ctrl)
	cdcEtcdClient.EXPECT().GetEtcdClient().Return(rawEtcdClient).AnyTimes()

	ctx, cancel := context.WithCancel(context.Background())
	rawEtcdClient.EXPECT().
		TimeToLive(gomock.Any(), clientv3.LeaseID(100)).
		DoAndReturn(func(context.Context, clientv3.LeaseID, ...clientv3.LeaseOption) (*clientv3.LeaseTimeToLiveResponse, error) {
			cancel()
			return &clientv3.LeaseTimeToLiveResponse{TTL: 10}, nil
		})

	gate := writelease.NewGate()
	requestSentAt := time.Now()
	require.True(t, gate.RenewP2P(requestSentAt, writelease.P2PLeaseDuration))
	c := &server{EtcdClient: cdcEtcdClient, writeGate: gate}

	err := c.watchEtcdSession(ctx, make(chan struct{}), 100, time.Millisecond)

	require.NoError(t, err)
	require.True(t, gate.IsWritable())
}

func TestLocalFenceIsIdempotent(t *testing.T) {
	fencer := &testLocalFencer{}
	appctx.SetService(appctx.DispatcherOrchestrator, fencer)

	c := &server{}
	c.localFence("first")
	c.localFence("second")

	require.Equal(t, int32(1), fencer.count.Load())
	require.Equal(t, liveness.CaptureStopping, c.liveness.Load())
}
