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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCaptureInfoIncludesWriteLeaseState(t *testing.T) {
	t.Parallel()

	c := &server{info: &node.Info{
		ID:             node.ID("capture-1"),
		AdvertiseAddr:  "127.0.0.1:8300",
		Version:        "v1.0.0",
		GitHash:        "git-hash",
		DeployPath:     "/tmp/cdc",
		StartTimestamp: 100,
	}}

	info := c.captureInfo(true)

	require.Equal(t, config.CaptureID(c.info.ID), info.ID)
	require.Equal(t, c.info.AdvertiseAddr, info.AdvertiseAddr)
	require.Equal(t, heartbeatpb.CurrentWriteLeaseProtocolVersion, info.WriteLeaseProtocolVersion)
	require.True(t, info.WriteStopped)
}

func TestMarkCaptureWriteStoppedUpdatesOnlyCurrentLease(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	cdcEtcdClient := etcd.NewMockCDCEtcdClient(ctrl)
	rawEtcdClient := etcd.NewMockClient(ctrl)
	leaseID := clientv3.LeaseID(100)
	captureID := node.ID("capture-1")
	key := etcd.GetEtcdKeyCaptureInfo(etcd.DefaultCDCClusterID, string(captureID))
	c := &server{
		info: &node.Info{
			ID:            captureID,
			AdvertiseAddr: "127.0.0.1:8300",
		},
		EtcdClient: cdcEtcdClient,
	}

	cdcEtcdClient.EXPECT().GetClusterID().Return(etcd.DefaultCDCClusterID)
	cdcEtcdClient.EXPECT().GetEtcdClient().Return(rawEtcdClient)
	rawEtcdClient.EXPECT().Txn(
		gomock.Any(),
		[]clientv3.Cmp{clientv3.Compare(clientv3.LeaseValue(key), "=", int64(leaseID))},
		gomock.Any(),
		etcd.TxnEmptyOpsElse,
	).DoAndReturn(func(
		_ context.Context,
		_ []clientv3.Cmp,
		opsThen []clientv3.Op,
		_ []clientv3.Op,
	) (*clientv3.TxnResponse, error) {
		require.Equal(t, []clientv3.Op{
			clientv3.OpPut(key, string(mustMarshalCaptureInfo(t, c.captureInfo(true))), clientv3.WithLease(leaseID)),
		}, opsThen)
		return &clientv3.TxnResponse{Succeeded: true}, nil
	})

	marked, err := c.markCaptureWriteStopped(t.Context(), leaseID)

	require.NoError(t, err)
	require.True(t, marked)
}

func mustMarshalCaptureInfo(t *testing.T, info *config.CaptureInfo) []byte {
	t.Helper()

	data, err := info.Marshal()
	require.NoError(t, err)
	return data
}
