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

package etcd

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestExtractChangefeedKeySuffix(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name             string
		args             args
		wantKs           string
		wantCf           string
		wantIsStatus     bool
		wantIsChangefeed bool
	}{
		{
			name: "an empty key",
			args: args{
				key: "",
			},
			wantIsChangefeed: false,
		},
		{
			name: "an invalid key",
			args: args{
				key: "foobar",
			},
			wantIsChangefeed: false,
		},
		{
			name: "an slash key",
			args: args{
				key: "/",
			},
			wantIsChangefeed: false,
		},
		{
			name: "3 parts",
			args: args{
				key: "/tidb/cdc/default",
			},
			wantIsChangefeed: false,
		},
		{
			name: "not a changefeed",
			args: args{
				key: "/tidb/cdc/default/keyspace1/foobar/info/hello",
			},
			wantIsChangefeed: false,
		},
		{
			name: "a changefeed info",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/info/hello",
			},
			wantKs:           "keyspace1",
			wantCf:           "hello",
			wantIsStatus:     false,
			wantIsChangefeed: true,
		},
		{
			name: "a changefeed status",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/status/hello",
			},
			wantKs:           "keyspace1",
			wantCf:           "hello",
			wantIsStatus:     true,
			wantIsChangefeed: true,
		},
		{
			name: "an invalid changefeed status",
			args: args{
				key: "/tidb/cdc/default/keyspace1/changefeed/status",
			},
			wantIsChangefeed: false,
		},
		{
			name: "capture info",
			args: args{
				key: "/tidb/cdc/default/__cdc_meta__/capture/786afb7b-c780-48df-8fb6-567d4647c007",
			},
			wantIsChangefeed: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKs, gotCf, gotIsStatus, gotIsChangefeed := extractChangefeedKeySuffix(tt.args.key)
			require.Equal(t, tt.wantKs, gotKs)
			require.Equal(t, tt.wantCf, gotCf)
			require.Equal(t, tt.wantIsStatus, gotIsStatus)
			require.Equal(t, tt.wantIsChangefeed, gotIsChangefeed)
		})
	}
}

func TestCDCEtcdClientImpl_GetChangefeedInfoAndStatus(t *testing.T) {
	type fields struct {
		Client        Client
		ClusterID     string
		etcdClusterID uint64
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name          string
		fields        func(ctx context.Context, ctrl *gomock.Controller) fields
		args          args
		wantRevision  int64
		wantStatusMap map[common.ChangeFeedDisplayName]*mvccpb.KeyValue
		wantInfoMap   map[common.ChangeFeedDisplayName]*mvccpb.KeyValue
		assertion     require.ErrorAssertionFunc
	}{
		{
			name: "get changefeeds failed",
			fields: func(ctx context.Context, ctrl *gomock.Controller) fields {
				client := NewMockClient(ctrl)
				client.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("/tidb/cdc/cluster-id/"), gomock.Any()).Return(nil, errors.New("etcd failed")).Times(1)
				return fields{
					Client:        client,
					ClusterID:     "cluster-id",
					etcdClusterID: uint64(1),
				}
			},
			args: args{
				ctx: context.Background(),
			},
			wantRevision:  int64(0),
			wantStatusMap: nil,
			wantInfoMap:   nil,
			assertion: func(t require.TestingT, err error, opts ...any) {
				require.ErrorContains(t, err, "etcd failed")
			},
		},
		{
			name: "get changefeeds success",
			fields: func(ctx context.Context, ctrl *gomock.Controller) fields {
				client := NewMockClient(ctrl)
				client.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("/tidb/cdc/cluster-id/"), gomock.Any()).Return(&clientv3.GetResponse{
					Header: &etcdserverpb.ResponseHeader{
						Revision: 3,
					},
					Kvs: []*mvccpb.KeyValue{
						{
							Key:   []byte("/invalid/key"),
							Value: []byte("/invalid/value"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed/status/changefeed1"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed/info/changefeed1"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace2/changefeed/status/changefeed2"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace3/changefeed/status/changefeed2"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace3/changefeed/info/changefeed3"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace4/changefeed/info/changefeed3"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed1/status/changefeed2"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed1/info/changefeed2"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id-index/keyspace1/changefeed/status/changefeed4"),
							Value: []byte("{}"),
						},
						{
							Key:   []byte("/tidb/cdc/cluster-id-index/keyspace1/changefeed/info/changefeed4"),
							Value: []byte("{}"),
						},
					},
					More:  false,
					Count: 0,
				}, nil).Times(1)
				return fields{
					Client:        client,
					ClusterID:     "cluster-id",
					etcdClusterID: uint64(1),
				}
			},
			args: args{
				ctx: context.Background(),
			},
			wantRevision: int64(3),
			wantStatusMap: map[common.ChangeFeedDisplayName]*mvccpb.KeyValue{
				{Name: "changefeed1", Keyspace: "keyspace1"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed/status/changefeed1"),
					Value: []byte("{}"),
				},
				{Name: "changefeed2", Keyspace: "keyspace2"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace2/changefeed/status/changefeed2"),
					Value: []byte("{}"),
				},
				{Name: "changefeed2", Keyspace: "keyspace3"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace3/changefeed/status/changefeed2"),
					Value: []byte("{}"),
				},
			},
			wantInfoMap: map[common.ChangeFeedDisplayName]*mvccpb.KeyValue{
				{Name: "changefeed1", Keyspace: "keyspace1"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace1/changefeed/info/changefeed1"),
					Value: []byte("{}"),
				},
				{Name: "changefeed3", Keyspace: "keyspace3"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace3/changefeed/info/changefeed3"),
					Value: []byte("{}"),
				},
				{Name: "changefeed3", Keyspace: "keyspace4"}: {
					Key:   []byte("/tidb/cdc/cluster-id/keyspace4/changefeed/info/changefeed3"),
					Value: []byte("{}"),
				},
			},
			assertion: func(t require.TestingT, err error, opts ...any) {
				require.NoError(t, err)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			fields := tt.fields(tt.args.ctx, ctrl)
			c := &CDCEtcdClientImpl{
				Client:        fields.Client,
				ClusterID:     fields.ClusterID,
				etcdClusterID: fields.etcdClusterID,
			}

			gotRevision, gotStatusMap, gotInfoMap, err := c.GetChangefeedInfoAndStatus(tt.args.ctx)
			tt.assertion(t, err)
			require.Equal(t, tt.wantRevision, gotRevision)
			require.EqualValues(t, tt.wantStatusMap, gotStatusMap)
			require.EqualValues(t, tt.wantInfoMap, gotInfoMap)
		})
	}
}

func TestCDCEtcdClientImpl_GetAllCDCInfo(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockClient(ctrl)
	client.EXPECT().
		Get(gomock.Eq(ctx), gomock.Eq("/tidb/cdc/cluster-id/"), gomock.Any()).
		Return(&clientv3.GetResponse{
			Kvs: []*mvccpb.KeyValue{
				{
					Key:   []byte("/tidb/cdc/cluster-id/default/changefeed/info/changefeed1"),
					Value: []byte("{}"),
				},
				{
					Key:   []byte("/tidb/cdc/cluster-id-index/default/changefeed/info/changefeed2"),
					Value: []byte("{}"),
				},
			},
		}, nil).
		Times(1)

	etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: "cluster-id"}
	kvs, err := etcdClient.GetAllCDCInfo(ctx)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("/tidb/cdc/cluster-id/default/changefeed/info/changefeed1"), kvs[0].Key)
}

func TestCDCEtcdClientImpl_ClearAllCDCInfo(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := NewMockClient(ctrl)
	client.EXPECT().
		Delete(gomock.Eq(ctx), gomock.Eq("/tidb/cdc/cluster-id/"), gomock.Any()).
		Return(&clientv3.DeleteResponse{}, nil).
		Times(1)

	etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: "cluster-id"}
	require.NoError(t, etcdClient.ClearAllCDCInfo(ctx))
}

func TestCDCEtcdClientImpl_CheckMultipleCDCClusterExist(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		kvs       []*mvccpb.KeyValue
		assertion require.ErrorAssertionFunc
	}{
		{
			name: "only default cluster exists",
			kvs: []*mvccpb.KeyValue{
				{Key: []byte("/tidb/cdc/default/default/changefeed/info/changefeed1")},
			},
			assertion: require.NoError,
		},
		{
			name: "default prefix collision should be treated as another cluster",
			kvs: []*mvccpb.KeyValue{
				{Key: []byte("/tidb/cdc/default-index/default/changefeed/info/changefeed1")},
			},
			assertion: func(t require.TestingT, err error, _ ...any) {
				require.True(t, cerrors.ErrMultipleCDCClustersExist.Equal(err))
			},
		},
		{
			name: "reserved prefix collision should be treated as another cluster",
			kvs: []*mvccpb.KeyValue{
				{Key: []byte("/tidb/cdc/owner-index/default/changefeed/info/changefeed1")},
			},
			assertion: func(t require.TestingT, err error, _ ...any) {
				require.True(t, cerrors.ErrMultipleCDCClustersExist.Equal(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := NewMockClient(ctrl)
			client.EXPECT().
				Get(gomock.Eq(ctx), gomock.Eq("/tidb/cdc/"), gomock.Any(), gomock.Any()).
				Return(&clientv3.GetResponse{Kvs: tt.kvs}, nil).
				Times(1)

			etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: "cluster-id"}
			err := etcdClient.CheckMultipleCDCClusterExist(ctx)
			tt.assertion(t, err)
		})
	}
}

func TestCDCEtcdClientImpl_GetLogCoordinatorRevision(t *testing.T) {
	ctx := context.Background()
	clusterID := "cluster-id"
	captureID := "capture-1"

	t.Run("get leader failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockClient(ctrl)
		client.EXPECT().
			Get(gomock.Eq(ctx), gomock.Eq(LogCoordinatorKey(clusterID)), gomock.Any()).
			Return(nil, errors.New("etcd failed")).
			Times(1)

		etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: clusterID}
		_, err := etcdClient.GetLogCoordinatorRevision(ctx, captureID)
		require.ErrorContains(t, err, "etcd failed")
	})

	t.Run("leader not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockClient(ctrl)
		client.EXPECT().
			Get(gomock.Eq(ctx), gomock.Eq(LogCoordinatorKey(clusterID)), gomock.Any()).
			Return(&clientv3.GetResponse{Kvs: nil}, nil).
			Times(1)

		etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: clusterID}
		_, err := etcdClient.GetLogCoordinatorRevision(ctx, captureID)
		require.True(t, cerrors.ErrOwnerNotFound.Equal(err))
	})

	t.Run("not the log coordinator", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockClient(ctrl)
		client.EXPECT().
			Get(gomock.Eq(ctx), gomock.Eq(LogCoordinatorKey(clusterID)), gomock.Any()).
			Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{Value: []byte("other-capture"), ModRevision: 99},
				},
			}, nil).
			Times(1)

		etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: clusterID}
		_, err := etcdClient.GetLogCoordinatorRevision(ctx, captureID)
		require.True(t, cerrors.ErrNotOwner.Equal(err))
	})

	t.Run("success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockClient(ctrl)
		client.EXPECT().
			Get(gomock.Eq(ctx), gomock.Eq(LogCoordinatorKey(clusterID)), gomock.Any()).
			Return(&clientv3.GetResponse{
				Kvs: []*mvccpb.KeyValue{
					{Value: []byte(captureID), ModRevision: 123},
				},
			}, nil).
			Times(1)

		etcdClient := &CDCEtcdClientImpl{Client: client, ClusterID: clusterID}
		rev, err := etcdClient.GetLogCoordinatorRevision(ctx, captureID)
		require.NoError(t, err)
		require.Equal(t, int64(123), rev)
	})
}
