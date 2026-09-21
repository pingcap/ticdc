// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package changefeed

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCheckpointPreservesSnapshotAcrossConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdc := etcd.NewMockCDCEtcdClient(ctrl)
	raw := etcd.NewMockClient(ctrl)
	cdc.EXPECT().GetEtcdClient().Return(raw).AnyTimes()
	cdc.EXPECT().GetClusterID().Return("test").AnyTimes()
	backend := NewEtcdBackend(cdc)
	id := common.NewChangeFeedIDWithName("demo", common.DefaultKeyspaceName)
	first := &config.ChangeFeedStatus{CheckpointTs: 42, Snapshot: &protocol.State{Generation: "g", Phase: "APPLYING"}}
	marker := &protocol.Marker{SnapshotTS: 42, SnapshotID: "s", PlanDigest: "p"}
	second := &config.ChangeFeedStatus{CheckpointTs: 42, Progress: config.ProgressStopping, Snapshot: &protocol.State{Generation: "g", Phase: "SNAPSHOT_COMMITTED", Marker: marker}}
	gomock.InOrder(cdc.EXPECT().GetChangeFeedStatus(gomock.Any(), id).Return(first, int64(1), nil), raw.EXPECT().Txn(gomock.Any(), gomock.Len(1), gomock.Any(), gomock.Any()).Return(&clientv3.TxnResponse{}, nil), cdc.EXPECT().GetChangeFeedStatus(gomock.Any(), id).Return(second, int64(2), nil), raw.EXPECT().Txn(gomock.Any(), gomock.Len(1), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ []clientv3.Cmp, ops []clientv3.Op, _ []clientv3.Op) (*clientv3.TxnResponse, error) {
		var s config.ChangeFeedStatus
		require.NoError(t, s.Unmarshal(ops[0].ValueBytes()))
		require.Equal(t, uint64(100), s.CheckpointTs)
		require.Equal(t, config.ProgressStopping, s.Progress)
		require.Equal(t, marker, s.Snapshot.Marker)
		return &clientv3.TxnResponse{Succeeded: true}, nil
	}))
	require.NoError(t, backend.UpdateChangefeedCheckpointTs(context.Background(), map[common.ChangeFeedID]uint64{id: 100}))
}

func TestSnapshotRejectsOldOwnerAndMarkerRollback(t *testing.T) {
	ctrl := gomock.NewController(t)
	cdc := etcd.NewMockCDCEtcdClient(ctrl)
	raw := etcd.NewMockClient(ctrl)
	cdc.EXPECT().GetEtcdClient().Return(raw).AnyTimes()
	cdc.EXPECT().GetClusterID().Return("test").AnyTimes()
	backend := NewEtcdBackend(cdc)
	id := common.NewChangeFeedIDWithName("demo", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{ChangefeedID: id, Epoch: 2, State: config.StateNormal}
	b, e := info.Marshal()
	require.NoError(t, e)
	raw.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Value: []byte(b), ModRevision: 7}}}, nil).Times(2)
	require.ErrorContains(t, backend.SaveSnapshot(context.Background(), id, 1, &protocol.State{Generation: "g"}), "owner")
	old := &protocol.State{Generation: "g", SnapshotID: "s", SnapshotTS: 42, Phase: "SNAPSHOT_COMMITTED", Marker: &protocol.Marker{SnapshotTS: 42}}
	cdc.EXPECT().GetChangeFeedStatus(gomock.Any(), id).Return(&config.ChangeFeedStatus{Snapshot: old}, int64(9), nil)
	require.ErrorContains(t, backend.SaveSnapshot(context.Background(), id, 2, &protocol.State{Generation: "g", SnapshotID: "s", SnapshotTS: 42}), "roll back")
}
