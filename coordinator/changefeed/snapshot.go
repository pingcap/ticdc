// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package changefeed

import (
	"context"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (b *EtcdBackend) LoadSnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64) (*protocol.State, error) {
	info, e := b.etcdClient.GetChangeFeedInfo(ctx, id.DisplayName)
	if e != nil {
		return nil, protocol.Wrap(e)
	}
	if info.Epoch != epoch {
		return nil, protocol.Invalid("stale snapshot owner")
	}
	status, _, e := b.etcdClient.GetChangeFeedStatus(ctx, id)
	if e != nil {
		return nil, protocol.Wrap(e)
	}
	return status.Snapshot, nil
}

func (b *EtcdBackend) SaveSnapshot(ctx context.Context, id common.ChangeFeedID, epoch uint64, next *protocol.State) error {
	infoKey := etcd.GetEtcdKeyChangeFeedInfo(b.etcdClient.GetClusterID(), id.DisplayName)
	statusKey := etcd.GetEtcdKeyJob(b.etcdClient.GetClusterID(), id.DisplayName)
	for retry := 0; retry < 20; retry++ {
		ir, e := b.etcdClient.GetEtcdClient().Get(ctx, infoKey)
		if e != nil {
			return protocol.Wrap(e)
		}
		if len(ir.Kvs) != 1 {
			return protocol.Invalid("changefeed disappeared")
		}
		var info config.ChangeFeedInfo
		if e = info.Unmarshal(ir.Kvs[0].Value); e != nil {
			return e
		}
		if info.Epoch != epoch || info.ChangefeedID != id || !info.State.IsRunning() {
			return protocol.Invalid("snapshot owner no longer active")
		}
		status, rev, e := b.etcdClient.GetChangeFeedStatus(ctx, id)
		if e != nil {
			return protocol.Wrap(e)
		}
		if old := status.Snapshot; old != nil {
			if old.Generation != next.Generation || old.SnapshotID != next.SnapshotID || old.SnapshotTS != next.SnapshotTS {
				return protocol.Invalid("snapshot identity changed")
			}
			if old.Marker != nil && (next.Marker == nil || *old.Marker != *next.Marker) {
				return protocol.Invalid("cannot roll back snapshot marker")
			}
		}
		if next.Marker != nil && (next.Phase != "SNAPSHOT_COMMITTED" || next.Marker.PlanDigest != next.PlanRef.Digest || next.Marker.SnapshotTS != next.SnapshotTS) {
			return protocol.Invalid("marker/phase mismatch")
		}
		status.Snapshot = next
		bts, e := status.Marshal()
		if e != nil {
			return e
		}
		res, e := b.etcdClient.GetEtcdClient().Txn(ctx, []clientv3.Cmp{clientv3.Compare(clientv3.ModRevision(infoKey), "=", ir.Kvs[0].ModRevision), clientv3.Compare(clientv3.ModRevision(statusKey), "=", rev)}, []clientv3.Op{clientv3.OpPut(statusKey, bts)}, nil)
		if e != nil {
			return protocol.Wrap(e)
		}
		if res.Succeeded {
			return nil
		}
	}
	return protocol.Invalid("snapshot runtime CAS contention")
}
