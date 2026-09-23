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

package changefeed

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Exercise the metadata lifecycle against embedded etcd, including mixed legacy
// and migrated feeds with the same display name in different keyspaces.
func TestRuntimeMetadataLifecycle(t *testing.T) {
	ctx := context.Background()
	url, server, err := etcd.SetupEmbedEtcd(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(server.Close)
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{url.String()}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cdcClient, err := etcd.NewCDCEtcdClient(ctx, client, "runtime-test")
	require.NoError(t, err)
	backend := NewEtcdBackend(cdcClient)
	readKV := func(key string) *mvccpb.KeyValue {
		resp, err := client.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, resp.Kvs, 1)
		return resp.Kvs[0]
	}
	feeds := make([]*config.ChangeFeedInfo, 0, 2)
	for _, keyspace := range []string{"legacy", "migrated"} {
		info := &config.ChangeFeedInfo{
			ChangefeedID: common.NewChangeFeedIDWithName("same-name", keyspace),
			Config:       config.GetDefaultReplicaConfig(),
			SinkURI:      "blackhole://",
			StartTs:      100,
			State:        config.StateNormal,
			Epoch:        10,
		}
		id := info.ChangefeedID
		if keyspace == "legacy" {
			value, err := info.Marshal()
			require.NoError(t, err)
			_, err = client.Put(ctx, etcd.GetEtcdKeyChangeFeedInfo("runtime-test", id.DisplayName), value)
			require.NoError(t, err)
			_, err = client.Put(ctx, etcd.GetEtcdKeyJob("runtime-test", id.DisplayName), `{"checkpoint-ts":100}`)
			require.NoError(t, err)
			// A leftover runtime must never override an unmarked legacy info.
			_, err = client.Put(ctx, etcd.GetEtcdKeyChangeFeedRuntime("runtime-test", id.DisplayName), `{"state":"failed","epoch":999}`)
			require.NoError(t, err)
		} else {
			require.NoError(t, backend.CreateChangefeed(ctx, info))
			require.True(t, info.UseRuntime)
			infoKV := readKV(etcd.GetEtcdKeyChangeFeedInfo("runtime-test", id.DisplayName))
			require.Equal(t, infoKV.ModRevision, readKV(etcd.GetEtcdKeyJob("runtime-test", id.DisplayName)).ModRevision)
			require.Equal(t, infoKV.ModRevision, readKV(etcd.GetEtcdKeyChangeFeedRuntime("runtime-test", id.DisplayName)).ModRevision)
		}
		feeds = append(feeds, info)
	}
	all, err := backend.GetAllChangefeeds(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	for _, info := range feeds {
		require.Equal(t, info.GetRuntime(), all[info.ChangefeedID].Info.GetRuntime())
	}
	for _, info := range feeds {
		id := info.ChangefeedID
		infoKey := etcd.GetEtcdKeyChangeFeedInfo("runtime-test", id.DisplayName)
		runtimeKey := etcd.GetEtcdKeyChangeFeedRuntime("runtime-test", id.DisplayName)
		jobKey := etcd.GetEtcdKeyJob("runtime-test", id.DisplayName)
		initialInfo := readKV(infoKey)
		initialRuntime := readKV(runtimeKey)
		info.State = config.StateFailed
		info.Error = &config.RunningError{Code: "test", Message: "failure"}
		require.NoError(t, backend.UpdateChangefeedRuntime(ctx, info, 120, config.ProgressStopping))
		if info.UseRuntime {
			require.Equal(t, initialInfo, readKV(infoKey))
		} else {
			require.Equal(t, initialRuntime, readKV(runtimeKey))
		}
		loaded, err := backend.GetChangefeedInfo(ctx, id)
		require.NoError(t, err)
		require.Equal(t, info.GetRuntime(), loaded.GetRuntime())
		require.Equal(t, info.UseRuntime, loaded.UseRuntime)

		require.NoError(t, backend.PauseChangefeed(ctx, id))
		loaded, err = backend.GetChangefeedInfo(ctx, id)
		require.NoError(t, err)
		require.Equal(t, config.StateStopped, loaded.State)
		require.Equal(t, info.Error, loaded.Error)
		loaded, err = backend.ResumeChangefeed(ctx, id, 5, 130)
		require.NoError(t, err)
		require.Equal(t, config.StateNormal, loaded.State)
		require.Nil(t, loaded.Error)
		require.Equal(t, uint64(11), loaded.Epoch)
		status, _, err := cdcClient.GetChangeFeedStatus(ctx, id)
		require.NoError(t, err)
		require.Equal(t, uint64(130), status.CheckpointTs)
		require.Equal(t, config.ProgressNone, status.Progress)
		beforeWarning := readKV(jobKey)
		warning := config.StateWarning
		loaded, err = backend.BumpChangefeedEpoch(ctx, id, 15, EpochBumpOptions{
			State: &warning, UpdateError: true, Error: info.Error,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(15), loaded.Epoch)
		require.Equal(t, warning, loaded.State)
		require.Equal(t, beforeWarning, readKV(jobKey))
		if info.UseRuntime {
			require.Equal(t, initialInfo, readKV(infoKey))
		} else {
			require.False(t, loaded.UseRuntime)
			require.Equal(t, initialRuntime, readKV(runtimeKey))
		}

		// Config updates migrate legacy feeds and replace any leftover runtime
		// atomically with the marker. Already migrated feeds stay migrated.
		loaded.SinkURI = "mysql://127.0.0.1:4000"
		require.NoError(t, backend.UpdateChangefeed(ctx, loaded, 130, config.ProgressNone))
		require.True(t, loaded.UseRuntime)
		infoKV := readKV(infoKey)
		require.Equal(t, infoKV.ModRevision, readKV(runtimeKey).ModRevision)
		require.Equal(t, infoKV.ModRevision, readKV(jobKey).ModRevision)
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(infoKV.Value, &fields))
		for _, field := range []string{"state", "error", "epoch"} {
			require.NotContains(t, fields, field)
		}
		restarted := NewEtcdBackend(cdcClient)
		restored, err := restarted.GetAllChangefeeds(ctx)
		require.NoError(t, err)
		require.Equal(t, loaded.GetRuntime(), restored[id].Info.GetRuntime())
		require.Equal(t, loaded.SinkURI, restored[id].Info.SinkURI)
		require.NoError(t, restarted.DeleteChangefeed(ctx, id))
		for _, key := range []string{infoKey, runtimeKey, jobKey} {
			resp, err := client.Get(ctx, key)
			require.NoError(t, err)
			require.Empty(t, resp.Kvs)
		}
	}

	info := feeds[0]
	require.NoError(t, backend.CreateChangefeed(ctx, info))
	infoKV := readKV(etcd.GetEtcdKeyChangeFeedInfo("runtime-test", info.ChangefeedID.DisplayName))
	runtimeKey := etcd.GetEtcdKeyChangeFeedRuntime("runtime-test", info.ChangefeedID.DisplayName)
	for _, value := range []string{"", "invalid json"} {
		if value == "" {
			_, err = client.Delete(ctx, runtimeKey)
		} else {
			_, err = client.Put(ctx, runtimeKey, value)
		}
		require.NoError(t, err)
		_, err = backend.GetChangefeedInfo(ctx, info.ChangefeedID)
		require.Error(t, err)
		_, err = backend.GetAllChangefeeds(ctx)
		require.Error(t, err)
		_, err = backend.BumpChangefeedEpoch(ctx, info.ChangefeedID, 20, EpochBumpOptions{})
		require.Error(t, err)
		require.Equal(t, infoKV, readKV(string(infoKV.Key)))
	}
	require.NoError(t, cdcClient.ClearAllCDCInfo(ctx))
	resp, err := client.Get(ctx, runtimeKey)
	require.NoError(t, err)
	require.Empty(t, resp.Kvs)
}

func TestBumpRuntimeEpochRetriesOnCASConflict(t *testing.T) {
	for _, tc := range []struct {
		name         string
		updateStatus bool
		migrate      bool
	}{
		{name: "runtime only"},
		{name: "runtime and status", updateStatus: true},
		{name: "migration during epoch bump", migrate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			cdcClient := etcd.NewMockCDCEtcdClient(ctrl)
			client := etcd.NewMockClient(ctrl)
			cdcClient.EXPECT().GetEtcdClient().Return(client).AnyTimes()
			cdcClient.EXPECT().GetClusterID().Return("test").AnyTimes()
			backend := NewEtcdBackend(cdcClient)
			id := common.NewChangeFeedIDWithName("feed", "default")
			info := &config.ChangeFeedInfo{ChangefeedID: id, Config: config.GetDefaultReplicaConfig(), UseRuntime: true, Epoch: 8}
			infoKey := etcd.GetEtcdKeyChangeFeedInfo("test", id.DisplayName)
			runtimeKey := etcd.GetEtcdKeyChangeFeedRuntime("test", id.DisplayName)
			jobKey := etcd.GetEtcdKeyJob("test", id.DisplayName)
			for attempt := range 2 {
				info.UseRuntime = !tc.migrate || attempt > 0
				value, err := info.MarshalForStorage()
				require.NoError(t, err)
				client.EXPECT().Get(gomock.Any(), infoKey).Return(&clientv3.GetResponse{
					Kvs: []*mvccpb.KeyValue{{Value: []byte(value), ModRevision: 1}},
				}, nil)
				if info.UseRuntime {
					client.EXPECT().Get(gomock.Any(), runtimeKey).Return(&clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{{Value: []byte(fmt.Sprintf(`{"state":"warning","epoch":%d}`, 8+attempt)), ModRevision: int64(2 + attempt)}},
					}, nil)
				}
				cmps := []clientv3.Cmp{
					clientv3.Compare(clientv3.ModRevision(infoKey), "=", int64(1)),
				}
				if info.UseRuntime {
					cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(runtimeKey), "=", int64(2+attempt)))
				}
				if tc.updateStatus {
					cdcClient.EXPECT().GetChangeFeedStatus(gomock.Any(), id).
						Return(&config.ChangeFeedStatus{CheckpointTs: 100}, int64(4), nil)
					cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(jobKey), "=", int64(4)))
				}
				client.EXPECT().Txn(gomock.Any(), cmps, gomock.Any(), gomock.Len(0)).
					DoAndReturn(func(_ context.Context, _ []clientv3.Cmp, ops []clientv3.Op, _ []clientv3.Op) (*clientv3.TxnResponse, error) {
						expectedKey := runtimeKey
						if tc.migrate && attempt == 0 {
							expectedKey = infoKey
						}
						require.Equal(t, expectedKey, string(ops[0].KeyBytes()))
						var runtime config.ChangeFeedRuntime
						require.NoError(t, json.Unmarshal(ops[0].ValueBytes(), &runtime))
						require.Equal(t, uint64(9+attempt), runtime.Epoch)
						if tc.updateStatus {
							require.Len(t, ops, 2)
							require.Equal(t, jobKey, string(ops[1].KeyBytes()))
						} else {
							require.Len(t, ops, 1)
						}
						return &clientv3.TxnResponse{Succeeded: attempt == 1}, nil
					})
			}
			got, err := backend.BumpChangefeedEpoch(context.Background(), id, 7, EpochBumpOptions{UpdateStatus: tc.updateStatus, CheckpointTs: 110})
			require.NoError(t, err)
			require.Equal(t, uint64(10), got.Epoch)
		})
	}
}
