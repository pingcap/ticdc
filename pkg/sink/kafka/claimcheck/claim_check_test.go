// Copyright 2023 PingCAP, Inc.
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

package claimcheck

import (
	"context"
	"testing"
	"time"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
<<<<<<< HEAD
=======
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/mockobjstore"
>>>>>>> 46132a925 (server: fence capture writes with etcd and P2P leases (#6092))
	"github.com/stretchr/testify/require"
)

func TestClaimCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	changefeedID := commonType.NewChangeFeedIDWithName("test", "")
	largeHandleConfig := config.NewDefaultLargeMessageHandleConfig()

	claimCheck, err := New(ctx, largeHandleConfig, changefeedID)
	require.NoError(t, err)
	require.Nil(t, claimCheck)

	largeHandleConfig.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	largeHandleConfig.ClaimCheckStorageURI = "file:///tmp/abc/"
	claimCheck, err = New(ctx, largeHandleConfig, changefeedID)
	require.NoError(t, err)

	fileName := claimCheck.FileNameWithPrefix("file.json")
	require.Equal(t, "file:///tmp/abc/file.json", fileName)
}
<<<<<<< HEAD
=======

func TestClaimCheckStorageErrorWrappedOnce(t *testing.T) {
	largeHandleConfig := config.NewDefaultLargeMessageHandleConfig()
	largeHandleConfig.LargeMessageHandleOption = config.LargeMessageHandleOptionClaimCheck
	largeHandleConfig.ClaimCheckStorageURI = "invalid://bucket"

	claimCheck, err := New(context.Background(), largeHandleConfig, common.NewChangeFeedIDWithName("test", "default"))

	require.Nil(t, claimCheck)
	require.ErrorIs(t, err, errors.ErrExternalStorageAPI)
	require.Equal(t, 1, strings.Count(err.Error(), string(errors.ErrExternalStorageAPI.RFCCode())))
}

func TestClaimCheckCloseClosesStorage(t *testing.T) {
	var nilClaimCheck *ClaimCheck
	require.NotPanics(t, nilClaimCheck.Close)

	ctrl := gomock.NewController(t)
	storage := mockobjstore.NewMockStorage(ctrl)
	storage.EXPECT().Close().Times(1)
	claimCheck := &ClaimCheck{
		storage:      storage,
		changefeedID: common.NewChangeFeedIDWithName("test", "default"),
	}

	claimCheck.Close()
}

func TestClaimCheckConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	storage := objstore.NewMemStorage()
	changefeedID := common.NewChangeFeedIDWithName("test", "default")
	claimCheck := &ClaimCheck{
		storage:                   storage,
		rawValue:                  true,
		changefeedID:              changefeedID,
		metricSendMessageDuration: claimCheckSendMessageDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricSendMessageCount:    claimCheckSendMessageCount.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	}
	t.Cleanup(claimCheck.Close)

	const concurrency = 32
	group := new(errgroup.Group)
	for i := range concurrency {
		fileName := fmt.Sprintf("%d.json", i)
		group.Go(func() error {
			return claimCheck.WriteMessage(ctx, nil, []byte(fileName), fileName)
		})
	}
	require.NoError(t, group.Wait())

	for i := range concurrency {
		fileName := fmt.Sprintf("%d.json", i)
		data, err := storage.ReadFile(ctx, fileName)
		require.NoError(t, err)
		require.Equal(t, fileName, string(data))
	}
}

func TestClaimCheckWriteGateBlocksObjectPublication(t *testing.T) {
	ctx := t.Context()
	storage := objstore.NewMemStorage()
	changefeedID := common.NewChangeFeedIDWithName("test", "default")
	claimCheck := &ClaimCheck{
		storage:                   storage,
		rawValue:                  true,
		changefeedID:              changefeedID,
		metricSendMessageDuration: claimCheckSendMessageDuration.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
		metricSendMessageCount:    claimCheckSendMessageCount.WithLabelValues(changefeedID.Keyspace(), changefeedID.Name()),
	}
	t.Cleanup(claimCheck.Close)
	gate := writelease.NewGate()
	claimCheck.SetWriteGate(gate)

	done := make(chan error, 1)
	go func() {
		done <- claimCheck.WriteMessage(ctx, nil, []byte("large-message"), "message.json")
	}()

	time.Sleep(100 * time.Millisecond)
	exists, err := storage.FileExists(ctx, "message.json")
	require.NoError(t, err)
	require.False(t, exists)

	require.True(t, gate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))
	require.NoError(t, <-done)
	exists, err = storage.FileExists(ctx, "message.json")
	require.NoError(t, err)
	require.True(t, exists)
}
>>>>>>> 46132a925 (server: fence capture writes with etcd and P2P leases (#6092))
