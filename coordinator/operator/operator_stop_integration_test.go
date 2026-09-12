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

package operator

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/coordinator/changefeed"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestPauseReplacedByRemoveWithEtcd covers the complete metadata lifecycle for
// the regression where a canceled pause operator reset ProgressRemoving to
// ProgressNone before the remove operator deleted the changefeed.
func TestPauseReplacedByRemoveWithEtcd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientURL, etcdServer, err := etcd.SetupEmbedEtcd(t.TempDir())
	require.NoError(t, err)
	defer etcdServer.Close()

	rawClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{clientURL.String()},
		DialTimeout: 3 * time.Second,
	})
	require.NoError(t, err)
	defer rawClient.Close()

	cdcClient, err := etcd.NewCDCEtcdClient(ctx, rawClient, "operator-integration-test")
	require.NoError(t, err)
	backend := changefeed.NewEtcdBackend(cdcClient)

	cfID := common.NewChangeFeedIDWithName("pause-then-remove", common.DefaultKeyspaceName)
	info := &config.ChangeFeedInfo{
		ChangefeedID: cfID,
		Config:       config.GetDefaultReplicaConfig(),
		SinkURI:      "blackhole://",
		StartTs:      1,
		State:        config.StateNormal,
	}
	require.NoError(t, backend.CreateChangefeed(ctx, info))

	changefeedDB := changefeed.NewChangefeedDB(1216)
	cf := changefeed.NewChangefeed(cfID, info, info.StartTs, true)
	oc, self, _ := newOperatorControllerForTest(t, changefeedDB, backend, nil)
	changefeedDB.AddReplicatingMaintainer(cf, self.ID)

	// Match the controller's pause path, then replace its in-flight stop
	// operator with a remove operation before the maintainer has stopped.
	require.NoError(t, backend.PauseChangefeed(ctx, cfID))
	pauseOp := oc.StopChangefeed(ctx, cfID, false)
	require.NoError(t, backend.SetChangefeedProgress(ctx, cfID, config.ProgressRemoving))
	removeOp := oc.StopChangefeed(ctx, cfID, true)
	require.NotSame(t, pauseOp, removeOp)

	status, _, err := cdcClient.GetChangeFeedStatus(ctx, cfID)
	require.NoError(t, err)
	require.Equal(t, config.ProgressRemoving, status.Progress)

	removeOp.Check(self.ID, &heartbeatpb.MaintainerStatus{
		State:           heartbeatpb.ComponentState_Stopped,
		MaintainerEpoch: info.Epoch,
	})
	oc.Execute()

	_, err = backend.GetChangefeedInfo(ctx, cfID)
	require.True(t, errors.ErrChangeFeedNotExists.Equal(err))
}
