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

package watcher

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type CheckpointWatcher struct {
	upstreamClusterID   string
	downstreamClusterID string
	changefeedID        common.ChangeFeedID
	etcdClient          etcd.CDCEtcdClient
}

func NewCheckpointWatcher(
	upstreamClusterID, downstreamClusterID, changefeedID string,
	etcdClient etcd.CDCEtcdClient,
) *CheckpointWatcher {
	return &CheckpointWatcher{
		upstreamClusterID:   upstreamClusterID,
		downstreamClusterID: downstreamClusterID,
		changefeedID:        common.NewChangeFeedIDWithName(changefeedID, "default"),
		etcdClient:          etcdClient,
	}
}

// advanceCheckpointTs waits for the checkpoint to exceed minCheckpointTs
func (cw *CheckpointWatcher) AdvanceCheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
	// First, get the current chceckpoint status from etcd
	status, modRev, err := cw.etcdClient.GetChangeFeedStatus(ctx, cw.changefeedID)
	if err != nil {
		return 0, errors.Annotate(err, "failed to get changefeed status")
	}
	statusKey := etcd.GetEtcdKeyJob(cw.etcdClient.GetClusterID(), cw.changefeedID.DisplayName)
	// Watch for checkpoint updates
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	log.Debug("Starting to watch checkpoint",
		zap.String("changefeedID", cw.changefeedID.String()),
		zap.String("statusKey", statusKey),
		zap.String("upstreamClusterID", cw.upstreamClusterID),
		zap.String("downstreamClusterID", cw.downstreamClusterID),
		zap.Uint64("checkpoint", status.CheckpointTs),
		zap.Int64("startRev", modRev+1),
		zap.Uint64("minCheckpointTs", minCheckpointTs))

	watchCh := cw.etcdClient.GetEtcdClient().Watch(
		watchCtx,
		statusKey,
		"checkpoint-watcher",
		clientv3.WithRev(modRev+1),
	)

	for {
		select {
		case <-ctx.Done():
			return 0, errors.Annotate(ctx.Err(), "context canceled")
		case watchResp, ok := <-watchCh:
			if !ok {
				return 0, errors.Errorf("[changefeedID: %s] watch channel closed", cw.changefeedID.String())
			}

			if err := watchResp.Err(); err != nil {
				return 0, errors.Annotatef(err, "[changefeedID: %s] watch error", cw.changefeedID.String())
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					return 0, errors.Errorf("[changefeedID: %s] changefeed status key is deleted", cw.changefeedID.String())
				}

				// Parse the updated status
				status := &config.ChangeFeedStatus{}
				if err := status.Unmarshal(event.Kv.Value); err != nil {
					return 0, errors.Annotatef(err, "[changefeedID: %s] failed to unmarshal changefeed status", cw.changefeedID.String())
				}

				checkpointTs := status.CheckpointTs
				log.Debug("Checkpoint updated",
					zap.String("changefeedID", cw.changefeedID.String()),
					zap.Uint64("checkpoint", checkpointTs),
					zap.Uint64("minCheckpointTs", minCheckpointTs))

				// Check if checkpoint exceeds minCheckpointTs
				if checkpointTs > minCheckpointTs {
					log.Debug("Checkpoint exceeds minCheckpointTs, getting TSO from downstream",
						zap.String("changefeedID", cw.changefeedID.String()),
						zap.Uint64("checkpoint", checkpointTs))
					return checkpointTs, nil
				}
			}
		}
	}
}
