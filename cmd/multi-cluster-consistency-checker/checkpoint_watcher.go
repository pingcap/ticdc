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

package main

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// CheckpointWatcher watches CDC checkpoint from etcd and records TSO from PD
type CheckpointWatcher struct {
	// checkpoint stores the current CDC checkpoint
	checkpoint uint64

	// pdclientUp is the PD client for upstream cluster
	pdclientUp pd.Client

	// pdclientDown is the PD client for downstream cluster
	pdclientDown pd.Client

	// etcdClientUp is the etcd client for upstream cluster (used to watch checkpoint)
	etcdClientUp etcd.CDCEtcdClient

	// p stores the TSO obtained from pdclientDown
	p uint64

	// changefeedID is the ID of the changefeed to watch
	changefeedID common.ChangeFeedID
}

// NewCheckpointWatcher creates a new CheckpointWatcher instance
func NewCheckpointWatcher(
	changefeedID common.ChangeFeedID,
	pdclientUp pd.Client,
	pdclientDown pd.Client,
	etcdClientUp etcd.CDCEtcdClient,
) *CheckpointWatcher {
	return &CheckpointWatcher{
		changefeedID: changefeedID,
		pdclientUp:   pdclientUp,
		pdclientDown: pdclientDown,
		etcdClientUp: etcdClientUp,
	}
}

// WaitForCheckpoint waits for the checkpoint to exceed minCheckpointTs,
// then gets a TSO from pdclientDown and records it to p
func (cw *CheckpointWatcher) WaitForCheckpoint(ctx context.Context, minCheckpointTs uint64) error {
	log.Info("Starting to watch checkpoint",
		zap.String("changefeedID", cw.changefeedID.String()),
		zap.Uint64("minCheckpointTs", minCheckpointTs))

	// First, get the current checkpoint status
	status, modRev, err := cw.etcdClientUp.GetChangeFeedStatus(ctx, cw.changefeedID)
	if err != nil {
		return fmt.Errorf("failed to get changefeed status: %w", err)
	}

	cw.checkpoint = status.CheckpointTs
	log.Info("Current checkpoint",
		zap.Uint64("checkpoint", cw.checkpoint),
		zap.Uint64("minCheckpointTs", minCheckpointTs))

	// Watch for checkpoint updates
	clusterID := cw.etcdClientUp.GetClusterID()
	statusKey := etcd.GetEtcdKeyJob(clusterID, cw.changefeedID.DisplayName)

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	watchCh := cw.etcdClientUp.GetEtcdClient().Watch(
		watchCtx,
		statusKey,
		"checkpoint-watcher",
		clientv3.WithRev(modRev+1),
	)

	log.Info("Watching checkpoint status",
		zap.String("statusKey", statusKey),
		zap.Int64("startRev", modRev+1))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case watchResp := <-watchCh:
			if err := watchResp.Err(); err != nil {
				return fmt.Errorf("watch error: %w", err)
			}

			for _, event := range watchResp.Events {
				if event.Type == clientv3.EventTypeDelete {
					log.Warn("Changefeed status deleted",
						zap.String("changefeedID", cw.changefeedID.String()))
					continue
				}

				// Parse the updated status
				status := &config.ChangeFeedStatus{}
				if err := status.Unmarshal(event.Kv.Value); err != nil {
					log.Warn("Failed to unmarshal changefeed status",
						zap.String("changefeedID", cw.changefeedID.String()),
						zap.Error(err))
					continue
				}

				cw.checkpoint = status.CheckpointTs
				log.Info("Checkpoint updated",
					zap.Uint64("checkpoint", cw.checkpoint),
					zap.Uint64("minCheckpointTs", minCheckpointTs))

				// Check if checkpoint exceeds minCheckpointTs
				if cw.checkpoint > minCheckpointTs {
					log.Info("Checkpoint exceeds minCheckpointTs, getting TSO from downstream")
					return cw.getAndRecordTSO(ctx)
				}
			}
		}
	}
}

// getAndRecordTSO gets a TSO from pdclientDown and records it to p
func (cw *CheckpointWatcher) getAndRecordTSO(ctx context.Context) error {
	// Get TSO from downstream PD client
	physical, logical, err := cw.pdclientDown.GetTS(ctx)
	if err != nil {
		return fmt.Errorf("failed to get TSO from downstream PD: %w", err)
	}

	// Compose TSO from physical and logical parts
	cw.p = oracle.ComposeTS(physical, logical)

	log.Info("TSO obtained and recorded",
		zap.Int64("physical", physical),
		zap.Int64("logical", logical),
		zap.Uint64("tso", cw.p),
		zap.Uint64("checkpoint", cw.checkpoint))

	return nil
}

// GetCheckpoint returns the current checkpoint
func (cw *CheckpointWatcher) GetCheckpoint() uint64 {
	return cw.checkpoint
}

// GetRecordedTSO returns the recorded TSO (p)
func (cw *CheckpointWatcher) GetRecordedTSO() uint64 {
	return cw.p
}
