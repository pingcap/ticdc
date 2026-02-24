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

package advancer

import (
	"context"
	"maps"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type TimeWindowAdvancer struct {
	// round is the current round of the time window
	round uint64

	// timeWindowTriplet is the triplet of adjacent time windows, mapping from cluster ID to the triplet
	timeWindowTriplet map[string][3]types.TimeWindow

	// checkpointWatcher is the Active-Active checkpoint watcher for each cluster,
	// mapping from local cluster ID to replicated cluster ID to the checkpoint watcher
	checkpointWatcher map[string]map[string]watcher.Watcher

	// s3checkpointWatcher is the S3 checkpoint watcher for each cluster, mapping from cluster ID to the s3 checkpoint watcher
	s3Watcher map[string]*watcher.S3Watcher

	// pdClients is the pd clients for each cluster, mapping from cluster ID to the pd client
	pdClients map[string]pd.Client
}

func NewTimeWindowAdvancer(
	ctx context.Context,
	checkpointWatchers map[string]map[string]watcher.Watcher,
	s3Watchers map[string]*watcher.S3Watcher,
	pdClients map[string]pd.Client,
	checkpoint *recorder.Checkpoint,
) (*TimeWindowAdvancer, map[string]map[cloudstorage.DmlPathKey]types.IncrementalData, error) {
	timeWindowTriplet := make(map[string][3]types.TimeWindow)
	for clusterID := range pdClients {
		timeWindowTriplet[clusterID] = [3]types.TimeWindow{}
	}
	advancer := &TimeWindowAdvancer{
		round:             0,
		timeWindowTriplet: timeWindowTriplet,
		checkpointWatcher: checkpointWatchers,
		s3Watcher:         s3Watchers,
		pdClients:         pdClients,
	}
	newDataMap, err := advancer.initializeFromCheckpoint(ctx, checkpoint)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return advancer, newDataMap, nil
}

func (t *TimeWindowAdvancer) initializeFromCheckpoint(
	ctx context.Context,
	checkpoint *recorder.Checkpoint,
) (map[string]map[cloudstorage.DmlPathKey]types.IncrementalData, error) {
	if checkpoint == nil {
		return nil, nil
	}
	if checkpoint.CheckpointItems[2] == nil {
		return nil, nil
	}
	t.round = checkpoint.CheckpointItems[2].Round + 1
	for clusterID := range t.timeWindowTriplet {
		newTimeWindows := [3]types.TimeWindow{}
		newTimeWindows[2] = checkpoint.CheckpointItems[2].ClusterInfo[clusterID].TimeWindow
		if checkpoint.CheckpointItems[1] != nil {
			newTimeWindows[1] = checkpoint.CheckpointItems[1].ClusterInfo[clusterID].TimeWindow
		}
		if checkpoint.CheckpointItems[0] != nil {
			newTimeWindows[0] = checkpoint.CheckpointItems[0].ClusterInfo[clusterID].TimeWindow
		}
		t.timeWindowTriplet[clusterID] = newTimeWindows
	}

	var mu sync.Mutex
	newDataMap := make(map[string]map[cloudstorage.DmlPathKey]types.IncrementalData)
	eg, egCtx := errgroup.WithContext(ctx)
	for clusterID, s3Watcher := range t.s3Watcher {
		eg.Go(func() error {
			newData, err := s3Watcher.InitializeFromCheckpoint(egCtx, clusterID, checkpoint)
			if err != nil {
				return errors.Trace(err)
			}
			mu.Lock()
			newDataMap[clusterID] = newData
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	return newDataMap, nil
}

// AdvanceTimeWindow advances the time window for each cluster. Here is the steps:
// 1. Advance the checkpoint ts for each local-to-replicated changefeed.
//
// For any local-to-replicated changefeed, the checkpoint ts should be advanced to
// the maximum of pd timestamp after previous time window of the replicated cluster
// advanced and the right boundary of previous time window of every clusters.
//
// 2. Advance the right boundary for each cluster.
//
// For any cluster, the right boundary should be advanced to the maximum of pd timestamp of
// the cluster after the checkpoint ts of its local cluster advanced and the previous
// timewindow's checkpoint ts of changefeed where the cluster is the local or the replicated.
//
// 3. Update the time window for each cluster.
//
// For any cluster, the time window should be updated to the new time window.
func (t *TimeWindowAdvancer) AdvanceTimeWindow(
	pctx context.Context,
) (map[string]types.TimeWindowData, error) {
	log.Debug("advance time window", zap.Uint64("round", t.round))
	// mapping from local cluster ID to replicated cluster ID to the min checkpoint timestamp
	minCheckpointTsMap := make(map[string]map[string]uint64)
	maxTimeWindowRightBoundary := uint64(0)
	for replicatedClusterID, triplet := range t.timeWindowTriplet {
		for localClusterID, pdTimestampAfterTimeWindow := range triplet[2].PDTimestampAfterTimeWindow {
			if _, ok := minCheckpointTsMap[localClusterID]; !ok {
				minCheckpointTsMap[localClusterID] = make(map[string]uint64)
			}
			minCheckpointTsMap[localClusterID][replicatedClusterID] = max(minCheckpointTsMap[localClusterID][replicatedClusterID], pdTimestampAfterTimeWindow)
		}
		maxTimeWindowRightBoundary = max(maxTimeWindowRightBoundary, triplet[2].RightBoundary)
	}

	var lock sync.Mutex
	newTimeWindow := make(map[string]types.TimeWindow)
	maxPDTimestampAfterCheckpointTs := make(map[string]uint64)
	// for cluster ID, the max checkpoint timestamp is maximum of checkpoint from cluster to other clusters and checkpoint from other clusters to cluster
	maxCheckpointTs := make(map[string]uint64)
	// Advance the checkpoint ts for each cluster
	eg, ctx := errgroup.WithContext(pctx)
	for localClusterID, replicatedCheckpointWatcherMap := range t.checkpointWatcher {
		for replicatedClusterID, checkpointWatcher := range replicatedCheckpointWatcherMap {
			minCheckpointTs := max(minCheckpointTsMap[localClusterID][replicatedClusterID], maxTimeWindowRightBoundary)
			eg.Go(func() error {
				checkpointTs, err := checkpointWatcher.AdvanceCheckpointTs(ctx, minCheckpointTs)
				if err != nil {
					return errors.Trace(err)
				}
				// TODO: optimize this by getting pd ts in the end of all checkpoint ts advance
				pdtsos, err := t.getPDTsFromOtherClusters(ctx, localClusterID)
				if err != nil {
					return errors.Trace(err)
				}
				lock.Lock()
				timeWindow := newTimeWindow[localClusterID]
				if timeWindow.CheckpointTs == nil {
					timeWindow.CheckpointTs = make(map[string]uint64)
				}
				timeWindow.CheckpointTs[replicatedClusterID] = checkpointTs
				newTimeWindow[localClusterID] = timeWindow
				for otherClusterID, pdtso := range pdtsos {
					maxPDTimestampAfterCheckpointTs[otherClusterID] = max(maxPDTimestampAfterCheckpointTs[otherClusterID], pdtso)
				}
				maxCheckpointTs[localClusterID] = max(maxCheckpointTs[localClusterID], checkpointTs)
				maxCheckpointTs[replicatedClusterID] = max(maxCheckpointTs[replicatedClusterID], checkpointTs)
				lock.Unlock()
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Annotate(err, "advance checkpoint timestamp failed")
	}

	// Update the time window for each cluster
	newDataMap := make(map[string]map[cloudstorage.DmlPathKey]types.IncrementalData)
	maxVersionMap := make(map[string]map[types.SchemaTableKey]types.VersionKey)
	eg, ctx = errgroup.WithContext(pctx)
	for clusterID, triplet := range t.timeWindowTriplet {
		minTimeWindowRightBoundary := max(maxCheckpointTs[clusterID], maxPDTimestampAfterCheckpointTs[clusterID], triplet[2].NextMinLeftBoundary)
		s3Watcher := t.s3Watcher[clusterID]
		eg.Go(func() error {
			s3CheckpointTs, err := s3Watcher.AdvanceS3CheckpointTs(ctx, minTimeWindowRightBoundary)
			if err != nil {
				return errors.Trace(err)
			}
			newData, maxClusterVersionMap, err := s3Watcher.ConsumeNewFiles(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			pdtso, err := t.getPDTsFromCluster(ctx, clusterID)
			if err != nil {
				return errors.Trace(err)
			}
			pdtsos, err := t.getPDTsFromOtherClusters(ctx, clusterID)
			if err != nil {
				return errors.Trace(err)
			}
			lock.Lock()
			newDataMap[clusterID] = newData
			maxVersionMap[clusterID] = maxClusterVersionMap
			timeWindow := newTimeWindow[clusterID]
			timeWindow.LeftBoundary = triplet[2].RightBoundary
			timeWindow.RightBoundary = s3CheckpointTs
			timeWindow.PDTimestampAfterTimeWindow = make(map[string]uint64)
			timeWindow.NextMinLeftBoundary = pdtso
			maps.Copy(timeWindow.PDTimestampAfterTimeWindow, pdtsos)
			newTimeWindow[clusterID] = timeWindow
			lock.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Annotate(err, "advance time window failed")
	}
	t.updateTimeWindow(newTimeWindow)
	t.round++
	return newTimeWindowData(newTimeWindow, newDataMap, maxVersionMap), nil
}

func (t *TimeWindowAdvancer) updateTimeWindow(newTimeWindow map[string]types.TimeWindow) {
	for clusterID, timeWindow := range newTimeWindow {
		triplet := t.timeWindowTriplet[clusterID]
		triplet[0] = triplet[1]
		triplet[1] = triplet[2]
		triplet[2] = timeWindow
		t.timeWindowTriplet[clusterID] = triplet
		log.Debug("update time window", zap.String("clusterID", clusterID), zap.Any("timeWindow", timeWindow))
	}
}

func (t *TimeWindowAdvancer) getPDTsFromCluster(ctx context.Context, clusterID string) (uint64, error) {
	pdClient := t.pdClients[clusterID]
	phyTs, logicTs, err := pdClient.GetTS(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	ts := oracle.ComposeTS(phyTs, logicTs)
	return ts, nil
}

func (t *TimeWindowAdvancer) getPDTsFromOtherClusters(pctx context.Context, clusterID string) (map[string]uint64, error) {
	var lock sync.Mutex
	pdtsos := make(map[string]uint64)
	eg, ctx := errgroup.WithContext(pctx)
	for otherClusterID := range t.pdClients {
		if otherClusterID == clusterID {
			continue
		}
		pdClient := t.pdClients[otherClusterID]
		eg.Go(func() error {
			phyTs, logicTs, err := pdClient.GetTS(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			ts := oracle.ComposeTS(phyTs, logicTs)
			lock.Lock()
			pdtsos[otherClusterID] = ts
			lock.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	return pdtsos, nil
}

func newTimeWindowData(
	newTimeWindow map[string]types.TimeWindow,
	newDataMap map[string]map[cloudstorage.DmlPathKey]types.IncrementalData,
	maxVersionMap map[string]map[types.SchemaTableKey]types.VersionKey,
) map[string]types.TimeWindowData {
	timeWindowDatas := make(map[string]types.TimeWindowData)
	for clusterID, timeWindow := range newTimeWindow {
		timeWindowDatas[clusterID] = types.TimeWindowData{
			TimeWindow: timeWindow,
			Data:       newDataMap[clusterID],
			MaxVersion: maxVersionMap[clusterID],
		}
	}
	return timeWindowDatas
}
