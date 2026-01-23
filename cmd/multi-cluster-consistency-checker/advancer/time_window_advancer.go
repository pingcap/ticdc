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
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/consumer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// TimeWindow is the time window of the cluster, including the left boundary, right boundary and checkpoint ts
// Assert 1: LeftBoundary < CheckpointTs < RightBoundary
// Assert 2: The other cluster's checkpoint timestamp of next time window should be larger than the PDTimestampAfterTimeWindow saved in this cluster's time window
// Assert 3: CheckpointTs of this cluster should be larger than other clusters' RightBoundary of previous time window
// Assert 4: RightBoundary of this cluster should be larger than other clusters' CheckpointTs of this time window
type TimeWindow struct {
	LeftBoundary  uint64
	RightBoundary uint64
	// CheckpointTs is the checkpoint timestamp for each changefeed from upstream cluster,
	// mapping from downstream cluster ID to the checkpoint timestamp
	CheckpointTs map[string]uint64
	// PDTimestampAfterTimeWindow is the max PD timestamp after the time window for each downstream cluster,
	// mapping from upstream cluster ID to the max PD timestamp
	PDTimestampAfterTimeWindow map[string]uint64
}

type TimeWindowData struct {
	TimeWindow
	Data map[cloudstorage.DmlPathKey]consumer.IncrementalData
}

type TimeWindowAdvancer struct {
	// round is the current round of the time window
	round uint64

	// timeWindowTriplet is the triplet of adjacent time windows, mapping from cluster ID to the triplet
	timeWindowTriplet map[string][3]TimeWindow

	// checkpointWatcher is the Active-Active checkpoint watcher for each cluster,
	// mapping from cluster ID to the downstream cluster ID to the checkpoint watcher
	checkpointWatcher map[string]map[string]*watcher.CheckpointWatcher

	// s3checkpointWatcher is the S3 checkpoint watcher for each cluster, mapping from cluster ID to the s3 checkpoint watcher
	s3Watcher map[string]*watcher.S3Watcher

	// pdClients is the pd clients for each cluster, mapping from cluster ID to the pd client
	pdClients map[string]pd.Client
}

func NewTimeWindowAdvancer(
	checkpointWatchers map[string]map[string]*watcher.CheckpointWatcher,
	s3Watchers map[string]*watcher.S3Watcher,
	pdClients map[string]pd.Client,
) *TimeWindowAdvancer {
	timeWindowTriplet := make(map[string][3]TimeWindow)
	for clusterID := range pdClients {
		timeWindowTriplet[clusterID] = [3]TimeWindow{}
	}
	return &TimeWindowAdvancer{
		round:             0,
		timeWindowTriplet: timeWindowTriplet,
		checkpointWatcher: checkpointWatchers,
		s3Watcher:         s3Watchers,
		pdClients:         pdClients,
	}
}

// AdvanceTimeWindow advances the time window for each cluster. Here is the steps:
// 1. Advance the checkpoint ts for each upstream-downstream cluster changefeed.
//
// For any upstream-downstream cluster changefeed, the checkpoint ts should be advanced to
// the maximum of pd timestamp after previouds time window of downstream advanced and
// the right boundary of previouds time window of every clusters.
//
// 2. Advance the right boundary for each cluster.
//
// For any cluster, the right boundary should be advanced to the maximum of pd timestamp of
// the cluster after the checkpoint ts of its upstream cluster advanced and the previous
// timewindow's checkpoint ts of changefeed where the cluster is the upstream cluster or
// the downstream cluster.
//
// 3. Update the time window for each cluster.
//
// For any cluster, the time window should be updated to the new time window.
func (t *TimeWindowAdvancer) AdvanceTimeWindow(
	pctx context.Context,
) (map[string]TimeWindowData, error) {
	log.Info("advance time window", zap.Uint64("round", t.round))
	// mapping from upstream cluster ID to the downstream cluster ID to the min checkpoint timestamp
	minCheckpointTsMap := make(map[string]map[string]uint64)
	maxTimeWindowRightBoundary := uint64(0)
	for downstreamClusterID, triplet := range t.timeWindowTriplet {
		for upstreamClusterID, pdTimestampAfterTimeWindow := range triplet[2].PDTimestampAfterTimeWindow {
			if _, ok := minCheckpointTsMap[upstreamClusterID]; !ok {
				minCheckpointTsMap[upstreamClusterID] = make(map[string]uint64)
			}
			minCheckpointTsMap[upstreamClusterID][downstreamClusterID] = max(minCheckpointTsMap[upstreamClusterID][downstreamClusterID], pdTimestampAfterTimeWindow)
		}
		maxTimeWindowRightBoundary = max(maxTimeWindowRightBoundary, triplet[2].RightBoundary)
	}

	var lock sync.Mutex
	newTimeWindow := make(map[string]TimeWindow)
	maxPDTimestampAfterCheckpointTs := make(map[string]uint64)
	// for cluster ID, the max checkpoint timestamp is maximum of checkpoint from cluster to other clusters and checkpoint from other clusters to cluster
	maxCheckpointTs := make(map[string]uint64)
	// Advance the checkpoint ts for each cluster
	eg, ctx := errgroup.WithContext(pctx)
	for upstreamClusterID, downstreamCheckpointWatcherMap := range t.checkpointWatcher {
		for downstreamClusterID, checkpointWatcher := range downstreamCheckpointWatcherMap {
			mincheckpointTs := max(minCheckpointTsMap[upstreamClusterID][downstreamClusterID], maxTimeWindowRightBoundary)
			eg.Go(func() error {
				checkpointTs, err := checkpointWatcher.AdvanceCheckpointTs(ctx, mincheckpointTs)
				if err != nil {
					return errors.Trace(err)
				}
				pdtsos, err := t.getPDTsFromOtherClusters(ctx, upstreamClusterID)
				if err != nil {
					return errors.Trace(err)
				}
				lock.Lock()
				timeWindow := newTimeWindow[upstreamClusterID]
				if timeWindow.CheckpointTs == nil {
					timeWindow.CheckpointTs = make(map[string]uint64)
				}
				timeWindow.CheckpointTs[downstreamClusterID] = checkpointTs
				newTimeWindow[upstreamClusterID] = timeWindow
				for otherClusterID, pdtso := range pdtsos {
					maxPDTimestampAfterCheckpointTs[otherClusterID] = max(maxPDTimestampAfterCheckpointTs[otherClusterID], pdtso)
				}
				maxCheckpointTs[upstreamClusterID] = max(maxCheckpointTs[upstreamClusterID], checkpointTs)
				maxCheckpointTs[downstreamClusterID] = max(maxCheckpointTs[downstreamClusterID], checkpointTs)
				lock.Unlock()
				return nil
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return nil, errors.Annotate(err, "advance checkpoint timestamp failed")
	}

	// Update the time window for each cluster
	newDataMap := make(map[string]map[cloudstorage.DmlPathKey]consumer.IncrementalData)
	eg, ctx = errgroup.WithContext(pctx)
	for clusterID, triplet := range t.timeWindowTriplet {
		minTimeWindowRightBoundary := max(maxCheckpointTs[clusterID], maxPDTimestampAfterCheckpointTs[clusterID])
		s3Watcher := t.s3Watcher[clusterID]
		eg.Go(func() error {
			s3CheckpointTs, newData, err := s3Watcher.AdvanceS3CheckpointTs(ctx, minTimeWindowRightBoundary)
			if err != nil {
				return errors.Trace(err)
			}
			pdtsos, err := t.getPDTsFromOtherClusters(ctx, clusterID)
			if err != nil {
				return errors.Trace(err)
			}
			lock.Lock()
			newDataMap[clusterID] = newData
			timeWindow := newTimeWindow[clusterID]
			timeWindow.LeftBoundary = triplet[2].RightBoundary
			timeWindow.RightBoundary = s3CheckpointTs
			timeWindow.PDTimestampAfterTimeWindow = make(map[string]uint64)
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
	t.round += 1
	return newTimeWindowData(newTimeWindow, newDataMap), nil
}

func (t *TimeWindowAdvancer) updateTimeWindow(newTimeWindow map[string]TimeWindow) {
	for clusterID, timeWindow := range newTimeWindow {
		triplet := t.timeWindowTriplet[clusterID]
		triplet[0] = triplet[1]
		triplet[1] = triplet[2]
		triplet[2] = timeWindow
		t.timeWindowTriplet[clusterID] = triplet
		log.Info("update time window", zap.String("clusterID", clusterID), zap.Any("timeWindow", timeWindow))
	}
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

func newTimeWindowData(newTimeWindow map[string]TimeWindow, newDataMap map[string]map[cloudstorage.DmlPathKey]consumer.IncrementalData) map[string]TimeWindowData {
	timeWindowDatas := make(map[string]TimeWindowData)
	for clusterID, timeWindow := range newTimeWindow {
		timeWindowDatas[clusterID] = TimeWindowData{
			TimeWindow: timeWindow,
			Data:       newDataMap[clusterID],
		}
	}
	return timeWindowDatas
}
