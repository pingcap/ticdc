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
	"maps"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// TimeWindow is the time window of the cluster, including the left boundary, right boundary and checkpoint ts
// Assert 1: LeftBoundary < CheckpointTs < RightBoundary
// Assert 2: The checkpoint timestamp of next time window should be larger than the MaxPDTimestampAfterTimeWindow
type TimeWindow struct {
	LeftBoundary  uint64
	RightBoundary uint64
	// CheckpointTs is the checkpoint timestamp for each changefeed from upstream cluster,
	// mapping from downstream cluster ID to the checkpoint timestamp
	CheckpointTs map[string]uint64
	// MaxPDTimestampAfterTimeWindow is the max PD timestamp after the time window for each downstream cluster,
	// mapping from upstream cluster ID to the max PD timestamp
	MaxPDTimestampAfterTimeWindow map[string]uint64
}

type TimeWindowAdvancer struct {
	// round is the current round of the time window
	round uint64

	// timeWindowTriplet is the triplet of adjacent time windows, mapping from cluster ID to the triplet
	timeWindowTriplet map[string][3]TimeWindow

	// checkpointWatcher is the Active-Active checkpoint watcher for each cluster,
	// mapping from cluster ID to the downstream cluster ID to the checkpoint watcher
	checkpointWatcher map[string]map[string]*checkpointWatcher

	// s3checkpointWatcher is the S3 checkpoint watcher for each cluster, mapping from cluster ID to the s3 checkpoint watcher
	s3Watcher map[string]*s3Watcher

	// pdClients is the pd clients for each cluster, mapping from cluster ID to the pd client
	pdClients map[string]pd.Client
}

func NewTimeWindowAdvancer(
	checkpointWatchers map[string]map[string]*checkpointWatcher,
	s3Watchers map[string]*s3Watcher,
	pdClients map[string]pd.Client,
) *TimeWindowAdvancer {
	return &TimeWindowAdvancer{
		round:             0,
		timeWindowTriplet: make(map[string][3]TimeWindow),
		checkpointWatcher: checkpointWatchers,
		s3Watcher:         s3Watchers,
		pdClients:         pdClients,
	}
}

func (t *TimeWindowAdvancer) AdvanceTimeWindow(pctx context.Context) error {
	log.Info("advance time window", zap.Uint64("round", t.round))
	// mapping from upstream cluster ID to the downstream cluster ID to the min checkpoint timestamp
	minCheckpointTsMap := make(map[string]map[string]uint64)
	maxTimeWindowRightBoundary := uint64(0)
	for downstreamClusterID, triplet := range t.timeWindowTriplet {
		for upstreamClusterID, maxPDTimestampAfterTimeWindow := range triplet[2].MaxPDTimestampAfterTimeWindow {
			if _, ok := minCheckpointTsMap[upstreamClusterID]; !ok {
				minCheckpointTsMap[upstreamClusterID] = make(map[string]uint64)
			}
			minCheckpointTsMap[upstreamClusterID][downstreamClusterID] = max(minCheckpointTsMap[upstreamClusterID][downstreamClusterID], maxPDTimestampAfterTimeWindow)
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
				checkpointTs, err := checkpointWatcher.advanceCheckpointTs(ctx, mincheckpointTs)
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
		return errors.Annotate(err, "advance checkpoint timestamp failed")
	}

	// Update the time window for each cluster
	eg, ctx = errgroup.WithContext(pctx)
	for clusterID := range t.timeWindowTriplet {
		minTimeWindowRightBoundary := max(maxCheckpointTs[clusterID], maxPDTimestampAfterCheckpointTs[clusterID])
		s3Watcher := t.s3Watcher[clusterID]
		eg.Go(func() error {
			s3CheckpointTs, err := s3Watcher.advanceS3CheckpointTs(ctx, minTimeWindowRightBoundary)
			if err != nil {
				return errors.Trace(err)
			}
			pdtsos, err := t.getPDTsFromOtherClusters(ctx, clusterID)
			if err != nil {
				return errors.Trace(err)
			}
			lock.Lock()
			timeWindow := newTimeWindow[clusterID]
			timeWindow.RightBoundary = s3CheckpointTs
			timeWindow.MaxPDTimestampAfterTimeWindow = make(map[string]uint64)
			maps.Copy(timeWindow.MaxPDTimestampAfterTimeWindow, pdtsos)
			newTimeWindow[clusterID] = timeWindow
			lock.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return errors.Annotate(err, "advance time window failed")
	}
	t.updateTimeWindow(newTimeWindow)
	t.round += 1
	return nil
}

func (t *TimeWindowAdvancer) updateTimeWindow(newTimeWindow map[string]TimeWindow) {
	for clusterID, timeWindow := range newTimeWindow {
		triplet := t.timeWindowTriplet[clusterID]
		triplet[0] = triplet[1]
		triplet[1] = triplet[2]
		timeWindow.LeftBoundary = triplet[2].RightBoundary
		triplet[2] = timeWindow
		t.timeWindowTriplet[clusterID] = triplet
		log.Info("update time window", zap.String("clusterID", clusterID), zap.Any("timeWindow", timeWindow))
	}
}

func (t *TimeWindowAdvancer) getPDTsFromOtherClusters(ctx context.Context, clusterID string) (map[string]uint64, error) {
	var lock sync.Mutex
	pdtsos := make(map[string]uint64)
	eg, ctx := errgroup.WithContext(ctx)
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
	return pdtsos, nil
}
