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

	"github.com/pingcap/ticdc/pkg/errors"
	pd "github.com/tikv/pd/client"
)

func runTask(ctx context.Context, cfg *Config) error {
	checkpointWatchers, s3Watchers, pdClients, err := initClients(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	timeWindowAdvancer := NewTimeWindowAdvancer(checkpointWatchers, s3Watchers, pdClients)
	for {
		err = timeWindowAdvancer.AdvanceTimeWindow(ctx)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

func initClients(ctx context.Context, cfg *Config) (map[string]map[string]*checkpointWatcher, map[string]*s3Watcher, map[string]pd.Client, error) {
	checkpointWatchers := make(map[string]map[string]*checkpointWatcher)
	s3Watchers := make(map[string]*s3Watcher)
	pdClients := make(map[string]pd.Client)
	for clusterID, clusterConfig := range cfg.Clusters {
		pdClient, etcdClient, err := newClient(ctx, clusterConfig.PDAddr, clusterConfig.SecurityConfig)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		upstreamCheckpointWatchers := make(map[string]*checkpointWatcher)
		for downstreamClusterID, downstreamClusterChangefeedConfig := range clusterConfig.DownstreamClusterChangefeedConfig {
			checkpointWatcher := NewCheckpointWatcher(clusterID, downstreamClusterID, downstreamClusterChangefeedConfig.ChangefeedID, etcdClient)
			upstreamCheckpointWatchers[downstreamClusterID] = checkpointWatcher
		}
		checkpointWatchers[clusterID] = upstreamCheckpointWatchers
		s3Watcher := &s3Watcher{
			checkpointWatcher: NewCheckpointWatcher(clusterID, "s3", clusterConfig.S3ChangefeedID, etcdClient),
		}
		s3Watchers[clusterID] = s3Watcher
		pdClients[clusterID] = pdClient
	}

	return checkpointWatchers, s3Watchers, pdClients, nil
}
