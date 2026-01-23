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
	"time"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/checker"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/security"
	putil "github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
)

func runTask(ctx context.Context, cfg *config.Config) error {
	checkpointWatchers, s3Watchers, pdClients, err := initClients(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}

	timeWindowAdvancer := advancer.NewTimeWindowAdvancer(checkpointWatchers, s3Watchers, pdClients)
	dataChecker := checker.NewDataChecker(cfg.Clusters)
	for {
		newTimeWindowData, err := timeWindowAdvancer.AdvanceTimeWindow(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if err := dataChecker.CheckInNextTimeWindow(ctx, newTimeWindowData); err != nil {
			return errors.Trace(err)
		}
	}
}

func initClients(ctx context.Context, cfg *config.Config) (map[string]map[string]*watcher.CheckpointWatcher, map[string]*watcher.S3Watcher, map[string]pd.Client, error) {
	checkpointWatchers := make(map[string]map[string]*watcher.CheckpointWatcher)
	s3Watchers := make(map[string]*watcher.S3Watcher)
	pdClients := make(map[string]pd.Client)
	for clusterID, clusterConfig := range cfg.Clusters {
		pdClient, etcdClient, err := newPDClient(ctx, clusterConfig.PDAddr, &clusterConfig.SecurityConfig)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		upstreamCheckpointWatchers := make(map[string]*watcher.CheckpointWatcher)
		for downstreamClusterID, downstreamClusterChangefeedConfig := range clusterConfig.DownstreamClusterChangefeedConfig {
			checkpointWatcher := watcher.NewCheckpointWatcher(clusterID, downstreamClusterID, downstreamClusterChangefeedConfig.ChangefeedID, etcdClient)
			upstreamCheckpointWatchers[downstreamClusterID] = checkpointWatcher
		}
		checkpointWatchers[clusterID] = upstreamCheckpointWatchers
		s3Storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, clusterConfig.S3SinkURI)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		s3Watcher := watcher.NewS3Watcher(
			watcher.NewCheckpointWatcher(clusterID, "s3", clusterConfig.S3ChangefeedID, etcdClient),
			s3Storage,
		)
		s3Watchers[clusterID] = s3Watcher
		pdClients[clusterID] = pdClient
	}

	return checkpointWatchers, s3Watchers, pdClients, nil
}

func newPDClient(ctx context.Context, pdAddr string, securityConfig *security.Credential) (pd.Client, *etcd.CDCEtcdClientImpl, error) {
	pdClient, err := pd.NewClientWithContext(
		ctx, "consistency-checker", []string{pdAddr}, securityConfig.PDSecurityOption(),
		pdopt.WithCustomTimeoutOption(10*time.Second),
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	etcdCli, err := etcd.CreateRawEtcdClient(securityConfig, grpc.EmptyDialOption{}, pdAddr)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, "default")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return pdClient, cdcEtcdClient, nil
}
