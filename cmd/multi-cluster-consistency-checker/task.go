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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/checker"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/security"
	putil "github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func runTask(ctx context.Context, cfg *config.Config) error {
	checkpointWatchers, s3Watchers, pdClients, etcdClients, err := initClients(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	// Ensure cleanup happens even if there's an error
	defer cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)

	recorder, err := recorder.NewRecorder(cfg.GlobalConfig.DataDir, cfg.Clusters)
	if err != nil {
		return errors.Trace(err)
	}
	timeWindowAdvancer, checkpointDataMap, err := advancer.NewTimeWindowAdvancer(ctx, checkpointWatchers, s3Watchers, pdClients, recorder.GetCheckpoint())
	if err != nil {
		return errors.Trace(err)
	}
	dataChecker := checker.NewDataChecker(ctx, cfg.Clusters, checkpointDataMap, recorder.GetCheckpoint())

	log.Info("Starting consistency checker task")
	for {
		// Check if context is cancelled before starting a new iteration
		select {
		case <-ctx.Done():
			log.Info("Context cancelled, shutting down gracefully")
			return ctx.Err()
		default:
		}

		newTimeWindowData, err := timeWindowAdvancer.AdvanceTimeWindow(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		report, err := dataChecker.CheckInNextTimeWindow(ctx, newTimeWindowData)
		if err != nil {
			return errors.Trace(err)
		}

		if err := recorder.RecordTimeWindow(newTimeWindowData, report); err != nil {
			return errors.Trace(err)
		}
	}
}

func initClients(ctx context.Context, cfg *config.Config) (
	map[string]map[string]watcher.Watcher,
	map[string]*watcher.S3Watcher,
	map[string]pd.Client,
	map[string]*etcd.CDCEtcdClientImpl,
	error,
) {
	checkpointWatchers := make(map[string]map[string]watcher.Watcher)
	s3Watchers := make(map[string]*watcher.S3Watcher)
	pdClients := make(map[string]pd.Client)
	etcdClients := make(map[string]*etcd.CDCEtcdClientImpl)

	for clusterID, clusterConfig := range cfg.Clusters {
		pdClient, etcdClient, err := newPDClient(ctx, clusterConfig.PDAddr, &clusterConfig.SecurityConfig)
		if err != nil {
			// Clean up already created clients before returning error
			cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)
			return nil, nil, nil, nil, errors.Trace(err)
		}
		etcdClients[clusterID] = etcdClient

		upstreamCheckpointWatchers := make(map[string]watcher.Watcher)
		for downstreamClusterID, downstreamClusterChangefeedConfig := range clusterConfig.DownstreamClusterChangefeedConfig {
			checkpointWatcher := watcher.NewCheckpointWatcher(ctx, clusterID, downstreamClusterID, downstreamClusterChangefeedConfig.ChangefeedID, etcdClient)
			upstreamCheckpointWatchers[downstreamClusterID] = checkpointWatcher
		}
		checkpointWatchers[clusterID] = upstreamCheckpointWatchers

		s3Storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, clusterConfig.S3SinkURI)
		if err != nil {
			// Clean up already created clients before returning error
			cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)
			return nil, nil, nil, nil, errors.Trace(err)
		}
		s3Watcher := watcher.NewS3Watcher(
			watcher.NewCheckpointWatcher(ctx, clusterID, "s3", clusterConfig.S3ChangefeedID, etcdClient),
			s3Storage,
			cfg.GlobalConfig.Tables,
		)
		s3Watchers[clusterID] = s3Watcher
		pdClients[clusterID] = pdClient
	}

	return checkpointWatchers, s3Watchers, pdClients, etcdClients, nil
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
		// Clean up PD client if etcd client creation fails
		if pdClient != nil {
			pdClient.Close()
		}
		return nil, nil, errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, "default")
	if err != nil {
		// Clean up resources if CDC etcd client creation fails
		etcdCli.Close()
		pdClient.Close()
		return nil, nil, errors.Trace(err)
	}

	return pdClient, cdcEtcdClient, nil
}

// cleanupClients closes all PD and etcd clients gracefully
func cleanupClients(
	pdClients map[string]pd.Client,
	etcdClients map[string]*etcd.CDCEtcdClientImpl,
	checkpointWatchers map[string]map[string]watcher.Watcher,
	s3Watchers map[string]*watcher.S3Watcher,
) {
	log.Info("Cleaning up clients",
		zap.Int("pdClients", len(pdClients)),
		zap.Int("etcdClients", len(etcdClients)),
		zap.Int("checkpointWatchers", len(checkpointWatchers)),
		zap.Int("s3Watchers", len(s3Watchers)),
	)

	// Close PD clients
	for clusterID, pdClient := range pdClients {
		if pdClient != nil {
			pdClient.Close()
			log.Debug("PD client closed", zap.String("clusterID", clusterID))
		}
	}

	// Close etcd clients
	for clusterID, etcdClient := range etcdClients {
		if etcdClient != nil {
			if err := etcdClient.Close(); err != nil {
				log.Warn("Failed to close etcd client",
					zap.String("clusterID", clusterID),
					zap.Error(err))
			} else {
				log.Debug("Etcd client closed", zap.String("clusterID", clusterID))
			}
		}
	}

	// Close checkpoint watchers
	for _, clusterWatchers := range checkpointWatchers {
		for _, watcher := range clusterWatchers {
			watcher.Close()
		}
	}

	// Close s3 watchers
	for _, s3Watcher := range s3Watchers {
		s3Watcher.Close()
	}

	log.Info("Client cleanup completed")
}
