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
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/advancer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/checker"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/config"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/pingcap/ticdc/pkg/common"
	cdcconfig "github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func runTask(ctx context.Context, cfg *config.Config, dryRun bool) error {
	checkpointWatchers, s3Watchers, pdClients, etcdClients, err := initClients(ctx, cfg)
	if err != nil {
		return errors.Trace(err)
	}
	// Ensure cleanup happens even if there's an error
	defer cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)

	if dryRun {
		log.Info("Dry-run mode: config validation and connectivity check passed, exiting")
		return nil
	}

	recorder, err := recorder.NewRecorder(cfg.GlobalConfig.DataDir, cfg.Clusters, cfg.GlobalConfig.MaxReportFiles)
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

		report, err := dataChecker.CheckInNextTimeWindow(newTimeWindowData)
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
		pdClient, etcdClient, err := newPDClient(ctx, clusterConfig.PDAddrs, &clusterConfig.SecurityConfig)
		if err != nil {
			// Clean up already created clients before returning error
			cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)
			return nil, nil, nil, nil, errors.Trace(err)
		}
		etcdClients[clusterID] = etcdClient

		clusterCheckpointWatchers := make(map[string]watcher.Watcher)
		for peerClusterID, peerClusterChangefeedConfig := range clusterConfig.PeerClusterChangefeedConfig {
			checkpointWatcher := watcher.NewCheckpointWatcher(ctx, clusterID, peerClusterID, peerClusterChangefeedConfig.ChangefeedID, etcdClient)
			clusterCheckpointWatchers[peerClusterID] = checkpointWatcher
		}
		checkpointWatchers[clusterID] = clusterCheckpointWatchers

		// Validate s3 changefeed sink config from etcd
		if err := validateS3ChangefeedSinkConfig(ctx, etcdClient, clusterID, clusterConfig.S3ChangefeedID); err != nil {
			cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)
			return nil, nil, nil, nil, errors.Trace(err)
		}

		s3Storage, err := util.GetExternalStorageWithDefaultTimeout(ctx, clusterConfig.S3SinkURI)
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

// validateS3ChangefeedSinkConfig fetches the changefeed info from etcd and validates that:
// 1. The protocol must be canal-json
// 2. The date separator must be "day"
// 3. The file index width must be DefaultFileIndexWidth
func validateS3ChangefeedSinkConfig(ctx context.Context, etcdClient *etcd.CDCEtcdClientImpl, clusterID string, s3ChangefeedID string) error {
	displayName := common.NewChangeFeedDisplayName(s3ChangefeedID, "default")
	cfInfo, err := etcdClient.GetChangeFeedInfo(ctx, displayName)
	if err != nil {
		return errors.Annotate(err, fmt.Sprintf("failed to get changefeed info for s3 changefeed %s in cluster %s", s3ChangefeedID, clusterID))
	}

	if cfInfo.Config == nil || cfInfo.Config.Sink == nil {
		return fmt.Errorf("cluster %s: s3 changefeed %s has no sink configuration", clusterID, s3ChangefeedID)
	}

	sinkConfig := cfInfo.Config.Sink

	// 1. Validate protocol must be canal-json
	protocolStr := strings.ToLower(util.GetOrZero(sinkConfig.Protocol))
	if protocolStr == "" {
		return fmt.Errorf("cluster %s: s3 changefeed %s has no protocol configured in sink config", clusterID, s3ChangefeedID)
	}
	protocol, err := cdcconfig.ParseSinkProtocolFromString(protocolStr)
	if err != nil {
		return errors.Annotate(err, fmt.Sprintf("cluster %s: s3 changefeed %s has invalid protocol", clusterID, s3ChangefeedID))
	}
	if protocol != cdcconfig.ProtocolCanalJSON {
		return fmt.Errorf("cluster %s: s3 changefeed %s protocol is %q, but only %q is supported",
			clusterID, s3ChangefeedID, protocolStr, cdcconfig.ProtocolCanalJSON.String())
	}

	// 2. Validate date separator must be "day"
	dateSeparatorStr := util.GetOrZero(sinkConfig.DateSeparator)
	if dateSeparatorStr == "" {
		dateSeparatorStr = cdcconfig.DateSeparatorNone.String()
	}
	var dateSep cdcconfig.DateSeparator
	if err := dateSep.FromString(dateSeparatorStr); err != nil {
		return errors.Annotate(err, fmt.Sprintf("cluster %s: s3 changefeed %s has invalid date-separator %q", clusterID, s3ChangefeedID, dateSeparatorStr))
	}
	if dateSep != cdcconfig.DateSeparatorDay {
		return fmt.Errorf("cluster %s: s3 changefeed %s date-separator is %q, but only %q is supported",
			clusterID, s3ChangefeedID, dateSep.String(), cdcconfig.DateSeparatorDay.String())
	}

	// 3. Validate file index width must be DefaultFileIndexWidth
	fileIndexWidth := util.GetOrZero(sinkConfig.FileIndexWidth)
	if fileIndexWidth != cdcconfig.DefaultFileIndexWidth {
		return fmt.Errorf("cluster %s: s3 changefeed %s file-index-width is %d, but only %d is supported",
			clusterID, s3ChangefeedID, fileIndexWidth, cdcconfig.DefaultFileIndexWidth)
	}

	log.Info("Validated s3 changefeed sink config from etcd",
		zap.String("clusterID", clusterID),
		zap.String("s3ChangefeedID", s3ChangefeedID),
		zap.String("protocol", protocolStr),
		zap.String("dateSeparator", dateSep.String()),
		zap.Int("fileIndexWidth", fileIndexWidth),
	)

	return nil
}

func newPDClient(ctx context.Context, pdAddrs []string, securityConfig *security.Credential) (pd.Client, *etcd.CDCEtcdClientImpl, error) {
	pdClient, err := pd.NewClientWithContext(
		ctx, "consistency-checker", pdAddrs, securityConfig.PDSecurityOption(),
		pdopt.WithCustomTimeoutOption(10*time.Second),
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	etcdCli, err := etcd.CreateRawEtcdClient(securityConfig, grpc.EmptyDialOption{}, pdAddrs...)
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
