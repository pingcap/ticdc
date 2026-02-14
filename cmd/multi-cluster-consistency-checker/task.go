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
	"net/url"
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
		// Client initialisation is typically a transient (network) failure.
		return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
	}
	// Ensure cleanup happens even if there's an error
	defer cleanupClients(pdClients, etcdClients, checkpointWatchers, s3Watchers)

	if dryRun {
		log.Info("Dry-run mode: config validation and connectivity check passed, exiting")
		return nil
	}

	rec, err := recorder.NewRecorder(cfg.GlobalConfig.DataDir, cfg.Clusters, cfg.GlobalConfig.MaxReportFiles)
	if err != nil {
		if errors.Is(err, recorder.ErrCheckpointCorruption) {
			return &ExitError{Code: ExitCodeCheckpointCorruption, Err: err}
		}
		// Other recorder init errors (e.g. mkdir, readdir) are transient.
		return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
	}
	timeWindowAdvancer, checkpointDataMap, err := advancer.NewTimeWindowAdvancer(ctx, checkpointWatchers, s3Watchers, pdClients, rec.GetCheckpoint())
	if err != nil {
		return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
	}
	dataChecker := checker.NewDataChecker(ctx, cfg.Clusters, checkpointDataMap, rec.GetCheckpoint())

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
			return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
		}

		report, err := dataChecker.CheckInNextTimeWindow(newTimeWindowData)
		if err != nil {
			return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
		}

		if err := rec.RecordTimeWindow(newTimeWindowData, report); err != nil {
			return &ExitError{Code: ExitCodeTransient, Err: errors.Trace(err)}
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
		if err := validateS3ChangefeedSinkConfig(ctx, etcdClient, clusterID, clusterConfig.S3ChangefeedID, clusterConfig.S3SinkURI); err != nil {
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
// 1. The changefeed SinkURI bucket/prefix matches the configured s3SinkURI
// 2. The protocol must be canal-json
// 3. The date separator must be "day"
// 4. The file index width must be DefaultFileIndexWidth
func validateS3ChangefeedSinkConfig(ctx context.Context, etcdClient *etcd.CDCEtcdClientImpl, clusterID string, s3ChangefeedID string, s3SinkURI string) error {
	displayName := common.NewChangeFeedDisplayName(s3ChangefeedID, "default")
	cfInfo, err := etcdClient.GetChangeFeedInfo(ctx, displayName)
	if err != nil {
		return errors.Annotate(err, fmt.Sprintf("failed to get changefeed info for s3 changefeed %s in cluster %s", s3ChangefeedID, clusterID))
	}

	// 1. Validate that the changefeed's SinkURI bucket/prefix matches the configured s3SinkURI.
	// This prevents the checker from reading data that was written by a different changefeed.
	if err := validateS3BucketPrefix(cfInfo.SinkURI, s3SinkURI, clusterID, s3ChangefeedID); err != nil {
		return err
	}

	if cfInfo.Config == nil || cfInfo.Config.Sink == nil {
		return fmt.Errorf("cluster %s: s3 changefeed %s has no sink configuration", clusterID, s3ChangefeedID)
	}

	sinkConfig := cfInfo.Config.Sink

	// 2. Validate protocol must be canal-json
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

	// 3. Validate date separator must be "day"
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

	// 4. Validate file index width must be DefaultFileIndexWidth
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

// validateS3BucketPrefix checks that the changefeed's SinkURI and the
// configured s3-sink-uri point to the same S3 bucket and prefix.
// This is a critical sanity check — a mismatch means the checker would
// read data from a different location than where the changefeed writes.
func validateS3BucketPrefix(changefeedSinkURI, configS3SinkURI, clusterID, s3ChangefeedID string) error {
	cfURL, err := url.Parse(changefeedSinkURI)
	if err != nil {
		return fmt.Errorf("cluster %s: s3 changefeed %s has invalid sink URI %q: %v",
			clusterID, s3ChangefeedID, changefeedSinkURI, err)
	}
	cfgURL, err := url.Parse(configS3SinkURI)
	if err != nil {
		return fmt.Errorf("cluster %s: configured s3-sink-uri %q is invalid: %v",
			clusterID, configS3SinkURI, err)
	}

	// Compare scheme (s3, gcs, …), bucket (Host) and prefix (Path).
	// Path is normalized by trimming trailing slashes so that
	// "s3://bucket/prefix" and "s3://bucket/prefix/" are considered equal.
	cfScheme := strings.ToLower(cfURL.Scheme)
	cfgScheme := strings.ToLower(cfgURL.Scheme)
	cfBucket := cfURL.Host
	cfgBucket := cfgURL.Host
	cfPrefix := strings.TrimRight(cfURL.Path, "/")
	cfgPrefix := strings.TrimRight(cfgURL.Path, "/")

	if cfScheme != cfgScheme || cfBucket != cfgBucket || cfPrefix != cfgPrefix {
		return fmt.Errorf("cluster %s: s3 changefeed %s sink URI bucket/prefix mismatch: "+
			"changefeed has %s://%s%s but config has %s://%s%s",
			clusterID, s3ChangefeedID,
			cfScheme, cfBucket, cfURL.Path,
			cfgScheme, cfgBucket, cfgURL.Path)
	}
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
