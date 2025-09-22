// Copyright 2024 PingCAP, Inc.
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

package server

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/pingcap/log"
	appctx "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/fsutil"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	defaultDataDir = "/tmp/cdc_data"
	// dataDirThreshold is used to warn if the free space of the specified data-dir is lower than it, unit is GB
	dataDirThreshold = 500
)

func (c *server) prepare(ctx context.Context) error {
	conf := config.GetGlobalServerConfig()
	grpcTLSOption, err := conf.Security.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}
	log.Info("create pd client", zap.Strings("endpoints", c.pdEndpoints))
	c.pdClient, err = pd.NewClientWithContext(
		ctx, "cdc-server", c.pdEndpoints, conf.Security.PDSecurityOption(),
		// the default `timeout` is 3s, maybe too small if the pd is busy,
		// set to 10s to avoid frequent timeout.
		pdopt.WithCustomTimeoutOption(10*time.Second),
		pdopt.WithGRPCDialOptions(
			grpcTLSOption,
			grpc.WithBlock(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
		),
		pdopt.WithForwardingOption(config.EnablePDForwarding))
	if err != nil {
		return errors.Trace(err)
	}
	pdAPIClient, err := pdutil.NewPDAPIClient(c.pdClient, conf.Security)
	if err != nil {
		return errors.Trace(err)
	}
	c.pdAPIClient = pdAPIClient
	appctx.SetService(appctx.PDAPIClient, pdAPIClient)
	log.Info("create etcdCli", zap.Strings("endpoints", c.pdEndpoints))
	// we do not pass a `context` to create an etcd client,
	// to prevent it's cancelled when the server is closing.
	// For example, when the non-owner watcher goes offline,
	// it would resign the campaign key which was put by call `campaign`,
	// if this is not done due to the passed context cancelled,
	// the key will be kept for the lease TTL, which is 10 seconds,
	// then cause the new owner cannot be elected immediately after the old owner offline.
	// see https://github.com/etcd-io/etcd/blob/525d53bd41/client/v3/concurrency/election.go#L98
	etcdCli, err := etcd.CreateRawEtcdClient(conf.Security, grpcTLSOption, c.pdEndpoints...)
	if err != nil {
		return errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, conf.ClusterID)
	if err != nil {
		return errors.Trace(err)
	}
	c.EtcdClient = cdcEtcdClient

	// Collect all endpoints from pd here to make the server more robust.
	// Because in some scenarios, the deployer may only provide one pd endpoint,
	// this will cause the TiCDC server to fail to restart when some pd watcher is down.
	allPDEndpoints, err := pdAPIClient.CollectMemberEndpoints(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	c.pdEndpoints = append(c.pdEndpoints, allPDEndpoints...)

	// Update meta-region label to ensure that meta region isolated from data regions.
	err = pdAPIClient.UpdateMetaLabel(ctx)
	if err != nil {
		log.Warn("Fail to verify region label rule",
			zap.Error(err),
			zap.String("advertiseAddr", conf.AdvertiseAddr),
			zap.Strings("upstreamEndpoints", c.pdEndpoints))
	}

	appctx.SetService(appctx.RegionCache, tikv.NewRegionCache(c.pdClient))

	if err = c.initDir(); err != nil {
		return errors.Trace(err)
	}
	c.setMemoryLimit()

	session, err := c.newEtcdSession(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	deployPath, err := os.Executable()
	if err != nil {
		deployPath = ""
	}
	// TODO: Get id from disk after restart.
	c.info = node.NewInfo(conf.AdvertiseAddr, deployPath)
	c.session = session
	return nil
}

func calcMemoryLimit(percentage float64) int64 {
	serverMemoryLimit, err := memory.MemTotal()
	if err != nil {
		log.Error("get system total memory fail", zap.Error(err))
		return math.MaxInt64
	}
	memoryLimit := int64(float64(serverMemoryLimit) * percentage) // `server_memory_limit` * `gc_limit_percentage`
	if memoryLimit == 0 {
		memoryLimit = math.MaxInt64
	}
	return memoryLimit
}

func (c *server) setMemoryLimit() {
	conf := config.GetGlobalServerConfig()
	if conf.MemoryLimitPercentage > 0 {
		memoryLimit := calcMemoryLimit(conf.MemoryLimitPercentage)
		debug.SetMemoryLimit(memoryLimit)
		log.Info("ticdc server set memory limit", zap.Int64("memoryLimit", memoryLimit))
	}
}

func (c *server) initDir() error {
	c.setUpDir()
	conf := config.GetGlobalServerConfig()
	// Ensure data dir exists and read-writable.
	diskInfo, err := checkDir(conf.DataDir)
	if err != nil {
		return errors.Trace(err)
	}
	log.Info(fmt.Sprintf("%s is set as data-dir (%dGB available), sort-dir=%s. "+
		"It is recommended that the disk for data-dir at least have %dGB available space",
		conf.DataDir, diskInfo.Avail, conf.Sorter.SortDir, dataDirThreshold))

	// Ensure sorter dir exists and read-writable.
	_, err = checkDir(conf.Sorter.SortDir)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *server) setUpDir() {
	conf := config.GetGlobalServerConfig()
	if conf.DataDir == "" {
		conf.DataDir = defaultDataDir
	}
	conf.Sorter.SortDir = filepath.Join(conf.DataDir, config.DefaultSortDir)
	config.StoreGlobalServerConfig(conf)
}

// registerNodeToEtcd the server by put the server's information in etcd
func (c *server) registerNodeToEtcd(ctx context.Context) error {
	cInfo := &config.CaptureInfo{
		ID:             config.CaptureID(c.info.ID),
		AdvertiseAddr:  c.info.AdvertiseAddr,
		Version:        c.info.Version,
		GitHash:        c.info.GitHash,
		DeployPath:     c.info.DeployPath,
		StartTimestamp: c.info.StartTimestamp,
		IsNewArch:      true,
	}
	err := c.EtcdClient.PutCaptureInfo(ctx, cInfo, c.session.Lease())
	if err != nil {
		return errors.WrapError(errors.ErrCaptureRegister, err)
	}
	return nil
}

func (c *server) newEtcdSession(ctx context.Context) (*concurrency.Session, error) {
	cfg := config.GetGlobalServerConfig()
	lease, err := c.EtcdClient.GetEtcdClient().Grant(ctx, int64(cfg.CaptureSessionTTL))
	if err != nil {
		return nil, errors.Trace(err)
	}
	sess, err := c.EtcdClient.GetEtcdClient().NewSession(concurrency.WithLease(lease.ID))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sess, nil
}

func checkDir(dir string) (*fsutil.DiskInfo, error) {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := fsutil.IsDirReadWritable(dir); err != nil {
		return nil, errors.Trace(err)
	}
	return fsutil.GetDiskInfo(dir)
}
