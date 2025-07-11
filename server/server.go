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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/dispatchermanager"
	"github.com/pingcap/ticdc/downstreamadapter/dispatcherorchestrator"
	"github.com/pingcap/ticdc/downstreamadapter/eventcollector"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/logpuller"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/logservice/txnutil"
	"github.com/pingcap/ticdc/maintainer"
	"github.com/pingcap/ticdc/pkg/api"
	"github.com/pingcap/ticdc/pkg/common"
	appctx "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/pingcap/ticdc/pkg/security"
	tiserver "github.com/pingcap/ticdc/pkg/server"
	"github.com/pingcap/ticdc/pkg/tcpserver"
	"github.com/pingcap/ticdc/pkg/upstream"
	"github.com/pingcap/ticdc/server/watcher"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	closeServiceTimeout  = 15 * time.Second
	cleanMetaDuration    = 10 * time.Second
	oldArchCheckInterval = 100 * time.Millisecond
)

type server struct {
	// mu is used to protect the server's Run method
	mu sync.Mutex

	info *node.Info

	liveness api.Liveness

	pdClient        pd.Client
	pdAPIClient     pdutil.PDAPIClient
	pdEndpoints     []string
	coordinatorMu   sync.Mutex
	coordinator     tiserver.Coordinator
	upstreamManager *upstream.Manager

	// session keeps alive between the server and etcd
	session *concurrency.Session

	security *security.Credential

	EtcdClient etcd.CDCEtcdClient

	KVStorage kv.Storage
	PDClock   pdutil.Clock

	tcpServer tcpserver.TCPServer

	// preServices is the preServices will be start before the server is running
	// And will be closed when the server is closing
	preServices []common.Closeable
	// subCommonModules contains common modules that start after PreServices.
	// These modules will be closed when the server shuts down.
	// These are shared modules across all components that:
	// 1. Can coexist with old architecture components
	// 2. Can guide the old architecture components offline
	// 3. Must start before subModules
	subCommonModules []common.SubModule
	// subModules contains modules that will be started after PreServices are started
	// and will be closed when the server is closing.
	// These modules must not start while old-architecture servers are still online
	// to avoid compatibility issues and unexpected behavior.
	subModules []common.SubModule

	closed atomic.Bool
}

// New returns a new Server instance
func New(conf *config.ServerConfig, pdEndpoints []string) (tiserver.Server, error) {
	// This is to make communication between nodes possible.
	// In other words, the nodes have to trust each other.
	if len(conf.Security.CertAllowedCN) != 0 {
		err := conf.Security.AddSelfCommonName()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	// tcpServer is the unified frontend of the CDC server that serves
	// both RESTful APIs and gRPC APIs.
	// Note that we pass the TLS config to the tcpServer, so there is no need to
	// configure TLS elsewhere.
	tcpServer, err := tcpserver.NewTCPServer(conf.Addr, conf.Security)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s := &server{
		pdEndpoints: pdEndpoints,
		tcpServer:   tcpServer,
		security:    conf.Security,
		preServices: make([]common.Closeable, 0),
	}
	return s, nil
}

// initialize the server before run it.
func (c *server) initialize(ctx context.Context) error {
	if err := c.prepare(ctx); err != nil {
		log.Error("server prepare failed", zap.Any("server", c.info), zap.Error(err))
		return errors.Trace(err)
	}

	if err := c.setPreServices(ctx); err != nil {
		log.Error("server set pre services failed", zap.Any("server", c.info), zap.Error(err))
		return errors.Trace(err)
	}

	nodeManager := watcher.NewNodeManager(c.session, c.EtcdClient)
	nodeManager.RegisterNodeChangeHandler(
		appctx.MessageCenter,
		appctx.GetService[messaging.MessageCenter](appctx.MessageCenter).OnNodeChanges)

	conf := config.GetGlobalServerConfig()
	subscriptionClient := logpuller.NewSubscriptionClient(
		&logpuller.SubscriptionClientConfig{
			RegionRequestWorkerPerStore: 8,
		}, c.pdClient,
		txnutil.NewLockerResolver(c.KVStorage.(tikv.Storage)), c.security,
	)
	schemaStore := schemastore.New(ctx, conf.DataDir, subscriptionClient, c.pdClient, c.KVStorage)
	eventStore := eventstore.New(ctx, conf.DataDir, subscriptionClient)
	eventService := eventservice.New(eventStore, schemaStore)
	c.upstreamManager = upstream.NewManager(ctx, upstream.NodeTopologyCfg{
		Info:        c.info,
		GCServiceID: c.EtcdClient.GetGCServiceID(),
		SessionTTL:  int64(conf.CaptureSessionTTL),
	})
	_, err := c.upstreamManager.AddDefaultUpstream(c.pdEndpoints, conf.Security, c.pdClient, c.EtcdClient.GetEtcdClient())
	if err != nil {
		return errors.Trace(err)
	}

	c.subCommonModules = []common.SubModule{
		nodeManager,
		NewElector(c),
		NewHttpServer(c, c.tcpServer.HTTP1Listener()),
		NewGrpcServer(c.tcpServer.GrpcListener()),
	}

	c.subModules = []common.SubModule{
		subscriptionClient,
		schemaStore,
		maintainer.NewMaintainerManager(c.info, conf.Debug.Scheduler),
		eventStore,
		eventService,
	}
	// register it into global var
	for _, subCommonModule := range c.subCommonModules {
		appctx.SetService(subCommonModule.Name(), subCommonModule)
	}
	for _, subModule := range c.subModules {
		appctx.SetService(subModule.Name(), subModule)
	}
	return nil
}

// setPreServices sets the preServices
func (c *server) setPreServices(ctx context.Context) error {
	// Set ID to Global Context
	appctx.SetID(c.info.ID.String())

	// Set PDClock to Global Context
	var err error
	c.PDClock, err = pdutil.NewClock(ctx, c.pdClient)
	if err != nil {
		return errors.Trace(err)
	}
	c.PDClock.Run(ctx)
	appctx.SetService(appctx.DefaultPDClock, c.PDClock)
	c.preServices = append(c.preServices, c.PDClock)
	// Set MessageCenter to Global Context
	mcCfg := config.NewDefaultMessageCenterConfig(c.info.AdvertiseAddr)
	messageCenter := messaging.NewMessageCenter(ctx, c.info.ID, mcCfg, c.security)
	messageCenter.Run(ctx)
	appctx.SetService(appctx.MessageCenter, messageCenter)
	c.preServices = append(c.preServices, messageCenter)

	// Set EventCollector to Global Context
	ec := eventcollector.New(c.info.ID)
	ec.Run(ctx)
	appctx.SetService(appctx.EventCollector, ec)
	c.preServices = append(c.preServices, ec)

	// Set HeartbeatCollector to Global Context
	hc := dispatchermanager.NewHeartBeatCollector(c.info.ID)
	hc.Run(ctx)
	appctx.SetService(appctx.HeartbeatCollector, hc)
	c.preServices = append(c.preServices, hc)

	// Set DispatcherOrchestrator to Global Context
	dispatcherOrchestrator := dispatcherorchestrator.New()
	appctx.SetService(appctx.DispatcherOrchestrator, dispatcherOrchestrator)
	c.preServices = append(c.preServices, dispatcherOrchestrator)

	log.Info("pre services all set", zap.Any("preServicesNum", len(c.preServices)))
	return nil
}

// Run runs the server
func (c *server) Run(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	err := c.initialize(ctx)
	if err != nil {
		log.Error("init server failed", zap.Error(err))
		return errors.Trace(err)
	}

	g, ctx := errgroup.WithContext(ctx)
	// start tcp server
	g.Go(func() error {
		log.Info("tcp server start to run")
		err := c.tcpServer.Run(ctx)
		if err != nil {
			log.Error("tcp server exited", zap.Error(errors.Trace(err)))
		}
		return nil
	})

	log.Info("server initialized", zap.Any("server", c.info))
	// start all submodules
	for _, sub := range c.subCommonModules {
		func(m common.SubModule) {
			g.Go(func() error {
				log.Info("starting sub common module", zap.String("module", m.Name()))
				defer log.Info("sub common module exited", zap.String("module", m.Name()))
				return m.Run(ctx)
			})
		}(sub)
	}

	// check the environment is valid to start the server
	err = c.validCheck(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// start all submodules
	for _, sub := range c.subModules {
		func(m common.SubModule) {
			g.Go(func() error {
				log.Info("starting sub module", zap.String("module", m.Name()))
				defer log.Info("sub module exited", zap.String("module", m.Name()))
				return m.Run(ctx)
			})
		}(sub)
	}
	// register server to etcd after we started all modules
	err = c.registerNodeToEtcd(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return g.Wait()
}

// validCheck checks whether the environment is valid to start the server
// return only when all the old-arch cdc capture is not running
// old-arch cdc capture will return when receive the unknown etcd key
// such as the election key for logCoordinator in func `LogCoordinatorKey()`
func (c *server) validCheck(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			// check whether the old-arch capture is running
			_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			oldArchCaptureRunning := false
			for _, captureInfo := range captureInfos {
				if !captureInfo.IsNewArch {
					log.Info("old-arch capture is running, server will not start", zap.String("captureID", captureInfo.ID))
					oldArchCaptureRunning = true
					break
				}
			}
			if !oldArchCaptureRunning {
				log.Info("new arch server is valid to start")
				return nil
			}
			time.Sleep(oldArchCheckInterval)
		}
	}
}

// SelfInfo gets the server info
func (c *server) SelfInfo() (*node.Info, error) {
	// when c.reset has not been called yet, c.info is nil.
	if c.info != nil {
		return c.info, nil
	}
	return nil, errors.ErrCaptureNotInitialized.GenWithStackByArgs()
}

func (c *server) setCoordinator(co tiserver.Coordinator) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	c.coordinator = co
}

// GetCoordinator returns coordinator if it is the coordinator.
func (c *server) GetCoordinator() (tiserver.Coordinator, error) {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	if c.coordinator == nil {
		return nil, errors.ErrNotOwner.GenWithStackByArgs()
	}
	return c.coordinator, nil
}

// Close closes the server by deregister it from etcd,
// it also closes the coordinator and processorManager
// Note: this function should be reentrant
func (c *server) Close(ctx context.Context) {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	log.Info("server closing", zap.Any("ServerInfo", c.info))
	// Safety: Here we mainly want to stop the coordinator
	// and ignore it if the coordinator does not exist or is not set.
	o, _ := c.GetCoordinator()
	if o != nil {
		o.Stop()
		log.Info("coordinator closed", zap.String("captureID", string(c.info.ID)))
	}

	var closeGroup sync.WaitGroup
	closeGroup.Add(1)
	go func() {
		defer closeGroup.Done()
		c.closePreServices()
	}()

	for _, subModule := range c.subModules {
		if err := subModule.Close(ctx); err != nil {
			log.Warn("failed to close sub module",
				zap.String("module", subModule.Name()),
				zap.Error(err))
		}
		log.Info("sub module closed", zap.String("module", subModule.Name()))
	}

	for _, subCommonModule := range c.subCommonModules {
		if err := subCommonModule.Close(ctx); err != nil {
			log.Warn("failed to close sub common module",
				zap.String("module", subCommonModule.Name()),
				zap.Error(err))
		}
		log.Info("sub common module closed", zap.String("module", subCommonModule.Name()))
	}

	// delete server info from etcd
	timeoutCtx, cancel := context.WithTimeout(context.Background(), cleanMetaDuration)
	defer cancel()
	if err := c.EtcdClient.DeleteCaptureInfo(timeoutCtx, string(c.info.ID)); err != nil {
		log.Warn("failed to delete server info when server exited",
			zap.String("captureID", string(c.info.ID)),
			zap.Error(err))
	} else {
		log.Info("server info deleted from etcd", zap.String("captureID", string(c.info.ID)))
	}

	closeGroup.Wait()
	log.Info("server closed", zap.Any("ServerInfo", c.info))
}

func (c *server) closePreServices() {
	closeCtx, cancel := context.WithTimeout(context.Background(), closeServiceTimeout)
	defer cancel()
	done := make(chan struct{})
	go func() {
		// close preServices in reverse order
		for idx := len(c.preServices) - 1; idx >= 0; idx-- {
			c.preServices[idx].Close()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-closeCtx.Done():
		log.Warn("service close operation timed out", zap.Error(closeCtx.Err()))
	}
}

// Liveness returns liveness of the server.
func (c *server) Liveness() api.Liveness {
	return c.liveness.Load()
}

// IsCoordinator returns whether the server is an coordinator
func (c *server) IsCoordinator() bool {
	c.coordinatorMu.Lock()
	defer c.coordinatorMu.Unlock()
	return c.coordinator != nil
}

func (c *server) GetPdClient() pd.Client {
	return c.pdClient
}

// GetCoordinatorInfo return the controller server info of current TiCDC cluster
func (c *server) GetCoordinatorInfo(ctx context.Context) (*node.Info, error) {
	_, captureInfos, err := c.EtcdClient.GetCaptures(ctx)
	if err != nil {
		return nil, err
	}

	coordinatorID, err := c.EtcdClient.GetOwnerID(ctx)
	if err != nil {
		return nil, err
	}

	for _, captureInfo := range captureInfos {
		if captureInfo.ID == coordinatorID {
			res := &node.Info{
				ID:            node.ID(captureInfo.ID),
				AdvertiseAddr: captureInfo.AdvertiseAddr,

				Version:        captureInfo.Version,
				DeployPath:     captureInfo.DeployPath,
				StartTimestamp: captureInfo.StartTimestamp,

				// Epoch is now not used in TiCDC, so we just set it to 0.
				Epoch: 0,
			}
			return res, nil
		}
	}
	return nil, errors.ErrOwnerNotFound.FastGenByArgs()
}

func isErrCompacted(err error) bool {
	return strings.Contains(err.Error(), "required revision has been compacted")
}

func (c *server) GetEtcdClient() etcd.CDCEtcdClient {
	return c.EtcdClient
}

func (c *server) GetMaintainerManager() *maintainer.Manager {
	return appctx.GetService[*maintainer.Manager](appctx.MaintainerManager)
}

func (c *server) GetKVStorage() kv.Storage {
	return c.KVStorage
}
