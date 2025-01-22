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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/coordinator"
	"github.com/pingcap/ticdc/coordinator/changefeed"
	logcoordinator "github.com/pingcap/ticdc/logservice/coordinator"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/tiflow/cdc/model"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.etcd.io/etcd/server/v3/mvcc"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

type elector struct {
	// election used for coordinator
	election *concurrency.Election
	// election used for log coordinator
	logElection *concurrency.Election
	svr         *server
}

func NewElector(server *server) common.SubModule {
	election := concurrency.NewElection(server.session,
		etcd.CaptureOwnerKey(server.EtcdClient.GetClusterID()))
	logElection := concurrency.NewElection(server.session,
		LogCoordinatorKey(server.EtcdClient.GetClusterID()))
	return &elector{
		election:    election,
		logElection: logElection,
		svr:         server,
	}
}

func (e *elector) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return e.campaignCoordinator(ctx) })
	g.Go(func() error { return e.campaignLogCoordinator(ctx) })
	return g.Wait()
}

func (e *elector) Name() string {
	return "elector"
}

func (e *elector) campaignCoordinator(ctx context.Context) error {
	// Limit the frequency of elections to avoid putting too much pressure on the etcd server
	rl := rate.NewLimiter(rate.Every(time.Second), 1 /* burst */)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return errors.Trace(err)
		}
		// Before campaign check liveness
		if e.svr.liveness.Load() == model.LivenessCaptureStopping {
			log.Info("do not campaign coordinator, liveness is stopping",
				zap.Any("captureID", e.svr.info.ID))
			return nil
		}
		log.Info("start to campaign coordinator", zap.Any("captureID", e.svr.info.ID))
		// Campaign to be the coordinator, it blocks until it been elected.
		err = e.election.Campaign(ctx, string(e.svr.info.ID))

		failpoint.Inject("campaign-compacted-error", func() {
			err = errors.Trace(mvcc.ErrCompacted)
		})

		if err != nil {
			cause := errors.Cause(err)
			if errors.Is(cause, context.Canceled) {
				return nil
			}
			if errors.Is(cause, mvcc.ErrCompacted) || isErrCompacted(cause) {
				log.Warn("campaign coordinator failed due to etcd revision "+
					"has been compacted, retry later", zap.Error(err))
				continue
			}
			log.Warn("campaign coordinator failed",
				zap.String("captureID", string(e.svr.info.ID)), zap.Error(err))
			return errors.ErrCaptureSuicide.GenWithStackByArgs()
		}
		// After campaign check liveness again.
		// It is possible it becomes the coordinator right after receiving SIGTERM.
		if e.svr.liveness.Load() == model.LivenessCaptureStopping {
			// If the server is stopping, resign actively.
			log.Info("resign coordinator actively, liveness is stopping")
			if resignErr := e.resign(ctx); resignErr != nil {
				log.Warn("resign coordinator actively failed",
					zap.String("captureID", string(e.svr.info.ID)), zap.Error(resignErr))
				return errors.Trace(err)
			}
			return nil
		}

		coordinatorVersion, err := e.svr.EtcdClient.GetOwnerRevision(ctx,
			model.CaptureID(e.svr.info.ID))
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("campaign coordinator successfully",
			zap.String("captureID", string(e.svr.info.ID)),
			zap.Int64("coordinatorVersion", coordinatorVersion))

		co := coordinator.New(e.svr.info,
			e.svr.pdClient, e.svr.PDClock, changefeed.NewEtcdBackend(e.svr.EtcdClient),
			e.svr.EtcdClient.GetClusterID(),
			coordinatorVersion, 10000, time.Minute)
		e.svr.setCoordinator(co)
		err = co.Run(ctx)
		// When coordinator exits, we need to stop it.
		e.svr.coordinator.AsyncStop()
		e.svr.setCoordinator(nil)
		log.Info("coordinator stop", zap.String("captureID", string(e.svr.info.ID)),
			zap.Int64("coordinatorVersion", coordinatorVersion), zap.Error(err))

		if !errors.ErrNotOwner.Equal(err) {
			// if coordinator exits, resign the coordinator key,
			// use a new context to prevent the context from being cancelled.
			resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if resignErr := e.resign(resignCtx); resignErr != nil {
				if errors.Cause(resignErr) != context.DeadlineExceeded {
					log.Info("coordinator resign failed", zap.String("captureID", string(e.svr.info.ID)),
						zap.Error(resignErr), zap.Int64("coordinatorVersion", coordinatorVersion))
					cancel()
					return errors.Trace(resignErr)
				}
				log.Warn("coordinator resign timeout", zap.String("captureID", string(e.svr.info.ID)),
					zap.Error(resignErr), zap.Int64("coordinatorVersion", coordinatorVersion))
			}
			cancel()
		}

		log.Info("coordinator resigned successfully",
			zap.String("captureID", string(e.svr.info.ID)),
			zap.Int64("coordinatorVersion", coordinatorVersion))

		// 1. If the context is cancelled by the parent context, the loop should exit at the next campaign.
		// 2. If the context is cancelled within the coordinator, it means the coordinator is exited by Resign API,
		// we should keep the loop running to try to election coordinator again.
		// So, regardless of the cause of the context.Canceled, we should not exit here.
		// We should proceed to the next iteration of the loop, allowing subsequent logic to make the determination.
		if err != nil && err != context.Canceled {
			log.Warn("run coordinator exited with error",
				zap.String("captureID", string(e.svr.info.ID)),
				zap.Int64("coordinatorVersion", coordinatorVersion),
				zap.Error(err))
			// for errors, return error and let server exits or restart
			return errors.Trace(err)
		}

		// If coordinator exits normally, continue the campaign loop and try to election coordinator again
		log.Info("run coordinator exited normally",
			zap.String("captureID", string(e.svr.info.ID)),
			zap.Int64("coordinatorVersion", coordinatorVersion),
			zap.String("error", err.Error()))
	}
}

func (e *elector) campaignLogCoordinator(ctx context.Context) error {
	// Limit the frequency of elections to avoid putting too much pressure on the etcd server
	rl := rate.NewLimiter(rate.Every(time.Second), 1 /* burst */)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		err := rl.Wait(ctx)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return nil
			}
			return errors.Trace(err)
		}
		// Before campaign check liveness
		if e.svr.liveness.Load() == model.LivenessCaptureStopping {
			log.Info("do not campaign log coordinator, liveness is stopping",
				zap.Any("captureID", e.svr.info.ID))
			return nil
		}
		// Campaign to be the log coordinator, it blocks until it been elected.
		if err = e.logElection.Campaign(ctx, string(e.svr.info.ID)); err != nil {
			cause := errors.Cause(err)
			if errors.Is(cause, context.Canceled) {
				return nil
			}
			if errors.Is(cause, mvcc.ErrCompacted) || isErrCompacted(cause) {
				log.Warn("campaign log coordinator failed due to etcd revision "+
					"has been compacted, retry later", zap.Error(err))
				continue
			}
			log.Warn("campaign log coordinator failed",
				zap.String("captureID", string(e.svr.info.ID)),
				zap.Error(err))
			return errors.ErrCaptureSuicide.GenWithStackByArgs()
		}
		// After campaign check liveness again.
		// It is possible it becomes the coordinator right after receiving SIGTERM.
		if e.svr.liveness.Load() == model.LivenessCaptureStopping {
			// If the server is stopping, resign actively.
			log.Info("resign log coordinator actively, liveness is stopping")
			if resignErr := e.resign(ctx); resignErr != nil {
				log.Warn("resign log coordinator actively failed",
					zap.String("captureID", string(e.svr.info.ID)),
					zap.Error(resignErr))
				return errors.Trace(err)
			}
			return nil
		}

		// FIXME: get log coordinator version from etcd and add it to log
		log.Info("campaign log coordinator successfully",
			zap.String("captureID", string(e.svr.info.ID)))

		co := logcoordinator.New()
		err = co.Run(ctx)

		if err != nil && !errors.Is(err, context.Canceled) {
			if !errors.ErrNotOwner.Equal(err) {
				if resignErr := e.resignLogCoordinator(); resignErr != nil {
					return errors.Trace(resignErr)
				}
			}
			log.Warn("run log coordinator exited with error",
				zap.String("captureID", string(e.svr.info.ID)),
				zap.Error(err))
			return errors.Trace(err)
		}

		// If coordinator exits normally, continue the campaign loop and try to election coordinator again
		log.Info("log coordinator exited normally",
			zap.String("captureID", string(e.svr.info.ID)),
			zap.String("error", err.Error()))
	}
}

func (e *elector) Close(_ context.Context) error {
	return nil
}

// resign lets the coordinator start a new election.
func (e *elector) resign(ctx context.Context) error {
	if e.election == nil {
		return nil
	}
	failpoint.Inject("resign-failed", func() error {
		return errors.Trace(errors.New("resign failed"))
	})
	return errors.WrapError(errors.ErrCaptureResignOwner,
		e.election.Resign(ctx))
}

// resign lets the log coordinator start a new election.
func (e *elector) resignLogCoordinator() error {
	if e.logElection == nil {
		return nil
	}
	// use a new context to prevent the context from being cancelled.
	resignCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if resignErr := e.logElection.Resign(resignCtx); resignErr != nil {
		if errors.Is(errors.Cause(resignErr), context.DeadlineExceeded) {
			log.Info("log coordinator resign failed",
				zap.String("captureID", string(e.svr.info.ID)),
				zap.Error(resignErr))
			cancel()
			return errors.Trace(resignErr)
		}

		log.Warn("log coordinator resign timeout",
			zap.String("captureID", string(e.svr.info.ID)),
			zap.Error(resignErr))
	}
	cancel()
	return nil
}

// FIXME: move the following code to the right package
var metaPrefix = "/__cdc_meta__"

func LogCoordinatorKey(clusterID string) string {
	return etcd.BaseKey(clusterID) + metaPrefix + "/log_coordinator"
}
