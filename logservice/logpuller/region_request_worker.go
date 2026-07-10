// Copyright 2023 PingCAP, Inc.
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

package logpuller

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/ticdc/utils/notifyqueue"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

const storeReconnectBackoff = time.Second

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

type deregisterRequest struct {
	subID      SubscriptionID
	filterLoop bool
}

type controlQueue struct {
	mu    sync.Mutex
	queue *notifyqueue.Queue[deregisterRequest]
}

func newControlQueue() *controlQueue {
	return &controlQueue{queue: notifyqueue.New[deregisterRequest]()}
}

func (q *controlQueue) push(req deregisterRequest) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue.Push(req)
}

func (q *controlQueue) tryPop() (deregisterRequest, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.TryPop()
}

func (q *controlQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Len()
}

func (q *controlQueue) ready() <-chan struct{} {
	return q.queue.Ready()
}

// regionRequestWorker owns one TiKV event-feed stream and the requests sent
// through it, including reconnect cleanup and subscription deregistration.
type regionRequestWorker struct {
	workerID uint64

	client *subscriptionClient
	store  *requestedStore

	requestCache *requestCache
	controlQueue *controlQueue
	tracker      *regionTracker
}

func newRegionRequestWorker(
	client *subscriptionClient,
	store *requestedStore,
	requestCache *requestCache,
) *regionRequestWorker {
	workerID := workerIDGen.Add(1)
	return &regionRequestWorker{
		workerID:     workerID,
		client:       client,
		store:        store,
		requestCache: requestCache,
		controlQueue: newControlQueue(),
		tracker:      newRegionTracker(),
	}
}

func (s *regionRequestWorker) Run(ctx context.Context) error {
	for {
		// Do not connect an idle worker to an unavailable store indefinitely.
		firstReq, err := s.requestCache.pop(ctx)
		if err != nil {
			return err
		}

		regionErr := error(&storeStreamErr{})
		if err := s.checkStoreVersion(ctx); err != nil {
			regionErr = err
		} else if err := s.runStream(ctx, firstReq); err != nil {
			regionErr = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Sent requests are owned by their states. Stopping those states removes
		// their requests before unsent requests are drained below.
		for subID, states := range s.tracker.Drain() {
			for _, state := range states {
				state.markStopped(regionErr)
				s.client.eventSink.Push(subID, regionEvent{states: []*regionFeedState{state}})
			}
		}
		for _, region := range s.requestCache.drainUnsentRegions() {
			s.client.onRegionFail(newRegionErrorInfo(region, regionErr))
		}

		if err := util.Hang(ctx, storeReconnectBackoff); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorker) checkStoreVersion(ctx context.Context) error {
	err := version.CheckStoreVersion(ctx, s.client.pd)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Error("event feed check store version fails",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.store.storeAddr),
		zap.Error(err))
	if cerror.Is(err, cerror.ErrGetAllStoresFailed) {
		return &getStoreErr{}
	}
	return &storeStreamErr{}
}

func (s *regionRequestWorker) runStream(ctx context.Context, firstReq *regionReq) (err error) {
	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.store.storeAddr))
	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
	}()

	g, gctx := errgroup.WithContext(ctx)
	conn, err := Connect(gctx, s.client.credential, s.store.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
		if conn != nil && conn.Conn != nil {
			_ = conn.Conn.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &storeStreamErr{}
	}
	defer func() { _ = conn.Conn.Close() }()

	g.Go(func() error { return s.receiveAndDispatchChangeEvents(conn) })
	g.Go(func() error { return s.processRegionSendTask(gctx, conn, firstReq) })

	failpoint.Inject("InjectForceReconnect", func() {
		timer := time.After(10 * time.Second)
		g.Go(func() error {
			<-timer
			err := errors.New("inject force reconnect")
			log.Info("inject force reconnect", zap.Error(err))
			return err
		})
	})

	err = g.Wait()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &storeStreamErr{}
	}
	return nil
}

func normalizeStreamError(err error) error {
	if StatusIsEOF(grpcstatus.Convert(err)) {
		return &storeStreamErr{}
	}
	return errors.Trace(err)
}

func (s *regionRequestWorker) receiveAndDispatchChangeEvents(conn *ConnAndClient) error {
	for {
		changeEvent, err := conn.Client.Recv()
		if err != nil {
			log.Info("region request worker receive from grpc stream failed",
				zap.Uint64("workerID", s.workerID),
				zap.String("addr", s.store.storeAddr),
				zap.String("code", grpcstatus.Code(err).String()),
				zap.Error(err))
			return normalizeStreamError(err)
		}
		if len(changeEvent.Events) > 0 {
			s.dispatchRegionChangeEvents(changeEvent.Events)
		}
		if changeEvent.ResolvedTs != nil {
			s.dispatchResolvedTsEvent(changeEvent.ResolvedTs)
		}
	}
}

func (s *regionRequestWorker) dispatchRegionChangeEvents(events []*cdcpb.Event) {
	for _, event := range events {
		regionID := event.RegionId
		subscriptionID := SubscriptionID(event.RequestId)
		state := s.tracker.Get(subscriptionID, regionID)
		if state != nil {
			regionEvent := regionEvent{states: []*regionFeedState{state}}
			switch eventData := event.Event.(type) {
			case *cdcpb.Event_Entries_:
				if eventData == nil {
					log.Warn("region request worker receives a region event with nil entries, ignore it",
						zap.Uint64("workerID", s.workerID),
						zap.Uint64("subscriptionID", uint64(subscriptionID)),
						zap.Uint64("regionID", regionID))
					continue
				}
				regionEvent.entries = eventData
			case *cdcpb.Event_Admin_:
				continue
			case *cdcpb.Event_Error:
				log.Debug("region request worker receives a region error",
					zap.Uint64("workerID", s.workerID),
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Uint64("regionID", event.RegionId),
					zap.Any("error", eventData.Error))
				state.markStopped(&eventError{err: eventData.Error})
			case *cdcpb.Event_ResolvedTs:
				regionEvent.resolvedTs = eventData.ResolvedTs
			case *cdcpb.Event_LongTxn_:
				continue
			default:
				log.Panic("unknown event type", zap.Any("event", event))
			}
			s.client.eventSink.Push(subscriptionID, regionEvent)
			continue
		}

		switch event.Event.(type) {
		case *cdcpb.Event_Error:
			log.Debug("region request worker receives an error for a stale region, ignore it",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", event.RegionId))
		default:
			log.Warn("region request worker receives a region event for an untracked region",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", event.RegionId))
		}
	}
}

func (s *regionRequestWorker) dispatchResolvedTsEvent(resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	metricsResolvedTsCount.Add(float64(len(resolvedTsEvent.Regions)))
	s.client.metrics.batchResolvedSize.Observe(float64(len(resolvedTsEvent.Regions)))
	if resolvedTsEvent.Ts == 0 {
		log.Warn("region request worker receives a resolved ts event with zero value, ignore it",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", resolvedTsEvent.RequestId),
			zap.Any("regionIDs", resolvedTsEvent.Regions))
		return
	}

	const resolvedTsStateBatchSize = 1024
	capHint := min(len(resolvedTsEvent.Regions), resolvedTsStateBatchSize)
	resolvedStates := make([]*regionFeedState, 0, capHint)
	flush := func() {
		if len(resolvedStates) == 0 {
			return
		}
		s.client.eventSink.Push(subscriptionID, regionEvent{
			resolvedTs: resolvedTsEvent.Ts,
			states:     resolvedStates,
		})
		resolvedStates = nil
	}
	for i, regionID := range resolvedTsEvent.Regions {
		if state := s.tracker.Get(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
				if i+1 < len(resolvedTsEvent.Regions) {
					capHint = min(len(resolvedTsEvent.Regions)-(i+1), resolvedTsStateBatchSize)
					resolvedStates = make([]*regionFeedState, 0, capHint)
				}
			}
			continue
		}
		log.Warn("region request worker receives a resolved ts event for an untracked region",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", regionID),
			zap.Uint64("resolvedTs", resolvedTsEvent.Ts))
	}
	flush()
}

func (s *regionRequestWorker) processRegionSendTask(
	ctx context.Context,
	conn *ConnAndClient,
	firstReq *regionReq,
) error {
	doSend := func(req *cdcpb.ChangeDataRequest) error {
		if err := conn.Client.Send(req); err != nil {
			log.Warn("region request worker send request to grpc stream failed",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", req.RequestId),
				zap.Uint64("regionID", req.RegionId),
				zap.String("addr", s.store.storeAddr),
				zap.Error(err))
			return normalizeStreamError(err)
		}
		return nil
	}
	sendDeregister := func(req deregisterRequest) error {
		changeDataReq := &cdcpb.ChangeDataRequest{
			Header:    &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
			RequestId: uint64(req.subID),
			Request: &cdcpb.ChangeDataRequest_Deregister_{
				Deregister: &cdcpb.ChangeDataRequest_Deregister{},
			},
			FilterLoop: req.filterLoop,
		}
		if err := doSend(changeDataReq); err != nil {
			return err
		}
		for _, state := range s.tracker.TakeSubscription(req.subID) {
			state.markStopped(&requestCancelledErr{})
			s.client.eventSink.Push(req.subID, regionEvent{states: []*regionFeedState{state}})
		}
		return nil
	}
	drainControl := func() error {
		for {
			req, ok := s.controlQueue.tryPop()
			if !ok {
				return nil
			}
			if err := sendDeregister(req); err != nil {
				return err
			}
		}
	}

	regionReq := firstReq
	for {
		if regionReq != nil {
			region := regionReq.regionInfo
			subID := region.subscribedSpan.subID
			log.Debug("region request worker gets a singleRegionInfo",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.String("addr", s.store.storeAddr),
				zap.Bool("bdrMode", region.filterLoop))

			if region.subscribedSpan.stopped.Load() {
				s.client.onRegionFail(newRegionErrorInfo(region, &storeStreamErr{}))
				s.requestCache.abortScan(regionReq)
			} else {
				state := newRegionFeedState(region, uint64(subID), s, regionReq)
				s.replaceRegionState(subID, region.verID.GetID(), state)
				// Make the request and its state visible in the same order. A fast
				// region error can then clean the request without racing markSent.
				s.requestCache.markSent(regionReq)
				if err := doSend(s.createRegionRequest(region)); err != nil {
					state.markStopped(err)
					return err
				}
			}
			regionReq = nil
			continue
		}

		if err := drainControl(); err != nil {
			return err
		}
		if regionReq = s.requestCache.tryPop(); regionReq != nil {
			continue
		}
		select {
		case <-s.controlQueue.ready():
		case <-s.requestCache.ready():
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *regionRequestWorker) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     region.verID.GetID(),
		RequestId:    uint64(region.subscribedSpan.subID),
		RegionEpoch:  region.rpcCtx.Meta.RegionEpoch,
		CheckpointTs: region.resolvedTs(),
		StartKey:     region.span.StartKey,
		EndKey:       region.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   region.filterLoop,
	}
}

func (s *regionRequestWorker) replaceRegionState(
	subscriptionID SubscriptionID,
	regionID uint64,
	state *regionFeedState,
) {
	oldState := s.tracker.Replace(subscriptionID, regionID, state)
	if oldState == nil {
		return
	}

	log.Warn("region request state overwritten",
		zap.Uint64("workerID", s.workerID),
		zap.Uint64("subscriptionID", uint64(subscriptionID)),
		zap.Uint64("regionID", regionID))
	oldState.abortScanIfNeeded()
}

func (s *regionRequestWorker) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	return s.requestCache.add(ctx, region, force)
}
