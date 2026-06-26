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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

type regionFeedStates map[uint64]*regionFeedState

type deregisterRequest struct {
	subID      SubscriptionID
	filterLoop bool
}

type controlQueue struct {
	ch     *chann.UnlimitedChannel[deregisterRequest, any]
	notify chan struct{}
}

func newControlQueue() *controlQueue {
	return &controlQueue{
		ch:     chann.NewUnlimitedChannelDefault[deregisterRequest](),
		notify: make(chan struct{}, 1),
	}
}

func (q *controlQueue) push(req deregisterRequest) {
	q.ch.Push(req)
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *controlQueue) tryPop() (deregisterRequest, bool) {
	if q.ch.Len() == 0 {
		return deregisterRequest{}, false
	}
	req, ok, err := q.ch.GetWithContext(context.Background())
	if err != nil {
		return deregisterRequest{}, false
	}
	return req, ok
}

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
type regionRequestWorker struct {
	workerID uint64

	store *requestedStore

	upstream        *upstreamHandle
	eventSink       *regionEventSink
	failureReporter *regionFailureReporter

	// we must always get a region to request before create a grpc stream.
	// only in this way we can avoid to try to connect to an offline store infinitely.
	preFetchForConnecting *regionReq

	// request cache with flow control
	requestCache *requestCache
	controlQueue *controlQueue

	// all regions maintained by this worker.
	requestedRegions struct {
		sync.RWMutex

		subscriptions map[SubscriptionID]regionFeedStates
	}
}

func newRegionRequestWorker(
	ctx context.Context,
	g *errgroup.Group,
	store *requestedStore,
	requestCacheSize int,
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureReporter *regionFailureReporter,
) *regionRequestWorker {
	worker := &regionRequestWorker{
		workerID:        workerIDGen.Add(1),
		store:           store,
		upstream:        upstream,
		eventSink:       eventSink,
		failureReporter: failureReporter,
		requestCache: newRequestCache(requestCacheSize, func() {
			store.promoteDeferredTask()
		}),
		controlQueue: newControlQueue(),
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	waitForPreFetching := func() error {
		if worker.preFetchForConnecting != nil {
			log.Panic("preFetchForConnecting should be nil",
				zap.Uint64("workerID", worker.workerID),
				zap.String("addr", store.storeAddr))
		}
		for {
			req, err := worker.requestCache.pop(ctx)
			if err != nil {
				return err
			}
			if req.regionInfo.isStopped() {
				req.finish()
				continue
			}
			worker.preFetchForConnecting = req
			return nil
		}
	}

	g.Go(func() error {
		for {
			if err := waitForPreFetching(); err != nil {
				return err
			}
			var regionErr error
			if err := version.CheckStoreVersion(ctx, worker.upstream.pd); err != nil {
				if errors.Cause(err) == context.Canceled {
					return nil
				}
				log.Error("event feed check store version fails",
					zap.Uint64("workerID", worker.workerID),
					zap.String("addr", worker.store.storeAddr),
					zap.Error(err))
				if cerror.Is(err, cerror.ErrGetAllStoresFailed) {
					regionErr = &getStoreErr{}
				} else {
					regionErr = &storeStreamErr{}
				}
			} else {
				if canceled := worker.run(ctx, worker.upstream.credential); canceled {
					return nil
				}
				regionErr = &storeStreamErr{}
			}
			for subID, m := range worker.clearRegionStates() {
				for _, state := range m {
					state.markStopped(regionErr)
					regionEvent := regionEvent{
						states: []*regionFeedState{state},
					}
					worker.eventSink.Push(subID, regionEvent)
				}
			}
			// The store may fail forever, so we need try to re-schedule all pending regions.
			for _, region := range worker.clearPendingRegions() {
				if region.isStopped() {
					// It means it's a special task for stopping the table.
					continue
				}
				worker.failureReporter.Report(newRegionErrorInfo(region, regionErr))
			}
			if err := util.Hang(ctx, time.Second); err != nil {
				return err
			}
		}
	})

	return worker
}

func (s *regionRequestWorker) run(ctx context.Context, credential *security.Credential) (canceled bool) {
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.store.storeAddr))

	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("canceled", canceled))
	}()

	g, gctx := errgroup.WithContext(ctx)
	conn, err := Connect(gctx, credential, s.store.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Error(err))
		// Close the connection if it was partially created to prevent goroutine leaks
		if conn != nil && conn.Conn != nil {
			_ = conn.Conn.Close()
		}
		return isCanceled()
	}
	defer func() {
		_ = conn.Conn.Close()
	}()

	g.Go(func() error {
		return s.receiveAndDispatchChangeEvents(conn)
	})
	g.Go(func() error { return s.processRegionSendTask(gctx, conn) })

	failpoint.Inject("InjectForceReconnect", func() {
		timer := time.After(10 * time.Second)
		g.Go(func() error {
			<-timer
			err := errors.New("inject force reconnect")
			log.Info("inject force reconnect", zap.Error(err))
			return err
		})
	})

	_ = g.Wait()
	return isCanceled()
}

func normalizeStreamError(err error) error {
	if StatusIsEOF(grpcstatus.Convert(err)) {
		return &storeStreamErr{}
	}
	return errors.Trace(err)
}

// receiveAndDispatchChangeEventsToProcessor receives events from the grpc stream and dispatches them to ds.
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
		state := s.getRegionState(subscriptionID, regionID)
		if state != nil {
			regionEvent := regionEvent{
				states: []*regionFeedState{state},
			}
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
				// ignore
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
				// ignore
				continue
			default:
				log.Panic("unknown event type", zap.Any("event", event))
			}
			s.eventSink.Push(subscriptionID, regionEvent)
		} else {
			switch event.Event.(type) {
			case *cdcpb.Event_Error:
				// it is normal to receive region error after deregister a subscription
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
}

func (s *regionRequestWorker) dispatchResolvedTsEvent(resolvedTsEvent *cdcpb.ResolvedTs) {
	subscriptionID := SubscriptionID(resolvedTsEvent.RequestId)
	metricsResolvedTsCount.Add(float64(len(resolvedTsEvent.Regions)))
	metrics.BatchResolvedEventSize.WithLabelValues("event-store").Observe(float64(len(resolvedTsEvent.Regions)))
	// TODO: resolvedTsEvent.Ts be 0 is impossible, we need find the root cause.
	if resolvedTsEvent.Ts == 0 {
		log.Warn("region request worker receives a resolved ts event with zero value, ignore it",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", resolvedTsEvent.RequestId),
			zap.Any("regionIDs", resolvedTsEvent.Regions))
		return
	}
	// Avoid allocating a huge states slice when resolvedTsEvent.Regions is large.
	// Push resolved-ts events in batches to reduce peak memory usage and improve GC behavior.
	const resolvedTsStateBatchSize = 1024
	capHint := len(resolvedTsEvent.Regions)
	if capHint > resolvedTsStateBatchSize {
		capHint = resolvedTsStateBatchSize
	}
	resolvedStates := make([]*regionFeedState, 0, capHint)
	flush := func() {
		if len(resolvedStates) == 0 {
			return
		}
		s.eventSink.Push(subscriptionID, regionEvent{
			resolvedTs: resolvedTsEvent.Ts,
			states:     resolvedStates,
		})
		resolvedStates = nil
	}
	for i, regionID := range resolvedTsEvent.Regions {
		if state := s.getRegionState(subscriptionID, regionID); state != nil {
			resolvedStates = append(resolvedStates, state)
			if len(resolvedStates) >= resolvedTsStateBatchSize {
				flush()
				if i+1 < len(resolvedTsEvent.Regions) {
					capHint = len(resolvedTsEvent.Regions) - (i + 1)
					if capHint > resolvedTsStateBatchSize {
						capHint = resolvedTsStateBatchSize
					}
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

// processRegionSendTask receives region requests from the channel and sends them to the remote store.
func (s *regionRequestWorker) processRegionSendTask(
	ctx context.Context,
	conn *ConnAndClient,
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
		// TODO: add a metric?
		return nil
	}
	sendDeregister := func(req deregisterRequest) error {
		changeDataReq := &cdcpb.ChangeDataRequest{
			Header:    &cdcpb.Header{ClusterId: s.upstream.clusterID, TicdcVersion: version.ReleaseSemver()},
			RequestId: uint64(req.subID),
			Request: &cdcpb.ChangeDataRequest_Deregister_{
				Deregister: &cdcpb.ChangeDataRequest_Deregister{},
			},
			FilterLoop: req.filterLoop,
		}
		if err := doSend(changeDataReq); err != nil {
			return err
		}
		for _, state := range s.takeRegionStates(req.subID) {
			state.markStopped(&requestCancelledErr{})
			regionEvent := regionEvent{
				states: []*regionFeedState{state},
			}
			s.eventSink.Push(req.subID, regionEvent)
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

	// Handle pre-fetched region first
	regionReq := s.preFetchForConnecting
	s.preFetchForConnecting = nil
	for {
		if err := drainControl(); err != nil {
			return err
		}
		if regionReq == nil {
			if regionReq = s.requestCache.tryPop(); regionReq != nil {
				continue
			}
			select {
			case <-s.controlQueue.notify:
				continue
			case <-s.requestCache.readyAvailable:
				regionReq = s.requestCache.tryPop()
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		region := regionReq.regionInfo
		subID := region.subscribedSpan.subID
		log.Debug("region request worker gets a singleRegionInfo",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("bdrMode", region.filterLoop))

		if region.isStopped() {
			regionReq.finish()
		} else if region.subscribedSpan.stopped.Load() {
			// It can be skipped directly because there must be no pending states from
			// the stopped subscribedTable, or the special singleRegionInfo for stopping
			// the table will be handled later.
			s.failureReporter.Report(newRegionErrorInfo(region, &storeStreamErr{}))
			regionReq.finish()
		} else {
			state := newRegionFeedState(region, uint64(subID), s, regionReq)
			state.start()
			s.addRegionState(subID, region.verID.GetID(), state)
			// Mark the request as sent before sending it.
			// Otherwise there is a race with the receiver goroutine:
			//  1. addRegionState makes the region visible to error handling.
			//  2. doSend sends the request.
			//  3. the receiver goroutine may receive a region error immediately.
			//  4. markStopped runs before markSent, so the request may be finished
			//     before it is marked as sent.
			//  5. the sender goroutine then calls markSent and must not make the
			//     finished request live again.
			//
			// Tracking the request before Send keeps requestedRegions and
			// request lifecycle visible in the same order and avoids leaving stale
			// requests behind.
			regionReq.markSent()
			if err := doSend(s.createRegionRequest(region)); err != nil {
				state.markStopped(err)
				return err
			}
		}
		regionReq = nil
	}
}

func (s *regionRequestWorker) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.upstream.clusterID, TicdcVersion: version.ReleaseSemver()},
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

func (s *regionRequestWorker) addRegionState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.requestedRegions.subscriptions[subscriptionID] = states
	}

	if oldState := states[regionID]; oldState != nil && oldState.request != state.request {
		log.Warn("region request state overwritten",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subscriptionID)),
			zap.Uint64("regionID", regionID))
		if oldState.request != nil {
			oldState.request.finish()
		}
	}
	states[regionID] = state
}

func (s *regionRequestWorker) getRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.RLock()
	defer s.requestedRegions.RUnlock()
	if states, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		return states[regionID]
	}
	return nil
}

func (s *regionRequestWorker) takeRegionState(subscriptionID SubscriptionID, regionID uint64) *regionFeedState {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	if statesMap, ok := s.requestedRegions.subscriptions[subscriptionID]; ok {
		state := statesMap[regionID]
		delete(statesMap, regionID)
		if len(statesMap) == 0 {
			delete(s.requestedRegions.subscriptions, subscriptionID)
		}
		return state
	}
	return nil
}

func (s *regionRequestWorker) takeRegionStates(subscriptionID SubscriptionID) regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	delete(s.requestedRegions.subscriptions, subscriptionID)
	return states
}

func (s *regionRequestWorker) clearRegionStates() map[SubscriptionID]regionFeedStates {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	subscriptions := s.requestedRegions.subscriptions
	s.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)
	return subscriptions
}

// add adds a region request to the worker's cache
// It blocks if the cache is full until there's space or ctx is cancelled
func (s *regionRequestWorker) add(ctx context.Context, region regionInfo, force bool) (bool, error) {
	return s.requestCache.add(ctx, region, force)
}

func (s *regionRequestWorker) clearPendingRegions() []regionInfo {
	var regions []regionInfo

	// Clear pre-fetched region
	if s.preFetchForConnecting != nil {
		req := s.preFetchForConnecting
		s.preFetchForConnecting = nil
		regions = append(regions, req.regionInfo)
		req.finish()
	}

	regions = append(regions, s.requestCache.takeUnsentRegions()...)
	return regions
}

func (s *regionRequestWorker) releaseAdmittedRegionRequests() {
	s.requestCache.clear()
}
