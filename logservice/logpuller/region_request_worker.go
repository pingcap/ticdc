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
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

// To generate a workerID in `newRegionRequestWorker`.
var workerIDGen atomic.Uint64

type regionFeedStates map[uint64]*regionFeedState

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
type regionRequestWorker struct {
	workerID uint64

<<<<<<< HEAD
	client *subscriptionClient
=======
	upstream       *upstreamHandle
	eventSink      *regionEventSink
	failureHandler *regionFailureHandler
	storeAddr      string
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))

	store *requestedStore

	// we must always get a region to request before create a grpc stream.
	// only in this way we can avoid to try to connect to an offline store infinitely.
	preFetchForConnecting *regionInfo

	// request cache with flow control
	requestCache *requestCache

	// all regions maintained by this worker.
	requestedRegions struct {
		sync.RWMutex

		subscriptions map[SubscriptionID]regionFeedStates
	}
}

func newRegionRequestWorker(
<<<<<<< HEAD
	ctx context.Context,
	client *subscriptionClient,
	credential *security.Credential,
	g *errgroup.Group,
	store *requestedStore,
	requestCacheSize int,
) *regionRequestWorker {
	worker := &regionRequestWorker{
		workerID:     workerIDGen.Add(1),
		client:       client,
		store:        store,
		requestCache: newRequestCache(requestCacheSize),
=======
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureHandler *regionFailureHandler,
	storeAddr string,
	currentWindow int,
	maxWindowMultiplier int,
) *regionRequestWorker {
	workerID := workerIDGen.Add(1)
	return &regionRequestWorker{
		workerID:       workerID,
		upstream:       upstream,
		eventSink:      eventSink,
		failureHandler: failureHandler,
		storeAddr:      storeAddr,
		admission:      newRegionAdmissionController(currentWindow, maxWindowMultiplier),
		controlQueue:   newControlQueue(),
		tracker:        newRegionTracker(),
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
				worker.requestCache.markDone()
				continue
			}
			worker.preFetchForConnecting = new(regionInfo)
			*worker.preFetchForConnecting = req.regionInfo
			return nil
		}
	}

	g.Go(func() error {
		for {
			if err := waitForPreFetching(); err != nil {
				return err
			}
			var regionErr error
			if err := version.CheckStoreVersion(ctx, worker.client.pd); err != nil {
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
				if canceled := worker.run(ctx, credential); canceled {
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
					worker.client.pushRegionEventToDS(subID, regionEvent)
				}
			}
			// The store may fail forever, so we need try to re-schedule all pending regions.
			for _, region := range worker.clearPendingRegions() {
				if region.isStopped() {
					// It means it's a special task for stopping the table.
					continue
				}
				client.onRegionFail(newRegionErrorInfo(region, regionErr))
			}
			if err := util.Hang(ctx, time.Second); err != nil {
				return err
			}
		}
	})

	return worker
}

<<<<<<< HEAD
func (s *regionRequestWorker) run(ctx context.Context, credential *security.Credential) (canceled bool) {
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
=======
func (s *regionRequestWorker) Run(ctx context.Context) error {
	handleStreamFailure := func(firstReq *regionReq, regionErr error) {
		// Stream failure handle cases:
		// - tracker: requests already sent to this stream.
		// - firstReq: popped from admission for this stream, but not necessarily
		//   added to tracker yet if the stream fails before sendRegionRequest calls
		//   tracker.Add.
		// - admission: requests owned by this worker but not sent yet.
		for _, state := range s.tracker.Drain() {
			state.markStopped(regionErr)
			s.eventSink.Push(
				SubscriptionID(state.requestID),
				regionEvent{states: []*regionFeedState{state}},
			)
		}
		// The failed stream no longer owns remote registrations.
		s.controlQueue.drain()
		if firstReq != nil && firstReq.abort() {
			s.failureHandler.Report(newRegionErrorInfo(firstReq.regionInfo, regionErr))
		}
		for _, task := range s.admission.drain() {
			s.failureHandler.Report(newRegionErrorInfo(task.regionInfo, regionErr))
		}
	}

	for {
		// Do not connect an idle worker to an unavailable store indefinitely.
		firstReq, err := s.waitForRegionRequest(ctx)
		if err != nil {
			return err
		}

		regionErr := s.runStream(ctx, firstReq)
		if ctx.Err() != nil {
			firstReq.abort()
			return ctx.Err()
		}
		// Treat an unexpected clean stream exit as a recoverable store-stream failure.
		if regionErr == nil {
			regionErr = &storeStreamErr{}
		}
		handleStreamFailure(firstReq, regionErr)
		if err := util.Hang(ctx, storeReconnectBackoff); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorker) waitForRegionRequest(ctx context.Context) (*regionReq, error) {
	// Without a stream there are no remote registrations to deregister.
	s.controlQueue.drain()
	req, err := s.admission.pop(ctx, nil)
	if err != nil {
		return nil, err
	}
	// Drop controls that raced with selecting the first request. Any later
	// controls will be handled by the stream send loop.
	s.controlQueue.drain()
	return req, nil
}

func (s *regionRequestWorker) checkStoreVersion(ctx context.Context) error {
	err := version.CheckStoreVersion(ctx, s.upstream.pd)
	if err == nil {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Error("event feed check store version fails",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.storeAddr),
		zap.Error(err))
	if cerror.Is(err, cerror.ErrGetAllStoresFailed) {
		return &getStoreErr{}
	}
	return &storeStreamErr{}
}

func (s *regionRequestWorker) runStream(ctx context.Context, firstReq *regionReq) (err error) {
	if err := s.checkStoreVersion(ctx); err != nil {
		return err
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
	}

	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
<<<<<<< HEAD
		zap.String("addr", s.store.storeAddr))

	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("canceled", canceled))
	}()

	g, gctx := errgroup.WithContext(ctx)
	conn, err := Connect(gctx, credential, s.store.storeAddr)
=======
		zap.String("addr", s.storeAddr))
	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
	}()

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	g, gctx := errgroup.WithContext(streamCtx)
	conn, err := Connect(gctx, s.upstream.credential, s.storeAddr)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
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
				zap.String("addr", s.storeAddr),
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
				continue
			default:
				log.Panic("unknown event type", zap.Any("event", event))
			}
<<<<<<< HEAD
			s.client.pushRegionEventToDS(subscriptionID, regionEvent)
=======
			s.eventSink.Push(subscriptionID, regionEvent)
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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
	s.client.metrics.batchResolvedSize.Observe(float64(len(resolvedTsEvent.Regions)))
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
<<<<<<< HEAD
		s.client.pushRegionEventToDS(subscriptionID, regionEvent{
=======
		s.eventSink.Push(subscriptionID, regionEvent{
>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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

<<<<<<< HEAD
// processRegionSendTask receives region requests from the channel and sends them to the remote store.
=======
func (s *regionRequestWorker) sendChangeDataRequest(
	conn *ConnAndClient,
	req *cdcpb.ChangeDataRequest,
) error {
	if err := conn.Client.Send(req); err != nil {
		log.Warn("region request worker send request to grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", req.RequestId),
			zap.Uint64("regionID", req.RegionId),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
		return normalizeStreamError(err)
	}
	return nil
}

func (s *regionRequestWorker) sendDeregisterRequest(
	conn *ConnAndClient,
	req deregisterRequest,
) error {
	changeDataReq := &cdcpb.ChangeDataRequest{
		Header:    &cdcpb.Header{ClusterId: s.upstream.clusterID, TicdcVersion: version.ReleaseSemver()},
		RequestId: uint64(req.subID),
		Request: &cdcpb.ChangeDataRequest_Deregister_{
			Deregister: &cdcpb.ChangeDataRequest_Deregister{},
		},
		FilterLoop: req.filterLoop,
	}
	if err := s.sendChangeDataRequest(conn, changeDataReq); err != nil {
		return err
	}
	for _, state := range s.tracker.TakeSubscription(req.subID) {
		state.markStopped(&requestCancelledErr{})
		s.eventSink.Push(req.subID, regionEvent{states: []*regionFeedState{state}})
	}
	return nil
}

func (s *regionRequestWorker) sendRegionRequest(conn *ConnAndClient, req *regionReq) error {
	if !req.isActive() {
		return nil
	}
	region := req.regionInfo
	subID := region.subscribedSpan.subID
	log.Debug("region request worker sends region request",
		zap.Uint64("workerID", s.workerID),
		zap.Uint64("subscriptionID", uint64(subID)),
		zap.Uint64("regionID", region.verID.GetID()),
		zap.String("addr", s.storeAddr),
		zap.Bool("bdrMode", region.filterLoop))

	if region.subscribedSpan.stopped.Load() {
		req.abort()
		s.failureHandler.Report(newRegionErrorInfo(region, &requestCancelledErr{}))
		return nil
	}

	// Publish the state before Send so a fast response observes its owner and
	// admission lease.
	state := newRegionFeedState(region, uint64(subID), s, req)
	if !s.tracker.Add(subID, region.verID.GetID(), state) {
		// RangeLock normally prevents duplicate active regions. Keep the existing
		// owner, including its range-lock ownership, if that invariant is ever
		// violated. Only the duplicate request's flow-control slot is released.
		state.abortScanIfNeeded()
		state.matcher.clear()
		log.Warn("duplicate active region request ignored",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()))
		return nil
	}
	if err := s.sendChangeDataRequest(conn, createRegionRequest(s.upstream.clusterID, region)); err != nil {
		// Transport failures are always recoverable at the region level. Preserve
		// the stream error as the function result, but classify the region for
		// rescheduling instead of exposing an arbitrary gRPC error downstream.
		state.markStopped(&storeStreamErr{})
		return err
	}
	return nil
}

>>>>>>> 8bd1d7e23 (logpuller: extract region request scheduler from subscription client (#5665))
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

	// Handle pre-fetched region first
	region := *s.preFetchForConnecting
	s.preFetchForConnecting = nil
	regionReq := newRegionReq(region)
	var err error
	for {
		region := regionReq.regionInfo
		subID := region.subscribedSpan.subID
		log.Debug("region request worker gets a singleRegionInfo",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", uint64(subID)),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.String("addr", s.store.storeAddr),
			zap.Bool("bdrMode", region.filterLoop))

		// It means it's a special task for stopping the table.
		if region.isStopped() {
			req := &cdcpb.ChangeDataRequest{
				Header:    &cdcpb.Header{ClusterId: s.client.clusterID, TicdcVersion: version.ReleaseSemver()},
				RequestId: uint64(subID),
				Request: &cdcpb.ChangeDataRequest_Deregister_{
					Deregister: &cdcpb.ChangeDataRequest_Deregister{},
				},
				FilterLoop: region.filterLoop,
			}
			s.requestCache.markDone()
			if err := doSend(req); err != nil {
				return err
			}
			for _, state := range s.takeRegionStates(subID) {
				state.markStopped(&requestCancelledErr{})
				regionEvent := regionEvent{
					states: []*regionFeedState{state},
				}
				s.client.pushRegionEventToDS(subID, regionEvent)
			}
		} else if region.subscribedSpan.stopped.Load() {
			// It can be skipped directly because there must be no pending states from
			// the stopped subscribedTable, or the special singleRegionInfo for stopping
			// the table will be handled later.
			s.client.onRegionFail(newRegionErrorInfo(region, &storeStreamErr{}))
			s.requestCache.markDone()
		} else {
			state := newRegionFeedState(region, uint64(subID), s)
			state.start()
			s.addRegionState(subID, region.verID.GetID(), state)
			// Mark the request as sent before sending it.
			// Otherwise there is a race with the receiver goroutine:
			//  1. addRegionState makes the region visible to error handling.
			//  2. doSend sends the request.
			//  3. the receiver goroutine may receive a region error immediately.
			//  4. markStopped runs before markSent, so requestCache.markStopped cannot
			//     find the request in sentRequests.
			//  5. the sender goroutine then calls markSent and leaves a stale sent
			//     request behind, even though the region has already been
			//     unlocked/rescheduled.
			//
			// Tracking the request before Send keeps requestedRegions and
			// sentRequests visible in the same order and avoids leaving stale
			// requests in cleanup.
			s.requestCache.markSent(regionReq)
			if err := doSend(s.createRegionRequest(region)); err != nil {
				state.markStopped(err)
				return err
			}
		}
		// Try to get from cache
		regionReq, err = s.requestCache.pop(ctx)
		if err != nil {
			return err
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

func (s *regionRequestWorker) addRegionState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	states := s.requestedRegions.subscriptions[subscriptionID]
	if states == nil {
		states = make(regionFeedStates)
		s.requestedRegions.subscriptions[subscriptionID] = states
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
		region := *s.preFetchForConnecting
		s.preFetchForConnecting = nil
		regions = append(regions, region)
		// The pre-fetched region was popped from pendingQueue but hasn't been marked as sent or done yet.
		// Release its pendingCount slot to avoid leaking flow control credits on worker failures.
		s.requestCache.markDone()
	}

	// Clear all regions from cache
	cacheRegions := s.requestCache.clear()
	regions = append(regions, cacheRegions...)
	return regions
}
