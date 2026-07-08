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
	"strconv"
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
	return &controlQueue{
		queue: notifyqueue.New[deregisterRequest](),
	}
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

// regionRequestWorker is responsible for sending region requests to a specific TiKV store.
type regionRequestWorker struct {
	workerID  uint64
	storeAddr string

	upstream       *upstreamHandle
	eventSink      *regionEventSink
	failureHandler *regionFailureHandler
	memoryQuota    *memoryQuotaController

	// request cache with flow control
	requestCache *requestCache
	controlQueue *controlQueue

	tracker *regionTracker
}

func newRegionRequestWorker(
	storeAddr string,
	requestCache *requestCache,
	upstream *upstreamHandle,
	eventSink *regionEventSink,
	failureHandler *regionFailureHandler,
	memoryQuota *memoryQuotaController,
) *regionRequestWorker {
	workerID := workerIDGen.Add(1)
	worker := &regionRequestWorker{
		workerID:       workerID,
		storeAddr:      storeAddr,
		upstream:       upstream,
		eventSink:      eventSink,
		failureHandler: failureHandler,
		memoryQuota:    memoryQuota,
		requestCache:   requestCache,
		controlQueue:   newControlQueue(),
		tracker:        newRegionTracker(workerID),
	}
	return worker
}

func (s *regionRequestWorker) Run(ctx context.Context) error {
	for {
		// Wait for one data request before connecting, so an idle worker does
		// not keep reconnecting to an unavailable store.
		firstReq, err := s.waitForRegion(ctx)
		if err != nil {
			return err
		}

		// Run one stream session. A clean stream exit is still treated as a
		// store failure so tracked regions can be retried.
		var regionErr error = &storeStreamErr{}
		if err := s.checkStoreVersion(ctx); err != nil {
			regionErr = err
		} else if err := s.runStream(ctx, firstReq); err != nil {
			regionErr = err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Stop all tracked regions for this stream and send events so the event
		// handler can reschedule them through the normal failure path.
		for subID, states := range s.tracker.Drain() {
			for _, state := range states {
				state.markStopped(regionErr)
				regionEvent := regionEvent{
					states: []*regionFeedState{state},
				}
				s.eventSink.Push(subID, regionEvent)
			}
		}
		// Unsent regions are not tracked yet, so report them through the
		// failure path explicitly.
		for _, region := range s.requestCache.drainUnsentRegions() {
			s.failureHandler.Report(newRegionErrorInfo(region, regionErr))
		}

		// Avoid a tight reconnect loop when the store keeps failing.
		if err := util.Hang(ctx, storeReconnectBackoff); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorker) waitForRegion(ctx context.Context) (*regionReq, error) {
	req, err := s.requestCache.pop(ctx)
	if err != nil {
		return nil, err
	}
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
	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
		zap.String("addr", s.storeAddr))

	defer func() {
		log.Info("region request worker exits",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
	}()

	g, gctx := errgroup.WithContext(ctx)
	conn, err := Connect(gctx, s.upstream.credential, s.storeAddr)
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
			zap.Error(err))
		// Close the connection if it was partially created to prevent goroutine leaks
		if conn != nil && conn.Conn != nil {
			_ = conn.Conn.Close()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &storeStreamErr{}
	}
	defer func() {
		_ = conn.Conn.Close()
	}()

	g.Go(func() error {
		return s.receiveAndDispatchChangeEvents(conn)
	})
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
		state := s.tracker.Get(subscriptionID, regionID)
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
		if state := s.tracker.Get(subscriptionID, regionID); state != nil {
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
	firstReq *regionReq,
) error {
	doSend := func(req *cdcpb.ChangeDataRequest) error {
		if err := conn.Client.Send(req); err != nil {
			log.Warn("region request worker send request to grpc stream failed",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", req.RequestId),
				zap.Uint64("regionID", req.RegionId),
				zap.String("addr", s.storeAddr),
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
		for _, state := range s.tracker.RemoveSubscription(req.subID) {
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

	regionReq := firstReq
	for {
		if regionReq != nil {
			region := regionReq.regionInfo
			subID := region.subscribedSpan.subID
			log.Debug("region request worker gets a singleRegionInfo",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subID)),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.String("addr", s.storeAddr),
				zap.Bool("bdrMode", region.filterLoop))

			if region.subscribedSpan.stopped.Load() {
				// The subscription has been stopped before this queued request is sent.
				s.failureHandler.Report(newRegionErrorInfo(region, &storeStreamErr{}))
				s.requestCache.abortScan(regionReq)
			} else {
				state := newRegionFeedState(region, uint64(subID), s, regionReq)
				s.tracker.Track(subID, region.verID.GetID(), state)
				// Mark the request as sent before sending it.
				// Otherwise there is a race with the receiver goroutine:
				//  1. tracker.Track makes the region visible to error handling.
				//  2. doSend sends the request.
				//  3. the receiver goroutine may receive a region error immediately.
				//  4. markStopped runs before markSent, so the request may be finished
				//     before it is marked as sent.
				//  5. the sender goroutine then calls markSent and must not make the
				//     finished request live again.
				//
				// Tracking the request before Send keeps regionTracker and
				// request lifecycle visible in the same order and avoids leaving stale
				// requests behind.
				s.requestCache.markSent(regionReq)
				if err := doSend(createRegionRequest(s.upstream.clusterID, region)); err != nil {
					state.markStopped(err)
					return err
				}
				metrics.SubscriptionClientRegionRequestSendCounter.WithLabelValues(
					s.storeAddr,
					strconv.FormatUint(s.workerID, 10),
				).Inc()
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

func createRegionRequest(clusterID uint64, region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: clusterID, TicdcVersion: version.ReleaseSemver()},
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
