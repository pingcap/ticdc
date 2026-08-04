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

var (
	metricsResolvedTsCount  = metrics.PullerEventCounter.WithLabelValues("resolved_ts")
	metricBatchResolvedSize = metrics.BatchResolvedEventSize.WithLabelValues("event-store")
)

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

func (q *controlQueue) drain() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for {
		if _, ok := q.queue.TryPop(); !ok {
			return
		}
	}
}

func (q *controlQueue) ready() <-chan struct{} {
	return q.queue.Ready()
}

// regionRequestWorker owns one TiKV event-feed stream and the requests sent
// through it, including reconnect cleanup and subscription deregistration.
type regionRequestWorker struct {
	workerID uint64

	upstream       *upstreamHandle
	eventSink      *regionEventSink
	failureHandler *regionFailureHandler
	storeAddr      string

	admission    *regionAdmissionController
	controlQueue *controlQueue
	tracker      *regionTracker
}

func newRegionRequestWorker(
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
	}
}

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
	}

	log.Info("region request worker going to create grpc stream",
		zap.Uint64("workerID", s.workerID),
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
	if err != nil {
		log.Warn("region request worker create grpc stream failed",
			zap.Uint64("workerID", s.workerID),
			zap.String("addr", s.storeAddr),
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
	metricBatchResolvedSize.Observe(float64(len(resolvedTsEvent.Regions)))
	// TODO: resolvedTsEvent.Ts be 0 is impossible, we need find the root cause.
	if resolvedTsEvent.Ts == 0 {
		log.Warn("region request worker receives a resolved ts event with zero value, ignore it",
			zap.Uint64("workerID", s.workerID),
			zap.Uint64("subscriptionID", resolvedTsEvent.RequestId),
			zap.Any("regionIDs", resolvedTsEvent.Regions))
		return
	}

	const resolvedTsStateBatchSize = 1024
	// Avoid allocating a huge states slice when resolvedTsEvent.Regions is large.
	// Push resolved-ts events in batches to reduce peak memory usage and improve GC behavior.
	capHint := min(len(resolvedTsEvent.Regions), resolvedTsStateBatchSize)
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

func (s *regionRequestWorker) processRegionSendTask(
	ctx context.Context,
	conn *ConnAndClient,
	firstReq *regionReq,
) error {
	regionReq := firstReq
	for {
		// Send the current region request before handling anything newly queued.
		if regionReq != nil {
			if err := s.sendRegionRequest(conn, regionReq); err != nil {
				return err
			}
		}
		// Flush pending deregisters before admitting the next region request.
		// Admission may still contain stale tasks from a stopped subscription, but
		// sendRegionRequest re-checks subscription liveness before tracker.Add/Send,
		// so those tasks are dropped locally instead of recreating remote registrations.
		for {
			req, ok := s.controlQueue.tryPop()
			if !ok {
				break
			}
			if err := s.sendDeregisterRequest(conn, req); err != nil {
				return err
			}
		}
		// Block for the next request, but wake early when deregisters arrive.
		// regionReq above is already consumed and will be replaced by the next pop.
		var err error
		regionReq, err = s.admission.pop(ctx, s.controlQueue.ready())
		if err != nil {
			return err
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
		ScanPriority: normalizeScanPriority(region.scanPriority),
	}
}
