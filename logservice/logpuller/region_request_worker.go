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

var metricBatchResolvedSize = metrics.BatchResolvedEventSize.WithLabelValues("event-store")

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
	for {
		// Do not connect an idle worker to an unavailable store indefinitely.
		firstReq, err := s.waitForRegionRequest(ctx)
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
			firstReq.abort()
			return ctx.Err()
		}

		// Stop sent requests first so their states release the admission leases.
		// firstReq still owns its lease only if the stream failed before Send.
		s.failStreamRegions(regionErr)
		if firstReq.abort() {
			s.failureHandler.Report(newRegionErrorInfo(firstReq.regionInfo, regionErr))
		}
		s.failPendingRegions(regionErr)

		if err := util.Hang(ctx, storeReconnectBackoff); err != nil {
			return err
		}
	}
}

// failStreamRegions transfers every request sent by a failed stream to the
// recovery pipeline.
func (s *regionRequestWorker) failStreamRegions(err error) {
	for _, states := range s.tracker.Drain() {
		for _, state := range states {
			s.notifyRegionError(state, err)
		}
	}
	// The failed stream no longer owns remote registrations.
	s.controlQueue.drain()
}

// failPendingRegions transfers requests owned by this worker but not yet sent
// to the recovery pipeline, so they can be resolved and routed again.
func (s *regionRequestWorker) failPendingRegions(err error) {
	for _, task := range s.admission.drain() {
		s.failureHandler.Report(newRegionErrorInfo(task.regionInfo, err))
	}
}

func (s *regionRequestWorker) notifyRegionError(state *regionFeedState, err error) {
	state.markStopped(err)
	s.eventSink.Push(
		SubscriptionID(state.requestID),
		regionEvent{states: []*regionFeedState{state}},
	)
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
				continue
			case *cdcpb.Event_Error:
				log.Debug("region request worker receives a region error",
					zap.Uint64("workerID", s.workerID),
					zap.Uint64("subscriptionID", uint64(subscriptionID)),
					zap.Uint64("regionID", event.RegionId),
					zap.Any("error", eventData.Error))
				s.notifyRegionError(state, &eventError{err: eventData.Error})
				continue
			case *cdcpb.Event_ResolvedTs:
				regionEvent.resolvedTs = eventData.ResolvedTs
			case *cdcpb.Event_LongTxn_:
				continue
			default:
				log.Panic("unknown event type", zap.Any("event", event))
			}
			s.eventSink.Push(subscriptionID, regionEvent)
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
	metricBatchResolvedSize.Observe(float64(len(resolvedTsEvent.Regions)))
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
		s.notifyRegionError(state, &requestCancelledErr{})
	}
	return nil
}

func (s *regionRequestWorker) drainControlQueue(conn *ConnAndClient) error {
	for {
		req, ok := s.controlQueue.tryPop()
		if !ok {
			return nil
		}
		if err := s.sendDeregisterRequest(conn, req); err != nil {
			return err
		}
	}
}

func (s *regionRequestWorker) sendRegionRequest(conn *ConnAndClient, req *regionReq) error {
	if !req.isActive() {
		return &storeStreamErr{}
	}
	region := req.regionInfo
	subID := region.subscribedSpan.subID
	log.Debug("region request worker gets a singleRegionInfo",
		zap.Uint64("workerID", s.workerID),
		zap.Uint64("subscriptionID", uint64(subID)),
		zap.Uint64("regionID", region.verID.GetID()),
		zap.String("addr", s.storeAddr),
		zap.Bool("bdrMode", region.filterLoop))

	if region.subscribedSpan.stopped.Load() {
		req.abort()
		s.failureHandler.Report(newRegionErrorInfo(region, &storeStreamErr{}))
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
	if err := s.sendChangeDataRequest(conn, s.createRegionRequest(region)); err != nil {
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
		if regionReq != nil {
			if err := s.sendRegionRequest(conn, regionReq); err != nil {
				return err
			}
			regionReq = nil
			continue
		}

		if err := s.drainControlQueue(conn); err != nil {
			return err
		}
		var err error
		regionReq, err = s.admission.pop(ctx, s.controlQueue.ready())
		if err != nil {
			return err
		}
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
