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

	client *subscriptionClient

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
	}
	worker.requestedRegions.subscriptions = make(map[SubscriptionID]regionFeedStates)

	waitForPreFetching := func() error {
		if worker.preFetchForConnecting != nil {
			log.Panic("preFetchForConnecting should be nil",
				zap.Uint64("workerID", worker.workerID),
				zap.String("addr", store.storeAddr))
		}
		for {
			region, err := worker.requestCache.pop(ctx)
			if err != nil {
				return err
			}
			if !region.regionInfo.isStopped() {
				worker.preFetchForConnecting = new(regionInfo)
				*worker.preFetchForConnecting = region.regionInfo
				return nil
			} else {
				continue
			}
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
					regionErr = &sendRequestToStoreErr{}
				}
			} else {
				if canceled := worker.run(ctx, credential); canceled {
					return nil
				}
				regionErr = &sendRequestToStoreErr{}
			}
			for subID, m := range worker.clearRegionStates() {
				for _, state := range m {
					state.markStopped(regionErr)
					regionEvent := regionEvent{
						state:  state,
						worker: worker,
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
			if StatusIsEOF(grpcstatus.Convert(err)) {
				return nil
			}
			return errors.Trace(err)
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
				state:  state,
				worker: s,
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
					zap.Bool("stateIsNil", state == nil),
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
			s.client.pushRegionEventToDS(SubscriptionID(event.RequestId), regionEvent)
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
	for _, regionID := range resolvedTsEvent.Regions {
		if state := s.getRegionState(subscriptionID, regionID); state != nil {
			s.client.pushRegionEventToDS(SubscriptionID(resolvedTsEvent.RequestId), regionEvent{
				state:      state,
				worker:     s,
				resolvedTs: resolvedTsEvent.Ts,
			})
		} else {
			log.Warn("region request worker receives a resolved ts event for an untracked region",
				zap.Uint64("workerID", s.workerID),
				zap.Uint64("subscriptionID", uint64(subscriptionID)),
				zap.Uint64("regionID", regionID),
				zap.Uint64("resolvedTs", resolvedTsEvent.Ts))
		}
	}
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
			return errors.Trace(err)
		}
		// TODO: add a metric?
		return nil
	}

	fetchMoreReq := func() (regionReq, error) {
		for {
			// Try to get from cache
			if req, err := s.requestCache.pop(ctx); err != nil {
				return regionReq{}, err
			} else {
				return req, nil
			}
		}
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
			if err := doSend(req); err != nil {
				return err
			}
			for _, state := range s.takeRegionStates(subID) {
				state.markStopped(&requestCancelledErr{})
				regionEvent := regionEvent{
					state:  state,
					worker: s,
				}
				s.client.pushRegionEventToDS(subID, regionEvent)
			}

		} else if region.subscribedSpan.stopped.Load() {
			// It can be skipped directly because there must be no pending states from
			// the stopped subscribedTable, or the special singleRegionInfo for stopping
			// the table will be handled later.
			s.client.onRegionFail(newRegionErrorInfo(region, &sendRequestToStoreErr{}))
		} else {
			state := newRegionFeedState(region, uint64(subID), s)
			state.start()
			s.addRegionState(subID, region.verID.GetID(), state)
			if err := doSend(s.createRegionRequest(region)); err != nil {
				return err
			}
			s.requestCache.markSent(regionReq)
		}
		regionReq, err = fetchMoreReq()
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
	}

	// Clear all regions from cache
	cacheRegions := s.requestCache.clear()
	regions = append(regions, cacheRegions...)
	return regions
}
