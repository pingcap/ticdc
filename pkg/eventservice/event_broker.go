// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

const (
	resolvedTsCacheSize = 512
	basicChannelSize    = 2048

	defaultMaxBatchSize            = 128
	defaultFlushResolvedTsInterval = 25 * time.Millisecond

	defaultReportDispatcherStatToStoreInterval = time.Second * 10

	maxReadyEventIntervalSeconds            = 10
	defaultSendResolvedTsInterval           = time.Second * 2
	defaultRefreshMinSentResolvedTsInterval = time.Second * 1
)

// eventBroker owns dispatcher runtime state but routes logic into three planes:
// 1. control plane: register/reset/remove/heartbeat/quota coordination
// 2. bootstrap plane: ready/handshake/open-stream convergence
// 3. data plane: scan/send/flow-control for ordered events
type eventBroker struct {
	tidbClusterID uint64
	incarnation   uint64

	eventStore  eventstore.EventStore
	schemaStore schemastore.SchemaStore
	mounter     event.Mounter
	msgSender   messaging.MessageSender
	pdClock     pdutil.Clock

	changefeedMap sync.Map
	dispatchers   sync.Map

	tableTriggerDispatchers sync.Map

	taskChan         []chan scanTask
	messageCh        []chan *wrapEvent
	redoMessageCh    []chan *wrapEvent
	cancel           context.CancelFunc
	g                *errgroup.Group
	metricsCollector *metricsCollector

	scanRateLimiter  *rate.Limiter
	scanLimitInBytes uint64
}

func newEventBroker(
	ctx context.Context,
	id uint64,
	incarnation uint64,
	eventStore eventstore.EventStore,
	schemaStore schemastore.SchemaStore,
	mc messaging.MessageSender,
	tz *time.Location,
	integrity *integrity.Config,
) *eventBroker {
	sendMessageWorkerCount := config.DefaultBasicEventHandlerConcurrency
	scanWorkerCount := config.DefaultBasicEventHandlerConcurrency * 4

	scanTaskQueueSize := config.GetGlobalServerConfig().Debug.EventService.ScanTaskQueueSize / scanWorkerCount
	sendMessageQueueSize := basicChannelSize * 4

	scanLimitInBytes := config.GetGlobalServerConfig().Debug.EventService.ScanLimitInBytes

	g, ctx := errgroup.WithContext(ctx)
	ctx, cancel := context.WithCancel(ctx)

	pdClock := appcontext.GetService[pdutil.Clock](appcontext.DefaultPDClock)
	c := &eventBroker{
		tidbClusterID:           id,
		incarnation:             incarnation,
		eventStore:              eventStore,
		pdClock:                 pdClock,
		mounter:                 event.NewMounter(tz, integrity),
		schemaStore:             schemaStore,
		changefeedMap:           sync.Map{},
		dispatchers:             sync.Map{},
		tableTriggerDispatchers: sync.Map{},
		msgSender:               mc,
		taskChan:                make([]chan scanTask, scanWorkerCount),
		messageCh:               make([]chan *wrapEvent, sendMessageWorkerCount),
		redoMessageCh:           make([]chan *wrapEvent, sendMessageWorkerCount),
		cancel:                  cancel,
		g:                       g,
		scanRateLimiter:         rate.NewLimiter(rate.Limit(scanLimitInBytes), scanLimitInBytes),
		scanLimitInBytes:        uint64(scanLimitInBytes),
	}

	c.metricsCollector = newMetricsCollector(c)

	for i := 0; i < sendMessageWorkerCount; i++ {
		c.messageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i, messaging.EventCollectorTopic)
		})
		c.redoMessageCh[i] = make(chan *wrapEvent, sendMessageQueueSize)
		g.Go(func() error {
			return c.runSendMessageWorker(ctx, i, messaging.RedoEventCollectorTopic)
		})
	}

	for i := 0; i < scanWorkerCount; i++ {
		taskChan := make(chan scanTask, scanTaskQueueSize)
		c.taskChan[i] = taskChan
		g.Go(func() error {
			return c.runScanWorker(ctx, taskChan)
		})
	}

	g.Go(func() error {
		return c.tickTableTriggerDispatchers(ctx)
	})

	g.Go(func() error {
		return c.logUninitializedDispatchers(ctx)
	})

	g.Go(func() error {
		return c.reportDispatcherStatToStore(ctx, defaultReportDispatcherStatToStoreInterval)
	})

	g.Go(func() error {
		return c.metricsCollector.Run(ctx)
	})

	g.Go(func() error {
		return c.refreshMinSentResolvedTs(ctx)
	})

	log.Info("new event broker created", zap.Uint64("id", id), zap.Uint64("scanLimitInBytes", c.scanLimitInBytes))
	return c
}

func (c *eventBroker) getMessageCh(workerIndex int, isRedo bool) chan *wrapEvent {
	if isRedo {
		return c.redoMessageCh[workerIndex]
	}
	return c.messageCh[workerIndex]
}

func (c *eventBroker) refreshMinSentResolvedTs(ctx context.Context) error {
	ticker := time.NewTicker(defaultRefreshMinSentResolvedTsInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.changefeedMap.Range(func(key, value interface{}) bool {
				status := value.(*changefeedStatus)
				status.refreshMinSentResolvedTs()
				return true
			})
		}
	}
}

func (c *eventBroker) logUninitializedDispatchers(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	isUninitialized := func(d *dispatcherStat) bool {
		return !d.isRemoved.Load() && d.seq.Load() == 0
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isUninitialized(dispatcher) {
					log.Info("dispatcher not reset",
						zap.Stringer("changefeedID", dispatcher.changefeedStat.changefeedID),
						zap.Any("dispatcherID", dispatcher.id))
				}
				return true
			})
			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isUninitialized(dispatcher) {
					log.Info("table trigger dispatcher not reset",
						zap.Stringer("changefeedID", dispatcher.changefeedStat.changefeedID),
						zap.Any("dispatcherID", dispatcher.id))
				}
				return true
			})
		}
	}
}

func (c *eventBroker) reportDispatcherStatToStore(ctx context.Context, tickInterval time.Duration) error {
	ticker := time.NewTicker(tickInterval)
	log.Info("update dispatcher send ts goroutine is started")
	isInactiveDispatcher := func(d *dispatcherStat) bool {
		return d.isHandshaked() && time.Since(time.Unix(d.lastReceivedHeartbeatTime.Load(), 0)) > heartbeatTimeout
	}
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
			inActiveDispatchers := make([]*dispatcherStat, 0)
			c.dispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				checkpointTs := dispatcher.checkpointTs.Load()
				if checkpointTs > 0 && checkpointTs < dispatcher.sentResolvedTs.Load() {
					c.eventStore.UpdateDispatcherCheckpointTs(dispatcher.id, checkpointTs)
				}
				if isInactiveDispatcher(dispatcher) {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			c.tableTriggerDispatchers.Range(func(key, value interface{}) bool {
				dispatcher := value.(*atomic.Pointer[dispatcherStat]).Load()
				if isInactiveDispatcher(dispatcher) {
					inActiveDispatchers = append(inActiveDispatchers, dispatcher)
				}
				return true
			})

			for _, d := range inActiveDispatchers {
				log.Warn("remove in-active dispatcher",
					zap.Stringer("changefeedID", d.changefeedStat.changefeedID),
					zap.Stringer("dispatcherID", d.id), zap.Time("lastReceivedHeartbeatTime", time.Unix(d.lastReceivedHeartbeatTime.Load(), 0)))
				c.removeDispatcher(d.info)
			}
		}
	}
}

func (c *eventBroker) close() {
	c.cancel()
	_ = c.g.Wait()
}

func (c *eventBroker) getDispatcher(id common.DispatcherID) *atomic.Pointer[dispatcherStat] {
	stat, ok := c.dispatchers.Load(id)
	if ok {
		return stat.(*atomic.Pointer[dispatcherStat])
	}
	stat, ok = c.tableTriggerDispatchers.Load(id)
	if ok {
		return stat.(*atomic.Pointer[dispatcherStat])
	}
	return nil
}
