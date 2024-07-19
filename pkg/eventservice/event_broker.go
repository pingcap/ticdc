package eventservice

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/flowbehappy/tigate/eventpb"
	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// eventBroker get event from the eventStore, and send the event to the dispatchers.
// Every TiDB cluster has a eventBroker.
// All span subscriptions and dispatchers of the TiDB cluster are managed by the eventBroker.
type eventBroker struct {
	ctx context.Context
	// tidbClusterID is the ID of the TiDB cluster this eventStore belongs to.
	tidbClusterID uint64
	// eventBroker get events from the eventStore.
	eventStore eventstore.EventStore
	// msgSender is used to send the events to the dispatchers.
	msgSender messaging.MessageSender

	// All the dispatchers that register to the eventBroker.
	dispatchers map[string]*dispatcherStat
	// changedCh is used to notify some span subscriptions have new events.
	changedCh chan *subscriptionChange
	// taskPool is used to store the scan tasks and merge the tasks of same dispatcher.
	// TODO: Make it support merge the tasks of the same table span, even if the tasks are from different dispatchers.
	taskPool *scanTaskPool

	// scanWorkerCount is the number of the scan workers to spawn.
	scanWorkerCount int

	// messageCh is used to receive message from the scanWorker,
	// and a goroutine is responsible for sending the message to the dispatchers.
	messageCh chan *messaging.TargetMessage
	// wg is used to spawn the goroutines.
	wg *sync.WaitGroup
	// cancel is used to cancel the goroutines spawned by the eventBroker.
	cancel context.CancelFunc
}

func newEventBroker(
	ctx context.Context,
	id uint64,
	eventStore eventstore.EventStore,
	mc messaging.MessageSender,
) *eventBroker {
	ctx, cancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	c := &eventBroker{
		ctx:             ctx,
		tidbClusterID:   id,
		eventStore:      eventStore,
		dispatchers:     make(map[string]*dispatcherStat),
		msgSender:       mc,
		changedCh:       make(chan *subscriptionChange, defaultChanelSize),
		taskPool:        newScanTaskPool(),
		scanWorkerCount: defaultWorkerCount,
		messageCh:       make(chan *messaging.TargetMessage, defaultChanelSize),
		cancel:          cancel,
		wg:              wg,
	}
	c.runGenerateScanTask()
	c.runScanWorker()
	c.runPushMessageWorker()
	return c
}

func (c *eventBroker) runGenerateScanTask() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case change := <-c.changedCh:
				dispatcher, ok := c.dispatchers[change.dispatcherInfo.GetID()]
				// The dispatcher may be deleted. In such case, we just the stale notification.
				if !ok {
					continue
				}
				startTs := dispatcher.watermark.Load()
				endTs := dispatcher.spanSubscription.watermark.Load()
				dataRange := common.NewDataRange(c.tidbClusterID, dispatcher.info.GetTableSpan(), startTs, endTs)
				task := &scanTask{
					dispatcherStat: dispatcher,
					dataRange:      dataRange,
					eventCount:     change.eventCount,
				}
				c.taskPool.pushTask(task)
			}
		}
	}()
}

func (c *eventBroker) runScanWorker() {
	for i := 0; i < c.scanWorkerCount; i++ {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for {
				select {
				case <-c.ctx.Done():
					return
				case task := <-c.taskPool.popTask():
					remoteID := messaging.ServerId(task.dispatcherStat.info.GetServerID())
					topic := task.dispatcherStat.info.GetTopic()
					dispatcherID := task.dispatcherStat.info.GetID()

					// The dispatcher has no new events. In such case, we don't need to scan the event store.
					// We just send the watermark to the dispatcher.
					if task.eventCount == 0 {
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
						task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
						continue
					}

					// scan the event store to get the events in the data range.
					events, err := c.eventStore.GetIterator(task.dataRange)
					if err != nil {
						log.Info("read events failed", zap.Error(err))
						// push the task back to the task pool.
						c.taskPool.pushTask(task)
						continue
					}

					// TODO: current we only pass a single task to the logService,
					// so that we only get a single event slice from the logService.
					event := events[0]
					// If the event is empty, it means no new events in the data range,
					// so we just send the watermark to the dispatcher.
					if len(event) == 0 {
						waterMarkMsg := &eventpb.EventFeed{
							ResolvedTs:   task.dataRange.EndTs,
							DispatcherId: dispatcherID,
						}
						// After all the events are sent, we send the watermark to the dispatcher.
						c.messageCh <- messaging.NewTargetMessage(remoteID, topic, waterMarkMsg)
						task.dispatcherStat.watermark.Store(task.dataRange.EndTs)
					}

					for _, e := range event {
						// Skip the events that have been sent to the dispatcher.
						if e.CommitTs <= task.dispatcherStat.watermark.Load() {
							continue
						}
						if e.IsDDLEvent() {
							msg := &eventpb.EventFeed{
								DispatcherId: dispatcherID,
							}
							// Send the event to the dispatcher.
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, msg)
						} else {
							msg := &eventpb.EventFeed{
								TxnEvents: []*eventpb.TxnEvent{
									{
										Events:   nil,
										StartTs:  e.StartTs,
										CommitTs: e.CommitTs,
									},
								},
								DispatcherId: dispatcherID,
							}
							// Send the event to the dispatcher.
							c.messageCh <- messaging.NewTargetMessage(remoteID, topic, msg)
						}
					}
				}
			}
		}()
	}
}

func (c *eventBroker) runPushMessageWorker() {
	c.wg.Add(1)
	// Use a single goroutine to send the messages in order.
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-c.ctx.Done():
				return
			case msg := <-c.messageCh:
				log.Info("send message", zap.Any("message", msg))
				c.msgSender.SendEvent(msg)
			}
		}
	}()
}

func (c *eventBroker) close() {
	c.cancel()
	c.wg.Wait()
}

// Store the progress of the dispatcher, and the incremental events stats.
// Those information will be used to decide when will the worker start to handle the push task of this dispatcher.
type dispatcherStat struct {
	info             DispatcherInfo
	spanSubscription *spanSubscription
	// The watermark of the events that have been sent to the dispatcher.
	watermark atomic.Uint64
	notify    chan *subscriptionChange
}

// onSubscriptionWatermark updates the watermark of the table span and send a notification to notify
// that this table span has new events.
func (a *dispatcherStat) onSubscriptionWatermark(watermark uint64) {
	if uint64(watermark) > a.spanSubscription.watermark.Load() {
		a.spanSubscription.watermark.Store(uint64(watermark))
	}
	sub := &subscriptionChange{
		dispatcherInfo: a.info,
		eventCount:     a.GetAndResetNewEventCount(),
	}
	select {
	case a.notify <- sub:
	default:
	}
}

// TODO: consider to use a better way to update the event count, may be we only need to
// know there are new events, and we don't need to know the exact number of the new events.
// So we can reduce the contention of the lock.
func (a *dispatcherStat) onNewEvent(raw *common.RawKVEntry) {
	a.spanSubscription.newEventCount.mu.Lock()
	defer a.spanSubscription.newEventCount.mu.Unlock()
	a.spanSubscription.newEventCount.v++
}

func (a *dispatcherStat) GetAndResetNewEventCount() uint64 {
	a.spanSubscription.newEventCount.mu.Lock()
	defer a.spanSubscription.newEventCount.mu.Unlock()
	v := a.spanSubscription.newEventCount.v
	a.spanSubscription.newEventCount.v = 0
	return v
}

// spanSubscription store the latest progress of the table span in the event store.
// And it also store the dispatchers that want to listen to the events of this table span.
type spanSubscription struct {
	span *common.TableSpan
	// The watermark of the events that have been stored in the event store.
	watermark atomic.Uint64
	// newEventCount is used to store the number of the new events that have been stored in the event store
	// since last scanTask is generated.
	newEventCount struct {
		mu sync.Mutex
		v  uint64
	}
}

type subscriptionChange struct {
	dispatcherInfo DispatcherInfo
	eventCount     uint64
}

type scanTask struct {
	dispatcherStat *dispatcherStat
	dataRange      *common.DataRange
	eventCount     uint64
}

type scanTaskPool struct {
	mu sync.Mutex
	// taskSet is used to merge the tasks with the same table span.
	taskSet map[string]*scanTask
	// fifoQueue is used to store the tasks that have new changes.
	fifoQueue chan *scanTask
}

func newScanTaskPool() *scanTaskPool {
	return &scanTaskPool{
		taskSet:   make(map[string]*scanTask),
		fifoQueue: make(chan *scanTask, defaultChanelSize),
	}
}

// addTask adds a task to the pool, and merge the task if the task is overlapped with the existing tasks.
func (p *scanTaskPool) pushTask(task *scanTask) {
	p.mu.Lock()
	defer p.mu.Unlock()
	spanTask := p.taskSet[task.dispatcherStat.info.GetID()]

	if spanTask == nil {
		spanTask = task
		p.taskSet[task.dispatcherStat.info.GetID()] = spanTask
	}

	// Merge the task into the existing task.
	mergedRange := task.dataRange.Merge(spanTask.dataRange)
	spanTask.dataRange = mergedRange
	spanTask.eventCount += task.eventCount
	// Update the existing task.
	select {
	case p.fifoQueue <- spanTask:
		p.taskSet[task.dispatcherStat.info.GetID()] = nil
	default:
	}
}

func (p *scanTaskPool) popTask() <-chan *scanTask {
	return p.fifoQueue
}
