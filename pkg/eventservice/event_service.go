package eventservice

import (
	"context"

	"github.com/flowbehappy/tigate/logservice/eventstore"
	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/flowbehappy/tigate/pkg/messaging"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"go.uber.org/zap"
)

const (
	defaultChanelSize  = 2048
	defaultWorkerCount = 32
)

// EventService accepts the requests of pulling events.
// The EventService is a singleton in the system.
type EventService interface {
	Run() error
	Close()
}

type logpuller interface {
	// SubscribeTableSpan subscribes the table span, and returns the latest progress of the table span.
	// afterUpdate is called when the watermark of the table span is updated.
	SubscribeTableSpan(span *common.TableSpan, startTs uint64, onSpanUpdate func(watermark uint64)) (uint64, error)
	// Read return the event of the data range.
	Read(dataRange ...*common.DataRange) ([][]*common.TxnEvent, error)
}

type DispatcherInfo interface {
	// GetID returns the ID of the dispatcher.
	GetID() string
	// GetClusterID returns the ID of the TiDB cluster the acceptor wants to accept events from.
	GetClusterID() uint64

	GetTopic() common.TopicType
	GetServerID() string
	GetTableSpan() *common.TableSpan
	GetStartTs() uint64
	IsRegister() bool
}

type eventService struct {
	ctx        context.Context
	mc         messaging.MessageCenter
	eventStore eventstore.EventStore
	brokers    map[uint64]*eventBroker

	// TODO: use a better way to cache the acceptorInfos
	acceptorInfoCh chan DispatcherInfo
}

func NewEventService(ctx context.Context, mc messaging.MessageCenter, eventStore eventstore.EventStore) EventService {
	es := &eventService{
		mc:             mc,
		eventStore:     eventStore,
		ctx:            ctx,
		brokers:        make(map[uint64]*eventBroker),
		acceptorInfoCh: make(chan DispatcherInfo, defaultChanelSize*16),
	}
	es.mc.RegisterHandler(messaging.EventServiceTopic, es.handleMessage)
	return es
}

func (s *eventService) Run() error {
	log.Info("start event service")
	for {
		select {
		case <-s.ctx.Done():
			log.Info("event service exited")
			return nil
		case info := <-s.acceptorInfoCh:
			if info.IsRegister() {
				s.registerDispatcher(info)
			} else {
				s.deregisterDispatcher(info.GetClusterID(), info.GetID())
			}
		}
	}
}

func (s *eventService) Close() {
	log.Info("event service is closing")
	for _, c := range s.brokers {
		c.close()
	}
	log.Info("event service is closed")
}

func (s *eventService) handleMessage(msg *messaging.TargetMessage) error {
	acceptorInfo := msgToAcceptorInfo(msg)
	s.acceptorInfoCh <- acceptorInfo
	return nil
}

func (s *eventService) registerDispatcher(acceptor DispatcherInfo) {
	clusterID := acceptor.GetClusterID()
	startTs := acceptor.GetStartTs()
	span := acceptor.GetTableSpan()

	c, ok := s.brokers[clusterID]
	if !ok {
		c = newEventBroker(s.ctx, clusterID, s.eventStore, s.mc)
		s.brokers[clusterID] = c
	}

	subscription := &spanSubscription{
		span: span,
	}
	subscription.watermark.Store(uint64(startTs))
	// add the acceptor to the cluster.
	ac := &dispatcherStat{
		info:             acceptor,
		spanSubscription: subscription,
		notify:           c.changedCh,
	}
	ac.watermark.Store(uint64(startTs))

	c.dispatchers[acceptor.GetID()] = ac

	// c.logpuller.SubscribeTableSpan(span, startTs, stat.UpdateWatermark)
	tbspan := tablepb.Span{
		TableID:  tablepb.TableID(span.TableID),
		StartKey: span.StartKey,
		EndKey:   span.EndKey,
	}
	id := uuid.MustParse(acceptor.GetID())
	c.eventStore.RegisterDispatcher(
		common.DispatcherID(id),
		tbspan,
		common.Ts(acceptor.GetStartTs()),
		ac.onNewEvent,
		ac.onSubscriptionWatermark,
	)
	log.Info("register acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", acceptor.GetID()))
}

func (s *eventService) deregisterDispatcher(clusterID uint64, accepterID string) {
	c, ok := s.brokers[clusterID]
	if !ok {
		return
	}
	_, ok = c.dispatchers[accepterID]
	if !ok {
		return
	}
	//TODO: release the resources of the acceptor.
	delete(c.dispatchers, accepterID)
	log.Info("deregister acceptor", zap.Uint64("clusterID", clusterID), zap.String("acceptorID", accepterID))
}

// TODO: implement the following functions
func msgToAcceptorInfo(msg *messaging.TargetMessage) DispatcherInfo {
	return msg.Message.(messaging.RegisterDispatcherRequest)
}
