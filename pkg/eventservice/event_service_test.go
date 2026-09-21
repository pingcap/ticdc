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
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/logservicepb"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/liveness"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func startEventService(
	ctx context.Context, t *testing.T,
	mc messaging.MessageCenter, mockStore eventstore.EventStore,
) *eventService {
	mockSchemaStore := NewMockSchemaStore()
	mockPDClock := pdutil.NewClock4Test()
	appcontext.SetService(appcontext.DefaultPDClock, mockPDClock)
	appcontext.SetService(appcontext.MessageCenter, mc)
	appcontext.SetService(appcontext.EventStore, mockStore)
	appcontext.SetService(appcontext.SchemaStore, mockSchemaStore)
	es := New(mockStore, mockSchemaStore, nil)
	esImpl := es.(*eventService)
	go func() {
		err := esImpl.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()
	return esImpl
}

func TestNewEventServiceRemovesOrphanedLargeTxnSpillFiles(t *testing.T) {
	original := config.GetGlobalServerConfig().Clone()
	cfg := original.Clone()
	cfg.DataDir = t.TempDir()
	config.StoreGlobalServerConfig(cfg)
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(original)
	})

	spillDir := getLargeTxnInsertSpillDir()
	require.NoError(t, os.MkdirAll(spillDir, 0o700))
	orphanPath := filepath.Join(spillDir, "eventservice-large-txn-insert-orphan.spill")
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o600))

	mc := messaging.NewMockMessageCenter()
	appcontext.SetService(appcontext.MessageCenter, mc)
	_ = New(newMockEventStore(100), NewMockSchemaStore(), nil)

	require.NoFileExists(t, orphanPath)
}

func TestEventServiceBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info("start event service basic test")

	mockStore := newMockEventStore(100)
	_ = mockStore.Run(ctx)

	mc := messaging.NewMockMessageCenter()
	esImpl := startEventService(ctx, t, mc, mockStore)
	_ = esImpl.Close(ctx)

	dispatcherInfo := newMockDispatcherInfo(t, 200, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	// register acceptor
	esImpl.registerDispatcher(ctx, dispatcherInfo)
	require.Equal(t, 1, len(esImpl.brokers))

	broker := esImpl.brokers[dispatcherInfo.GetClusterID()]
	require.NotNil(t, broker)

	controlM := commonEvent.NewCongestionControl()
	controlM.AddAvailableMemory(dispatcherInfo.GetChangefeedID().Id, broker.scanLimitInBytes+1024*1024)
	broker.handleCongestionControl(node.ID(dispatcherInfo.serverID), controlM)

	// add events to eventStore`
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper,
		`create table test.t(id int primary key, c char(50))`,
		[]string{
			`insert into test.t(id,c) values (0, "c0")`,
			`insert into test.t(id,c) values (1, "c1")`,
			`insert into test.t(id,c) values (2, "c2")`,
		}...)
	require.NotNil(t, kvEvents)

	esImpl.schemaStore.(*mockSchemaStore).AppendDDLEvent(dispatcherInfo.span.TableID, ddlEvent)

	resolvedTs := kvEvents[0].CRTs + 1
	_ = mockStore.AppendEvents(dispatcherInfo.id, resolvedTs, kvEvents[0])
	// receive events from msg center
	var (
		msgCnt   int
		dmlCount int
	)
	msgCh := mc.GetMessageChannel()
	for {
		msg := <-msgCh
		log.Info("receive message", zap.Any("message", msg))
		for _, m := range msg.Message {
			msgCnt++
			switch e := m.(type) {
			case *commonEvent.ReadyEvent:
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, dispatcherInfo.id, e.DispatcherID)
				require.Equal(t, uint64(0), e.GetSeq())
				log.Info("receive ready event", zap.Any("event", e))

				// 1. When a Dispatcher is register, it will send a ReadyEvent to the eventCollector.
				// 2. The eventCollector will send a reset request to the eventService.
				// 3. We are here to simulate the reset request.
				dispatcherInfo.epoch = 1
				esImpl.resetDispatcher(dispatcherInfo)
				_ = mockStore.AppendEvents(dispatcherInfo.id, resolvedTs+1)
			case *commonEvent.HandshakeEvent:
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, dispatcherInfo.id, e.DispatcherID)
				require.Equal(t, dispatcherInfo.startTs, e.GetStartTs())
				require.Equal(t, uint64(1), e.Seq)
				log.Info("receive handshake event", zap.Any("event", e))

				_ = mockStore.AppendEvents(dispatcherInfo.id, kvEvents[1].CRTs+1, kvEvents[1])
				_ = mockStore.AppendEvents(dispatcherInfo.id, kvEvents[2].CRTs+1, kvEvents[2])
			case *commonEvent.BatchDMLEvent:
				require.Equal(t, "event-collector", msg.Topic)
				// first dml has one event
				if dmlCount == 0 {
					require.Equal(t, int32(1), e.Len())
				}
				dmlCount += len(e.DMLEvents)
				require.Equal(t, kvEvents[dmlCount-1].CRTs, e.GetCommitTs())
				require.Equal(t, uint64(dmlCount+2), e.GetSeq())
			case *commonEvent.DDLEvent:
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, ddlEvent.FinishedTs, e.FinishedTs)
				require.Equal(t, uint64(2), e.Seq)
			case *commonEvent.BatchResolvedEvent:
				log.Info("receive watermark", zap.Uint64("ts", e.Events[0].ResolvedTs))
			}
		}
		if msgCnt >= 5 {
			break
		}
	}
}

func TestEventServiceDispatcherCount(t *testing.T) {
	ctx := t.Context()
	appcontext.SetService(appcontext.DefaultPDClock, pdutil.NewClock4Test())
	es := &eventService{
		mc:          messaging.NewMockMessageCenter(),
		eventStore:  newMockEventStore(100),
		schemaStore: NewMockSchemaStore(),
		brokers:     make(map[uint64]*eventBroker),
		tz:          time.UTC,
	}
	defer es.Close(ctx)
	require.Zero(t, es.GetDispatcherCount())

	// Log coordinator reports may read the count while registration creates another broker.
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
				es.GetDispatcherCount()
			}
		}
	})
	defer func() { close(done); wg.Wait() }()

	ordinary := newMockDispatcherInfoForTest(t)
	es.registerDispatcher(ctx, ordinary)
	require.Equal(t, 1, es.GetDispatcherCount())
	// Repeated registration reuses the existing entry and its resources.
	es.registerDispatcher(ctx, ordinary)
	require.Equal(t, 1, es.GetDispatcherCount())

	ddl := newMockDispatcherInfoForTest(t)
	ddl.clusterID = ordinary.clusterID + 1
	ddl.span = common.KeyspaceDDLSpan(0)
	es.registerDispatcher(ctx, ddl)
	require.Equal(t, 2, es.GetDispatcherCount())
	es.registerDispatcher(ctx, ddl)
	require.Equal(t, 2, es.GetDispatcherCount())

	es.deregisterDispatcher(ordinary)
	require.Equal(t, 1, es.GetDispatcherCount())
	es.deregisterDispatcher(ordinary)
	require.Equal(t, 1, es.GetDispatcherCount())
	es.deregisterDispatcher(ddl)
	require.Zero(t, es.GetDispatcherCount())
}

func TestStopAcceptingRegistrations(t *testing.T) {
	broker, store, schema, _ := newEventBrokerForTest()
	broker.close()
	mc := messaging.NewMockMessageCenter()
	var nodeLiveness liveness.Liveness
	service := &eventService{mc: mc, brokers: map[uint64]*eventBroker{broker.tidbClusterID: broker}, nodeLiveness: &nodeLiveness}
	info := newMockDispatcherInfoForTest(t)

	// An admitted registration can block in schema initialization without
	// delaying the log coordinator report or allowing it to report zero.
	started := make(chan struct{})
	resume := make(chan struct{})
	registered := make(chan struct{})
	release := sync.OnceFunc(func() { close(resume) })
	schema.registerTableHook = func() { close(started); <-resume }
	var wg sync.WaitGroup
	t.Cleanup(func() { release(); wg.Wait() })
	wg.Go(func() {
		service.registerDispatcher(t.Context(), info)
		close(registered)
	})
	<-started
	require.True(t, nodeLiveness.Store(liveness.CaptureDraining))
	require.True(t, nodeLiveness.Store(liveness.CaptureStopping))
	broadcast := messaging.NewSingleTargetMessage("capture", messaging.EventServiceTopic, &common.LogCoordinatorBroadcastRequest{})
	broadcast.From = "log-coordinator"
	require.NoError(t, service.handleMessage(t.Context(), broadcast))
	message := <-mc.GetMessageChannel()
	require.Equal(t, broadcast.From, message.To)
	require.Equal(t, messaging.LogCoordinatorTopic, message.Topic)
	report := message.Message[0].(*logservicepb.EventBrokerDispatcherCount)
	require.True(t, report.RegistrationsStopped)
	require.Equal(t, uint32(1), report.DispatcherCount)
	// Completing an admitted registration must not wait for a report reader
	// holding the broker map lock.
	service.brokersMu.RLock()
	release()
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		service.brokersMu.RUnlock()
		t.Fatal("registration completion blocked on the broker map lock")
	}
	service.brokersMu.RUnlock()
	wg.Wait()
	service.deregisterDispatcher(info)
	// Only the next direct report can authorize completion.
	require.NoError(t, service.handleMessage(t.Context(), broadcast))
	report = (<-mc.GetMessageChannel()).Message[0].(*logservicepb.EventBrokerDispatcherCount)
	require.True(t, report.RegistrationsStopped)
	require.Zero(t, report.DispatcherCount)

	for _, tc := range []struct {
		name      string
		onlyReuse bool
		mode      int64
		clusterID uint64
		topic     string
	}{
		{name: "local", clusterID: info.clusterID},
		{name: "remote", onlyReuse: true, clusterID: info.clusterID, topic: messaging.EventCollectorTopic},
		{name: "redo remote new cluster", onlyReuse: true, mode: common.RedoMode, clusterID: info.clusterID + 1, topic: messaging.RedoEventCollectorTopic},
	} {
		t.Run(tc.name, func(t *testing.T) {
			late := newMockDispatcherInfoForTest(t)
			late.onlyReuse, late.mode, late.clusterID = tc.onlyReuse, tc.mode, tc.clusterID
			service.registerDispatcher(t.Context(), late)
			require.Zero(t, service.GetDispatcherCount())
			require.Len(t, service.brokers, 1)
			_, active := store.dispatcherMap.Load(late.id)
			require.False(t, active)
			if tc.onlyReuse {
				response := <-mc.GetMessageChannel()
				require.Equal(t, tc.topic, response.Topic)
				require.Equal(t, late.GetID(), response.Message[0].(*commonEvent.NotReusableEvent).GetDispatcherID())
			} else {
				require.Empty(t, mc.GetMessageChannel())
			}
		})
	}
}

func TestEventServiceReportsWithoutBrokers(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	var nodeLiveness liveness.Liveness
	service := &eventService{mc: mc, nodeLiveness: &nodeLiveness}
	service.reportDispatcherCount("log-coordinator")
	report := (<-mc.GetMessageChannel()).Message[0].(*logservicepb.EventBrokerDispatcherCount)
	require.Zero(t, report.DispatcherCount)
	require.False(t, report.RegistrationsStopped)

	require.True(t, nodeLiveness.Store(liveness.CaptureDraining))
	require.True(t, nodeLiveness.Store(liveness.CaptureStopping))
	// Admission closes on registration too, without waiting for a broadcast.
	info := newMockDispatcherInfoForTest(t)
	info.onlyReuse = true
	service.registerDispatcher(t.Context(), info)
	require.Equal(t, messaging.TypeNotReusableEvent, (<-mc.GetMessageChannel()).Type)
	service.reportDispatcherCount("new-log-coordinator")
	message := <-mc.GetMessageChannel()
	require.Equal(t, node.ID("new-log-coordinator"), message.To)
	report = message.Message[0].(*logservicepb.EventBrokerDispatcherCount)
	require.Zero(t, report.DispatcherCount)
	require.True(t, report.RegistrationsStopped)
	require.Empty(t, service.brokers)
}

func TestPendingHeartbeatWithoutBroker(t *testing.T) {
	mc := messaging.NewMockMessageCenter()
	service := &eventService{mc: mc}
	id := common.NewDispatcherID()
	heartbeat := commonEvent.NewDispatcherHeartbeat()
	heartbeat.ClusterID = 42
	heartbeat.AddDispatcherProgress(id, 0, 0)
	service.StopAcceptingRegistrations()
	service.handleDispatcherHeartbeat(&DispatcherHeartBeatWithServerID{serverID: "consumer", heartbeat: heartbeat})
	response := (<-mc.GetMessageChannel()).Message[0].(*commonEvent.DispatcherHeartbeatResponse)
	require.Equal(t, uint64(42), response.ClusterID)
	require.Equal(t, []commonEvent.DispatcherState{commonEvent.NewDispatcherState(id, commonEvent.DSStateRemoved)}, response.DispatcherStates)
}

func TestPendingHeartbeatBeforeRegistration(t *testing.T) {
	for _, handshaked := range []bool{false, true} {
		t.Run(fmt.Sprintf("handshaked=%t", handshaked), func(t *testing.T) {
			broker, store, schema, responses := newEventBrokerForTest()
			// Drive both queues explicitly to reproduce the reordering without
			// depending on the service loop's select or background scans.
			broker.close()
			service := &eventService{
				brokers:             map[uint64]*eventBroker{broker.tidbClusterID: broker},
				dispatcherInfoChan:  make(chan DispatcherInfo, 1),
				dispatcherHeartbeat: make(chan *DispatcherHeartBeatWithServerID, 1),
			}
			var storeRegistrations, schemaReferences int
			store.registerDispatcherHook = func() bool { storeRegistrations++; return true }
			schema.registerTableHook = func() { schemaReferences++ }
			schema.unregisterTableHook = func() { schemaReferences-- }

			info := newMockDispatcherInfoForTest(t)
			heartbeat := commonEvent.NewDispatcherHeartbeat()
			heartbeat.ClusterID = info.clusterID
			heartbeat.AddDispatcherProgress(info.id, 0, 0)
			service.dispatcherInfoChan <- info
			service.dispatcherHeartbeat <- &DispatcherHeartBeatWithServerID{serverID: info.serverID, heartbeat: heartbeat}
			service.handleDispatcherHeartbeat(<-service.dispatcherHeartbeat)
			service.registerDispatcher(t.Context(), <-service.dispatcherInfoChan)

			if handshaked {
				reset := *info
				reset.epoch = 1
				reset.actionType = eventpb.ActionType_ACTION_TYPE_RESET
				require.NoError(t, broker.resetDispatcher(&reset))
				broker.getDispatcher(info.id).Load().setHandshaked()
			}
			original := broker.getDispatcher(info.id).Load()

			// Deliver the stale Removed response after registration succeeds.
			// The collector responds by retrying the same REGISTER.
			response := (<-responses).Message[0].(*commonEvent.DispatcherHeartbeatResponse)
			require.Equal(t, []commonEvent.DispatcherState{commonEvent.NewDispatcherState(info.id, commonEvent.DSStateRemoved)}, response.DispatcherStates)
			service.dispatcherInfoChan <- info
			service.registerDispatcher(t.Context(), <-service.dispatcherInfoChan)

			require.Equal(t, 1, storeRegistrations)
			require.Equal(t, 1, schemaReferences)
			require.Equal(t, 1, service.GetDispatcherCount())
			require.Same(t, original, broker.getDispatcher(info.id).Load())
			require.Equal(t, handshaked, original.isHandshaked())
			// A retry must receive Ready even if the initial registration has
			// already handshaked, so the collector can complete its retry.
			readyCh := broker.getMessageCh(original.messageWorkerIndex, common.IsRedoMode(info.mode))
			require.Len(t, readyCh, 1)
			ready := <-readyCh
			require.Equal(t, commonEvent.TypeReadyEvent, ready.msgType)
			require.Equal(t, node.ID(info.serverID), ready.serverID)
			ready.reset()

			service.deregisterDispatcher(info)
			require.Zero(t, service.GetDispatcherCount())
			require.Zero(t, schemaReferences)
			require.Equal(t, uint64(storeRegistrations), store.unregisterCount.Load())
			_, registered := store.dispatcherMap.Load(info.id)
			require.False(t, registered)
			_, subscribed := store.spansMap.Load(info.span)
			require.False(t, subscribed)
		})
	}
}

func TestHandleMessageIgnoresInvalidSingleMessagePayloads(t *testing.T) {
	es := &eventService{}

	require.NotPanics(t, func() {
		err := es.handleMessage(context.Background(), &messaging.TargetMessage{
			Type:    messaging.TypeDispatcherHeartbeat,
			Message: nil,
		})
		require.NoError(t, err)
	})

	require.NotPanics(t, func() {
		err := es.handleMessage(context.Background(), &messaging.TargetMessage{
			Type:    messaging.TypeCongestionControl,
			Message: nil,
		})
		require.NoError(t, err)
	})
}

var _ eventstore.EventStore = &mockEventStore{}

// mockEventStore is a mock implementation of the EventStore interface
type mockEventStore struct {
	resolvedTsUpdateInterval time.Duration
	dispatcherMap            sync.Map // key is common.DispatcherID, value is span
	spansMap                 sync.Map // key is *heartbeatpb.TableSpan
	unregisterCount          atomic.Uint64
	registerDispatcherHook   func() bool
}

func newMockEventStore(resolvedTsUpdateInterval int) *mockEventStore {
	return &mockEventStore{
		resolvedTsUpdateInterval: time.Millisecond * time.Duration(resolvedTsUpdateInterval),
		spansMap:                 sync.Map{},
	}
}

// AppendEvents appends events to the event store for a specific dispatcher.
// It will update the span stats and the span stats will notify the resolved ts notifier.
func (m *mockEventStore) AppendEvents(dispatcherID common.DispatcherID, resolvedTs uint64, events ...*common.RawKVEntry) error {
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if !ok {
		return fmt.Errorf("dispatcher not found: %v", dispatcherID)
	}
	spanStats, ok := m.spansMap.Load(span)
	if !ok {
		return fmt.Errorf("span not found: %v", span)
	}
	log.Info("append events", zap.Any("dispatcherID", dispatcherID), zap.Any("resolvedTs", resolvedTs), zap.Int("eventsNum", len(events)))
	spanStats.(*mockSpanStats).update(resolvedTs, events...)
	return nil
}

// Fake implementation for test
func (m *mockEventStore) GetDispatcherDMLEventState(dispatcherID common.DispatcherID) (
	bool,
	eventstore.DMLEventState,
) {
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if !ok {
		return false, eventstore.DMLEventState{
			MaxEventCommitTs: 0,
		}
	}
	spanStats, ok := m.spansMap.Load(span)
	if !ok {
		return false, eventstore.DMLEventState{
			MaxEventCommitTs: 0,
		}
	}
	return true, eventstore.DMLEventState{
		MaxEventCommitTs: spanStats.(*mockSpanStats).latestCommitTs(),
	}
}

func (m *mockEventStore) Name() string {
	return "mockEventStore"
}

func (m *mockEventStore) Run(ctx context.Context) error {
	// Loop all spans and notify the watermarkNotifier.
	ticker := time.NewTicker(time.Millisecond * 10)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.spansMap.Range(func(key, value any) bool {
					spanStats := value.(*mockSpanStats)
					spanStats.resolvedTsNotifier(spanStats.getResolvedTs(), spanStats.latestCommitTs())
					return true
				})
			}
		}
	}()
	return nil
}

func (m *mockEventStore) Close(ctx context.Context) error {
	return nil
}

func (m *mockEventStore) UpdateDispatcherCheckpointTs(dispatcherID common.DispatcherID, gcTS uint64) {
}

func (m *mockEventStore) UnregisterDispatcher(changefeedID common.ChangeFeedID, dispatcherID common.DispatcherID) {
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if ok {
		m.spansMap.Delete(span)
		m.dispatcherMap.Delete(dispatcherID)
	}
	m.unregisterCount.Add(1)
}

func (m *mockEventStore) GetIterator(
	dispatcherID common.DispatcherID, request eventstore.ScanRequest,
) (eventstore.EventIterator, error) {
	dataRange := request.Range
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if !ok {
		log.Panic("dispatcher not found", zap.Stringer("dispatcherID", dispatcherID))
	}

	v, ok := m.spansMap.Load(span)
	if !ok {
		log.Panic("span not found", zap.Any("span", span), zap.Stringer("dispatcherID", dispatcherID))
	}

	spanStats := v.(*mockSpanStats)
	events := spanStats.getAllEvents()

	entries := make([]*common.RawKVEntry, 0)
	positions := make([]eventstore.ScanPosition, 0)
	rowLevelStart := decodeMockScanPosition(request.Cursor.Position)
	for i, e := range events {
		if rowLevelStart >= 0 && i <= rowLevelStart {
			continue
		}
		if len(request.Cursor.Position) != 0 {
			if e.CRTs >= dataRange.CommitTsStart && e.CRTs <= dataRange.CommitTsEnd {
				entries = append(entries, e)
				positions = append(positions, encodeMockScanPosition(i))
			}
			continue
		}
		if request.Cursor.TxnStartTs != 0 {
			if e.CRTs == dataRange.CommitTsStart && e.StartTs <= request.Cursor.TxnStartTs {
				continue
			}
			if e.CRTs >= dataRange.CommitTsStart && e.CRTs <= dataRange.CommitTsEnd {
				entries = append(entries, e)
				positions = append(positions, encodeMockScanPosition(i))
			}
			continue
		}
		if e.CRTs > dataRange.CommitTsStart && e.CRTs <= dataRange.CommitTsEnd {
			entries = append(entries, e)
			positions = append(positions, encodeMockScanPosition(i))
		}
	}

	var iter eventstore.EventIterator
	if len(entries) != 0 {
		iter = &mockEventIterator{events: entries, positions: positions}
	}
	return iter, nil
}

func (m *mockEventStore) GetLogCoordinatorNodeID() node.ID {
	return ""
}

func (m *mockEventStore) RegisterDispatcher(
	changefeedID common.ChangeFeedID,
	dispatcherID common.DispatcherID,
	span *heartbeatpb.TableSpan,
	startTS common.Ts,
	notifier eventstore.ResolvedTsNotifier,
	_ bool,
	_ bool,
	_ bool,
) bool {
	if m.registerDispatcherHook != nil && !m.registerDispatcherHook() {
		return false
	}
	log.Info("subscribe table span", zap.Any("dispatcherID", dispatcherID),
		zap.Uint64("startTs", startTS),
		zap.Any("span", common.FormatTableSpan(span)))
	spanStats := &mockSpanStats{
		startTs:            startTS,
		resolvedTsNotifier: notifier,
		pendingEvents:      make([]*common.RawKVEntry, 0),
	}
	spanStats.resolvedTs = startTS
	m.spansMap.Store(span, spanStats)
	m.dispatcherMap.Store(dispatcherID, span)
	return true
}

type mockEventIterator struct {
	events       []*common.RawKVEntry
	positions    []eventstore.ScanPosition
	prevStartTS  uint64
	prevCommitTS uint64
	rowCount     int
	closeErr     error
}

func (iter *mockEventIterator) Next() (*common.RawKVEntry, bool) {
	row, _, isNewTxn := iter.NextWithScanPosition()
	return row, isNewTxn
}

func (iter *mockEventIterator) NextWithScanPosition() (*common.RawKVEntry, eventstore.ScanPosition, bool) {
	if len(iter.events) == 0 {
		return nil, nil, false
	}

	row := iter.events[0]
	iter.events = iter.events[1:]
	var position eventstore.ScanPosition
	if len(iter.positions) > 0 {
		position = iter.positions[0]
		iter.positions = iter.positions[1:]
	} else {
		position = encodeMockScanPosition(iter.rowCount)
	}
	isNewTxn := iter.prevCommitTS == 0 || row.StartTs != iter.prevStartTS || row.CRTs != iter.prevCommitTS

	iter.prevStartTS = row.StartTs
	iter.prevCommitTS = row.CRTs
	iter.rowCount++
	return row, position, isNewTxn
}

func (m *mockEventIterator) Close() (int64, error) {
	return int64(m.rowCount), m.closeErr
}

func encodeMockScanPosition(index int) eventstore.ScanPosition {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(index))
	position := make(eventstore.ScanPosition, len(buf))
	copy(position, buf[:])
	return position
}

func decodeMockScanPosition(position eventstore.ScanPosition) int {
	if len(position) == 0 {
		return -1
	}
	return int(binary.BigEndian.Uint64(position))
}

var _ schemastore.SchemaStore = &mockSchemaStore{}

type mockSpanStats struct {
	mu                 sync.RWMutex
	startTs            uint64
	resolvedTs         uint64
	pendingEvents      []*common.RawKVEntry
	resolvedTsNotifier func(watermark uint64, latestCommitTs uint64)
}

func (m *mockSpanStats) getAllEvents() []*common.RawKVEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	events := make([]*common.RawKVEntry, 0, len(m.pendingEvents))
	events = append(events, m.pendingEvents...)
	return events
}

func (m *mockSpanStats) getResolvedTs() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.resolvedTs
}

func (m *mockSpanStats) update(resolvedTs uint64, events ...*common.RawKVEntry) {
	m.mu.Lock()
	m.pendingEvents = append(m.pendingEvents, events...)
	m.resolvedTs = resolvedTs
	m.mu.Unlock()

	m.resolvedTsNotifier(resolvedTs, m.latestCommitTs())
}

func (m *mockSpanStats) latestCommitTs() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.pendingEvents) == 0 {
		return 0
	}
	return m.pendingEvents[len(m.pendingEvents)-1].CRTs
}

var _ DispatcherInfo = &mockDispatcherInfo{}

// mockDispatcherInfo is a mock implementation of the AcceptorInfo interface
type mockDispatcherInfo struct {
	changefeedID      common.ChangeFeedID
	clusterID         uint64
	serverID          string
	id                common.DispatcherID
	topic             string
	span              *heartbeatpb.TableSpan
	startTs           uint64
	actionType        eventpb.ActionType
	filterConfig      *eventpb.FilterConfig
	bdrMode           bool
	onlyReuse         bool
	integrity         *integrity.Config
	mode              int64
	epoch             uint64
	enableSyncPoint   bool
	nextSyncPoint     uint64
	syncPointInterval time.Duration
	lowLatencyMode    bool
}

func newMockDispatcherInfo(t *testing.T, startTs uint64, dispatcherID common.DispatcherID, tableID int64, actionType eventpb.ActionType) *mockDispatcherInfo {
	return &mockDispatcherInfo{
		clusterID:    1,
		serverID:     "server1",
		id:           dispatcherID,
		changefeedID: common.NewChangefeedID4Test("default", "test"),
		topic:        "topic1",
		span: &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
		startTs:    startTs,
		actionType: actionType,
		filterConfig: &eventpb.FilterConfig{
			FilterConfig: &eventpb.InnerFilterConfig{
				Rules: []string{"*.*"},
			},
		},
		bdrMode:   false,
		integrity: config.GetDefaultReplicaConfig().Integrity,
	}
}

func (m *mockDispatcherInfo) GetID() common.DispatcherID {
	return m.id
}

func (m *mockDispatcherInfo) GetClusterID() uint64 {
	return m.clusterID
}

func (m *mockDispatcherInfo) GetTopic() string {
	return m.topic
}

func (m *mockDispatcherInfo) GetServerID() string {
	return m.serverID
}

func (m *mockDispatcherInfo) GetTableSpan() *heartbeatpb.TableSpan {
	return m.span
}

func (m *mockDispatcherInfo) GetStartTs() uint64 {
	return m.startTs
}

func (m *mockDispatcherInfo) GetActionType() eventpb.ActionType {
	return m.actionType
}

func (m *mockDispatcherInfo) GetChangefeedID() common.ChangeFeedID {
	return m.changefeedID
}

func (m *mockDispatcherInfo) IsLowLatencyMode() bool {
	return m.lowLatencyMode
}

func (m *mockDispatcherInfo) GetFilterConfig() *eventpb.FilterConfig {
	return m.filterConfig
}

func (m *mockDispatcherInfo) SyncPointEnabled() bool {
	return m.enableSyncPoint
}

func (m *mockDispatcherInfo) GetSyncPointTs() uint64 {
	return m.nextSyncPoint
}

func (m *mockDispatcherInfo) GetSyncPointInterval() time.Duration {
	return m.syncPointInterval
}

func (m *mockDispatcherInfo) IsOnlyReuse() bool {
	return m.onlyReuse
}

func (m *mockDispatcherInfo) GetBdrMode() bool {
	return m.bdrMode
}

func (m *mockDispatcherInfo) GetIntegrity() *integrity.Config {
	return m.integrity
}

func (m *mockDispatcherInfo) GetMode() int64 {
	return m.mode
}

func (m *mockDispatcherInfo) GetEpoch() uint64 {
	return m.epoch
}

func (m *mockDispatcherInfo) IsOutputRawChangeEvent() bool {
	return false
}

func (m *mockDispatcherInfo) EnableIgnoreUpdateOnlyColumns() bool {
	return false
}

func (m *mockDispatcherInfo) GetTxnAtomicity() config.AtomicityLevel {
	return config.DefaultAtomicityLevel()
}

func newChangefeedStatusForTest(t testing.TB, info DispatcherInfo) *changefeedStatus {
	t.Helper()

	status := newChangefeedStatus(info.GetChangefeedID(), info.GetSyncPointInterval())
	status.lowLatencyMode = info.IsLowLatencyMode()
	status.filter = newChangefeedFilterForTest(t, info, time.UTC.String())
	return status
}

func addChangefeedStatusToBrokerForTest(
	t testing.TB,
	broker *eventBroker,
	changefeedID common.ChangeFeedID,
	syncPointInterval time.Duration,
) *changefeedStatus {
	t.Helper()

	status := newChangefeedStatus(changefeedID, syncPointInterval)
	broker.changefeedMap.Store(changefeedID, status)
	return status
}

func mustInitChangefeedStatusFilter(t testing.TB, status *changefeedStatus, info DispatcherInfo, timezone string) {
	t.Helper()
	if status.filter != nil {
		return
	}
	status.filter = newChangefeedFilterForTest(t, info, timezone)
}

func newChangefeedFilterForTest(t testing.TB, info DispatcherInfo, timezone string) filter.Filter {
	t.Helper()

	changefeedFilter, err := filter.GetSharedFilterStorage().
		GetOrSetFilter(info.GetChangefeedID(), info.GetFilterConfig(), timezone)
	require.NoError(t, err)
	return changefeedFilter
}

func genEvents(helper *commonEvent.EventTestHelper, ddl string, dmls ...string) (commonEvent.DDLEvent, []*common.RawKVEntry) {
	job := helper.DDL2Job(ddl)
	kvEvents := helper.DML2RawKv(job.TableID, job.BinlogInfo.FinishedTS, dmls...)
	return commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		FinishedTs: job.BinlogInfo.FinishedTS,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		Query:      ddl,
		TableInfo:  common.WrapTableInfo(job.SchemaName, job.BinlogInfo.TableInfo),
	}, kvEvents
}

// insertToDeleteRow converts an insert row to a delete row to facilitate the test.
func insertToDeleteRow(rawEvent *common.RawKVEntry) *common.RawKVEntry {
	res := &common.RawKVEntry{
		StartTs:  rawEvent.StartTs,
		CRTs:     rawEvent.CRTs,
		Key:      rawEvent.Key,
		OldValue: rawEvent.Value,
		OpType:   common.OpTypeDelete,
	}
	return res
}

// This test is to test the mockEventIterator works as expected.
func TestMockEventIterator(t *testing.T) {
	iter := &mockEventIterator{
		events: make([]*common.RawKVEntry, 0),
	}

	// Case 1: empty iterator
	row, isNewTxn := iter.Next()
	require.False(t, isNewTxn)
	require.Nil(t, row)

	// Case 2: iterator with 2 txns that has 2 rows
	row1 := &common.RawKVEntry{
		StartTs: 1,
		CRTs:    5,
	}
	row2 := &common.RawKVEntry{
		StartTs: 2,
		CRTs:    5,
	}

	iter.events = append(iter.events, row1, row1)
	iter.events = append(iter.events, row2, row2)

	// txn-1, row-1
	row, isNewTxn = iter.Next()
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn-1, row-2
	row, isNewTxn = iter.Next()
	require.False(t, isNewTxn)
	require.NotNil(t, row)

	// txn-2, row1
	row, isNewTxn = iter.Next()
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn2, row2
	row, isNewTxn = iter.Next()
	require.False(t, isNewTxn)
	require.NotNil(t, row)
}
