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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	"github.com/pingcap/ticdc/logservice/schemastore"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
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
	es := New(mockStore, mockSchemaStore)
	esImpl := es.(*eventService)
	go func() {
		err := esImpl.Run(ctx)
		if err != nil {
			t.Errorf("EventService.Run() error = %v", err)
		}
	}()
	return esImpl
}

func TestEventServiceBasic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Info("start event service basic test")

	mockStore := newMockEventStore(100)
	_ = mockStore.Run(ctx)

	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}
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
	for {
		msg := <-mc.messageCh
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

var _ messaging.MessageCenter = &mockMessageCenter{}

// mockMessageCenter is a mock implementation of the MessageCenter interface
type mockMessageCenter struct {
	messageCh chan *messaging.TargetMessage
}

func newMockMessageCenter() *mockMessageCenter {
	return &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}
}

func (m *mockMessageCenter) OnNodeChanges(nodeInfos map[node.ID]*node.Info) {
}

func (m *mockMessageCenter) SendEvent(event *messaging.TargetMessage) error {
	m.messageCh <- event
	return nil
}

func (m *mockMessageCenter) SendCommand(command *messaging.TargetMessage) error {
	m.messageCh <- command
	return nil
}

func (m *mockMessageCenter) RegisterHandler(topic string, handler messaging.MessageHandler) {
}

func (m *mockMessageCenter) DeRegisterHandler(topic string) {
}

func (m *mockMessageCenter) AddTarget(id node.ID, epoch uint64, addr string) {
}

func (m *mockMessageCenter) RemoveTarget(id node.ID) {
}

func (m *mockMessageCenter) Close() {
}

func (m *mockMessageCenter) IsReadyToSend(id node.ID) bool {
	return true
}

var _ eventstore.EventStore = &mockEventStore{}

// mockEventStore is a mock implementation of the EventStore interface
type mockEventStore struct {
	resolvedTsUpdateInterval time.Duration
	dispatcherMap            sync.Map // key is common.DispatcherID, value is span
	spansMap                 sync.Map // key is *heartbeatpb.TableSpan
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
	}
}

func (m *mockEventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) eventstore.EventIterator {
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
	for _, e := range events {
		if e.CRTs > dataRange.CommitTsStart && e.CRTs <= dataRange.CommitTsEnd {
			entries = append(entries, e)
		}
	}

	var iter eventstore.EventIterator
	if len(entries) != 0 {
		iter = &mockEventIterator{events: entries}
	}
	return iter
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
) bool {
	log.Info("subscribe table span", zap.Any("span", span), zap.Uint64("startTs", startTS), zap.Any("dispatcherID", dispatcherID))
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
	prevStartTS  uint64
	prevCommitTS uint64
	rowCount     int
}

func (iter *mockEventIterator) Next() (*common.RawKVEntry, bool) {
	if len(iter.events) == 0 {
		return nil, false
	}

	row := iter.events[0]
	iter.events = iter.events[1:]
	isNewTxn := false
	if iter.prevCommitTS == 0 || row.StartTs != iter.prevStartTS || row.CRTs != iter.prevCommitTS {
		isNewTxn = true
	}
	iter.prevStartTS = row.StartTs
	iter.prevCommitTS = row.CRTs
	iter.rowCount++
	return row, isNewTxn
}

func (m *mockEventIterator) Close() (int64, error) {
	return 0, nil
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
	filter            filter.Filter
	bdrMode           bool
	integrity         *integrity.Config
	tz                *time.Location
	mode              int64
	epoch             uint64
	enableSyncPoint   bool
	nextSyncPoint     uint64
	syncPointInterval time.Duration
}

func newMockDispatcherInfo(t *testing.T, startTs uint64, dispatcherID common.DispatcherID, tableID int64, actionType eventpb.ActionType) *mockDispatcherInfo {
	cfg := config.NewDefaultFilterConfig()
	filter, err := filter.NewFilter(cfg, "", false, false)
	require.NoError(t, err)
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
		filter:     filter,
		bdrMode:    false,
		integrity:  config.GetDefaultReplicaConfig().Integrity,
		tz:         time.Local,
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

func (m *mockDispatcherInfo) GetFilterConfig() *config.FilterConfig {
	return &config.FilterConfig{
		Rules: []string{"*.*"},
	}
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

func (m *mockDispatcherInfo) GetFilter() filter.Filter {
	return m.filter
}

func (m *mockDispatcherInfo) IsOnlyReuse() bool {
	return false
}

func (m *mockDispatcherInfo) GetBdrMode() bool {
	return m.bdrMode
}

func (m *mockDispatcherInfo) GetIntegrity() *integrity.Config {
	return m.integrity
}

func (m *mockDispatcherInfo) GetTimezone() *time.Location {
	return m.tz
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

func genEvents(helper *commonEvent.EventTestHelper, ddl string, dmls ...string) (commonEvent.DDLEvent, []*common.RawKVEntry) {
	job := helper.DDL2Job(ddl)
	kvEvents := helper.DML2RawKv(job.TableID, job.BinlogInfo.FinishedTS, dmls...)
	return commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion,
		FinishedTs: job.BinlogInfo.FinishedTS,
		TableID:    job.BinlogInfo.TableInfo.ID,
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
