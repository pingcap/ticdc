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
	"math"
	"sort"
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
	mockSchemaStore := newMockSchemaStore()
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
	mockStore.Run(ctx)

	mc := &mockMessageCenter{
		messageCh: make(chan *messaging.TargetMessage, 100),
	}
	esImpl := startEventService(ctx, t, mc, mockStore)
	esImpl.Close(ctx)

	dispatcherInfo := newMockDispatcherInfo(t, common.NewDispatcherID(), 1, eventpb.ActionType_ACTION_TYPE_REGISTER)
	// register acceptor
	esImpl.registerDispatcher(ctx, dispatcherInfo)
	require.Equal(t, 1, len(esImpl.brokers))
	require.NotNil(t, esImpl.brokers[dispatcherInfo.GetClusterID()])

	// add events to eventStore
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`, []string{
		`insert into test.t(id,c) values (0, "c0")`,
		`insert into test.t(id,c) values (1, "c1")`,
		`insert into test.t(id,c) values (2, "c2")`,
	}...)
	require.NotNil(t, kvEvents)
	schemastore := esImpl.schemaStore.(*mockSchemaStore)
	schemastore.AppendDDLEvent(dispatcherInfo.span.TableID, ddlEvent)

	resolvedTs := kvEvents[0].CRTs + 1
	mockStore.AppendEvents(dispatcherInfo.id, resolvedTs, kvEvents[0])
	// receive events from msg center
	msgCnt := 0
	dmlCount := 0
	for {
		msg := <-mc.messageCh
		log.Info("receive message", zap.Any("message", msg))
		for _, m := range msg.Message {
			msgCnt++
			switch e := m.(type) {
			case *commonEvent.ReadyEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, dispatcherInfo.id, e.DispatcherID)
				require.Equal(t, uint64(0), e.GetSeq())
				log.Info("receive ready event", zap.Any("event", e))
				// 1. When a Dispatcher is register, it will send a ReadyEvent to the eventCollector.
				// 2. The eventCollector will send a reset request to the eventService.
				// 3. We are here to simulate the reset request.
				esImpl.resetDispatcher(dispatcherInfo)
				mockStore.AppendEvents(dispatcherInfo.id, resolvedTs+1)
			case *commonEvent.HandshakeEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, dispatcherInfo.id, e.DispatcherID)
				require.Equal(t, dispatcherInfo.startTs, e.GetStartTs())
				require.Equal(t, uint64(1), e.Seq)
				log.Info("receive handshake event", zap.Any("event", e))
				mockStore.AppendEvents(dispatcherInfo.id, kvEvents[1].CRTs+1, kvEvents[1])
				mockStore.AppendEvents(dispatcherInfo.id, kvEvents[2].CRTs+1, kvEvents[2])
			case *commonEvent.BatchDMLEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				// first dml has one event, sencond dml has two events
				if dmlCount == 0 {
					require.Equal(t, int32(1), e.Len())
				} else if dmlCount == 1 {
					require.Equal(t, int32(2), e.Len())
				}
				dmlCount += len(e.DMLEvents)
				require.Equal(t, kvEvents[dmlCount-1].CRTs, e.GetCommitTs())
				require.Equal(t, uint64(dmlCount+2), e.GetSeq())
			case *commonEvent.DDLEvent:
				require.NotNil(t, msg)
				require.Equal(t, "event-collector", msg.Topic)
				require.Equal(t, ddlEvent.FinishedTs, e.FinishedTs)
				require.Equal(t, uint64(2), e.Seq)
			case *commonEvent.BatchResolvedEvent:
				require.NotNil(t, msg)
				log.Info("receive watermark", zap.Uint64("ts", e.Events[0].ResolvedTs))
			}
		}
		if msgCnt == 5 {
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

func (m *mockEventStore) UnregisterDispatcher(dispatcherID common.DispatcherID) {
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if ok {
		m.spansMap.Delete(span)
	}
}

func (m *mockEventStore) GetIterator(dispatcherID common.DispatcherID, dataRange common.DataRange) (eventstore.EventIterator, error) {
	iter := &mockEventIterator{
		events: make([]*common.RawKVEntry, 0),
	}
	span, ok := m.dispatcherMap.Load(dispatcherID)
	if !ok {
		return nil, fmt.Errorf("dispatcher not found: %v", dispatcherID)
	}

	v, ok := m.spansMap.Load(span)
	if !ok {
		return nil, fmt.Errorf("span not found: %v, dispatcherID: %v", span, dispatcherID)
	}

	spanStats := v.(*mockSpanStats)
	events := spanStats.getAllEvents()
	for _, e := range events {
		if e.CRTs > dataRange.StartTs && e.CRTs <= dataRange.EndTs {
			iter.events = append(iter.events, e)
		}
	}
	return iter, nil
}

func (m *mockEventStore) RegisterDispatcher(
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

func (iter *mockEventIterator) Next() (*common.RawKVEntry, bool, error) {
	if len(iter.events) == 0 {
		return nil, false, nil
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
	return row, isNewTxn, nil
}

func (m *mockEventIterator) Close() (int64, error) {
	return 0, nil
}

var _ schemastore.SchemaStore = &mockSchemaStore{}

type mockSchemaStore struct {
	DDLEvents map[common.TableID][]commonEvent.DDLEvent
	TableInfo map[common.TableID][]*common.TableInfo

	resolvedTs     uint64
	maxDDLCommitTs uint64
}

func newMockSchemaStore() *mockSchemaStore {
	return &mockSchemaStore{
		DDLEvents:      make(map[common.TableID][]commonEvent.DDLEvent),
		TableInfo:      make(map[common.TableID][]*common.TableInfo),
		resolvedTs:     math.MaxUint64,
		maxDDLCommitTs: math.MaxUint64,
	}
}

func (m *mockSchemaStore) Name() string {
	return "mockSchemaStore"
}

func (m *mockSchemaStore) Run(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStore) Close(ctx context.Context) error {
	return nil
}

func (m *mockSchemaStore) AppendDDLEvent(id common.TableID, ddls ...commonEvent.DDLEvent) {
	for _, ddl := range ddls {
		m.DDLEvents[id] = append(m.DDLEvents[id], ddl)
		m.TableInfo[id] = append(m.TableInfo[id], ddl.TableInfo)
	}
}

func (m *mockSchemaStore) GetTableInfo(tableID common.TableID, ts common.Ts) (*common.TableInfo, error) {
	infos := m.TableInfo[tableID]
	idx := sort.Search(len(infos), func(i int) bool {
		return infos[i].UpdateTS() > uint64(ts)
	})
	if idx == 0 {
		return nil, nil
	}
	return infos[idx-1], nil
}

func (m *mockSchemaStore) GetAllPhysicalTables(snapTs uint64, filter filter.Filter) ([]commonEvent.Table, error) {
	return nil, nil
}

func (m *mockSchemaStore) GetTableDDLEventState(tableID int64) schemastore.DDLEventState {
	return schemastore.DDLEventState{
		ResolvedTs:       m.resolvedTs,
		MaxEventCommitTs: m.maxDDLCommitTs,
	}
}

func (m *mockSchemaStore) RegisterTable(
	tableID int64,
	startTS common.Ts,
) error {
	return nil
}

func (m *mockSchemaStore) UnregisterTable(tableID int64) error {
	return nil
}

// GetNextDDLEvents returns the next ddl event which finishedTs is within the range (start, end]
func (m *mockSchemaStore) FetchTableDDLEvents(tableID int64, tableFilter filter.Filter, start, end uint64) ([]commonEvent.DDLEvent, error) {
	events := m.DDLEvents[tableID]
	if len(events) == 0 {
		return nil, nil
	}
	l := sort.Search(len(events), func(i int) bool {
		return events[i].FinishedTs > start
	})
	if l == len(events) {
		return nil, nil
	}
	r := sort.Search(len(events), func(i int) bool {
		return events[i].FinishedTs > end
	})
	return events[l:r], nil
}

func (m *mockSchemaStore) FetchTableTriggerDDLEvents(tableFilter filter.Filter, start uint64, limit int) ([]commonEvent.DDLEvent, uint64, error) {
	return nil, 0, nil
}

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
	clusterID  uint64
	serverID   string
	id         common.DispatcherID
	topic      string
	span       *heartbeatpb.TableSpan
	startTs    uint64
	actionType eventpb.ActionType
	filter     filter.Filter
	bdrMode    bool
	integrity  *integrity.Config
	tz         *time.Location
}

func newMockDispatcherInfo(t *testing.T, dispatcherID common.DispatcherID, tableID int64, actionType eventpb.ActionType) *mockDispatcherInfo {
	cfg := config.NewDefaultFilterConfig()
	filter, err := filter.NewFilter(cfg, "", false, false)
	require.NoError(t, err)
	return &mockDispatcherInfo{
		clusterID: 1,
		serverID:  "server1",
		id:        dispatcherID,
		topic:     "topic1",
		span: &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
		startTs:    1,
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
	return common.NewChangefeedID4Test("default", "test")
}

func (m *mockDispatcherInfo) GetFilterConfig() *config.FilterConfig {
	return &config.FilterConfig{
		Rules: []string{"*.*"},
	}
}

func (m *mockDispatcherInfo) SyncPointEnabled() bool {
	return false
}

func (m *mockDispatcherInfo) GetSyncPointTs() uint64 {
	return 0
}

func (m *mockDispatcherInfo) GetSyncPointInterval() time.Duration {
	return 0
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
	row, isNewTxn, err := iter.Next()
	require.Nil(t, err)
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
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn-1, row-2
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.NotNil(t, row)

	// txn-2, row1
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.True(t, isNewTxn)
	require.NotNil(t, row)
	// txn2, row2
	row, isNewTxn, err = iter.Next()
	require.Nil(t, err)
	require.False(t, isNewTxn)
	require.NotNil(t, row)
}
