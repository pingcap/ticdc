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

package kafka

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama/mocks"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/utils/chann"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newKafkaSinkForTestWithProducers(ctx context.Context,
	asyncProducer kafka.AsyncProducer,
	syncProducer kafka.SyncProducer,
) (*sink, error) {
	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := "open-protocol"
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafka.DefaultMockTopicName)

	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, errors.Trace(err)
	}
	statistics := metrics.NewStatistics(changefeedID, "sink")
	comp, protocol, err := newKafkaSinkComponentForTest(ctx, changefeedID, sinkURI, sinkConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && comp.adminClient != nil {
			comp.close()
		}
	}()

	if asyncProducer == nil {
		asyncProducer, err = comp.factory.AsyncProducer(ctx)
		if err != nil {
			return nil, err
		}
	}

	if syncProducer == nil {
		syncProducer, err = comp.factory.SyncProducer(ctx)
		if err != nil {
			return nil, err
		}
	}

	s := &sink{
		changefeedID:     changefeedID,
		dmlProducer:      asyncProducer,
		ddlProducer:      syncProducer,
		metricsCollector: comp.factory.MetricsCollector(comp.adminClient),

		partitionRule: helper.GetDDLDispatchRule(protocol),
		protocol:      protocol,
		comp:          comp,
		statistics:    statistics,

		checkpointChan: make(chan uint64, 16),
		eventChan:      chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:        chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),

		isNormal: atomic.NewBool(true),
		ctx:      ctx,
	}
	go s.Run(ctx)
	return s, nil
}

func newKafkaSinkForTest(ctx context.Context) (*sink, error) {
	return newKafkaSinkForTestWithProducers(ctx, nil, nil)
}

type staticTopicManager struct {
	partitionNum int32
}

func (m staticTopicManager) GetPartitionNum(context.Context, string) (int32, error) {
	return m.partitionNum, nil
}

func (m staticTopicManager) CreateTopicAndWaitUntilVisible(context.Context, string) (int32, error) {
	return m.partitionNum, nil
}

func (m staticTopicManager) Close() {}

// mockSyncProducer is used to count the calls to Heartbeat.
type mockSyncProducer struct {
	kafka.MockSaramaSyncProducer
	heartbeatCount int
	mu             sync.Mutex
}

func (m *mockSyncProducer) Heartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
}

func (m *mockSyncProducer) GetHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

func TestDDLProducerHeartbeat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	producer := &mockSyncProducer{}
	heartbeatInterval := 5 * time.Second
	_, err := newKafkaSinkForTestWithProducers(ctx, nil, producer)
	require.NoError(t, err)

	// Wait for a sufficient amount of time to ensure the heartbeat ticker triggers several times.
	// Waiting for 11 seconds to allow for at least two heartbeats.
	// Use Eventually to avoid test flakiness.
	require.Eventually(t, func() bool {
		return producer.GetHeartbeatCount() >= 2
	}, 11*time.Second, 150*time.Millisecond, "Heartbeat should be called periodically")

	// Verify that closing the manager stops the heartbeat.
	countBeforeClose := producer.GetHeartbeatCount()
	cancel()
	// Wait for a short period to ensure no new heartbeats occur.
	time.Sleep(heartbeatInterval * 2)
	require.Equal(t, countBeforeClose, producer.GetHeartbeatCount(), "Heartbeat should stop after manager is closed")
}

// mockSyncProducer is used to count the calls to Heartbeat.
type mockAsyncProducer struct {
	kafka.MockSaramaAsyncProducer
	heartbeatCount int
	mu             sync.Mutex
}

func (m *mockAsyncProducer) Heartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
}

func (m *mockAsyncProducer) GetHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

func TestDMLProducerHeartbeat(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	producer := &mockAsyncProducer{}
	producer.AsyncProducer = mocks.NewAsyncProducer(t, nil)
	heartbeatInterval := 5 * time.Second
	_, err := newKafkaSinkForTestWithProducers(ctx, producer, nil)
	require.NoError(t, err)

	// Wait for a sufficient amount of time to ensure the heartbeat ticker triggers several times.
	// Waiting for 11 seconds to allow for at least two heartbeats.
	// Use Eventually to avoid test flakiness.
	require.Eventually(t, func() bool {
		return producer.GetHeartbeatCount() >= 2
	}, 11*time.Second, 150*time.Millisecond, "Heartbeat should be called periodically")

	// Verify that closing the manager stops the heartbeat.
	countBeforeClose := producer.GetHeartbeatCount()
	cancel()
	// Wait for a short period to ensure no new heartbeats occur.
	time.Sleep(heartbeatInterval * 2)
	require.Equal(t, countBeforeClose, producer.GetHeartbeatCount(), "Heartbeat should stop after manager is closed")
}

func TestKafkaSinkBasicFunctionality(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("use test")
	createTableSQL := "create table t (id int primary key, name varchar(32));"
	job := helper.DDL2Job(createTableSQL)
	require.NotNil(t, job)

	var count atomic.Int64
	ddlEvent := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		TableInfo:  common.WrapTableInfo(job.SchemaName, job.BinlogInfo.TableInfo),
		FinishedTs: 1,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	ddlEvent2 := &commonEvent.DDLEvent{
		Query:      job.Query,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		TableInfo:  common.WrapTableInfo(job.SchemaName, job.BinlogInfo.TableInfo),
		FinishedTs: 4,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeNormal,
			TableIDs:      []int64{0},
		},
		NeedAddedTables: []commonEvent.Table{{TableID: 1, SchemaID: 1}},
		PostTxnFlushed: []func(){
			func() { count.Add(1) },
		},
	}

	dmlEvent := helper.DML2Event("test", "t",
		"insert into t values (1, 'test')",
		"insert into t values (2, 'test2');")
	dmlEvent.PostTxnFlushed = []func(){
		func() { count.Add(1) },
	}
	dmlEvent.CommitTs = 2

	ctx, cancel := context.WithCancel(context.Background())
	kafkaSink, err := newKafkaSinkForTest(ctx)
	require.NoError(t, err)
	defer cancel()

	kafkaSink.ddlProducer.(*kafka.MockSaramaSyncProducer).SyncProducer.ExpectSendMessageAndSucceed()
	err = kafkaSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	kafkaSink.dmlProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndSucceed()
	kafkaSink.dmlProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndSucceed()
	kafkaSink.AddDMLEvent(dmlEvent)

	ddlEvent2.PostFlush()

	require.Eventually(t,
		func() bool {
			return count.Load() == int64(3)
		}, 5*time.Second, time.Second)

	// case 2: add checkpoint ts when sink is closed and it will not block
	kafkaSink.Close(false)
	cancel()
	kafkaSink.AddCheckpointTs(12345)
}

func TestCalculateKeyPartitionsUsesSourceIdentityForColumnSelector(t *testing.T) {
	t.Parallel()

	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()

	helper.Tk().MustExec("create database source_db")
	helper.DDL2Job("create table source_db.orders (id int primary key, name varchar(32))")

	dmlEvent := helper.DML2Event("source_db", "orders", "insert into source_db.orders values (1, 'alice')")
	dmlEvent.TableInfo = dmlEvent.TableInfo.CloneWithRouting("target_db", "orders_routed")

	sinkConfig := &config.SinkConfig{
		DispatchRules: []*config.DispatchRule{
			{
				Matcher:       []string{"source_db.orders"},
				PartitionRule: "table",
				TopicRule:     "source_topic",
			},
			{
				Matcher:       []string{"target_db.orders_routed"},
				PartitionRule: "ts",
				TopicRule:     "target_topic",
			},
		},
		ColumnSelectors: []*config.ColumnSelector{
			{
				Matcher: []string{"source_db.orders"},
				Columns: []string{"id"},
			},
			{
				Matcher: []string{"target_db.orders_routed"},
				Columns: []string{"id", "name"},
			},
		},
	}

	router, err := eventrouter.NewEventRouter(sinkConfig, "default_topic", false, false)
	require.NoError(t, err)
	selectors, err := columnselector.New(sinkConfig)
	require.NoError(t, err)

	s := &sink{
		changefeedID: common.NewChangefeedID4Test("test", "test"),
		comp: components{
			eventRouter:    router,
			columnSelector: selectors,
			topicManager:   staticTopicManager{partitionNum: 4},
		},
		eventChan: chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		rowChan:   chann.NewUnlimitedChannelDefault[*commonEvent.MQRowEvent](),
	}

	s.eventChan.Push(dmlEvent)
	s.eventChan.Close()

	require.NoError(t, s.calculateKeyPartitions(context.Background()))

	mqEvent, ok := s.rowChan.Get()
	require.True(t, ok)
	require.Equal(t, "source_topic", mqEvent.Key.Topic)

	idCol := mustGetColumnByName(t, dmlEvent.TableInfo, "id")
	nameCol := mustGetColumnByName(t, dmlEvent.TableInfo, "name")
	require.True(t, mqEvent.RowEvent.ColumnSelector.Select(idCol))
	require.False(t, mqEvent.RowEvent.ColumnSelector.Select(nameCol))
}

func mustGetColumnByName(t *testing.T, tableInfo *common.TableInfo, name string) *timodel.ColumnInfo {
	t.Helper()

	for _, col := range tableInfo.GetColumns() {
		if col != nil && col.Name.O == name {
			return col
		}
	}

	t.Fatalf("column %s not found", name)
	return nil
}
