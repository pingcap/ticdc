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
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
<<<<<<< HEAD
	"github.com/pingcap/ticdc/utils/chann"
=======
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/pingcap/tidb/pkg/meta/model"
>>>>>>> 46132a925 (server: fence capture writes with etcd and P2P leases (#6092))
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
	statistics := metrics.NewStatistics(changefeedID, common.DefaultKeyspaceID, "sink")
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
<<<<<<< HEAD
=======

func TestKafkaSinkWriteGateBlocksDMLSend(t *testing.T) {
	eventHelper := commonEvent.NewEventTestHelper(t)
	defer eventHelper.Close()
	eventHelper.Tk().MustExec("use test")
	require.NotNil(t, eventHelper.DDL2Job("create table t (id int primary key)"))
	dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (1)")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kafkaSink, topicManager, asyncProducer, _ := newKafkaSinkForTest(
		t, ctx, config.ProtocolOpen, &config.SinkConfig{})
	topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(1), nil)
	sent := make(chan struct{}, 1)
	asyncProducer.EXPECT().AsyncRunCallback(gomock.Any()).Return(nil).AnyTimes()
	asyncProducer.EXPECT().AsyncSend(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			_ context.Context,
			_ string,
			_ int32,
			message *codecCommon.Message,
		) error {
			if message.Callback != nil {
				message.Callback()
			}
			sent <- struct{}{}
			return nil
		}).Times(1)
	gate := writelease.NewGate()
	kafkaSink.SetWriteGate(gate)

	runDone := make(chan error, 1)
	go func() {
		runDone <- kafkaSink.Run(ctx)
	}()
	kafkaSink.AddDMLEvent(dmlEvent)

	select {
	case <-sent:
		t.Fatal("Kafka DML was sent while the capture write gate was closed")
	case <-time.After(100 * time.Millisecond):
	}

	require.True(t, gate.RenewEtcd(time.Now(), writelease.EtcdProofDuration))
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("Kafka DML was not sent after the capture write gate reopened")
	}

	cancel()
	require.ErrorIs(t, <-runDone, context.Canceled)
}

func TestKafkaSinkBatchConfig(t *testing.T) {
	sink := &sink{}
	require.Equal(t, 4096, sink.BatchCount())
	require.Zero(t, sink.BatchBytes())
}

func TestKafkaSinkConstructionAndCleanup(t *testing.T) {
	t.Run("async producer creation fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		factory := kafka.NewMockFactory(ctrl)
		adminClient := kafka.NewMockAdminClient(ctrl)
		topicManager := topicmanager.NewMockTopicManager(ctrl)
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()

		factory.EXPECT().AsyncProducer(gomock.Any()).Return(nil, cause)
		gomock.InOrder(
			adminClient.EXPECT().Close(),
			topicManager.EXPECT().Close(),
		)

		kafkaSink, err := newWithComponents(
			t.Context(),
			common.NewChangefeedID4Test("test", "async-creation-fails"),
			common.DefaultKeyspaceID,
			config.ProtocolOpen,
			components{factory: factory, adminClient: adminClient, topicManager: topicManager},
		)

		require.Nil(t, kafkaSink)
		require.Equal(t, cause, err)
	})

	t.Run("sync producer creation fails", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		factory := kafka.NewMockFactory(ctrl)
		adminClient := kafka.NewMockAdminClient(ctrl)
		topicManager := topicmanager.NewMockTopicManager(ctrl)
		asyncProducer := kafka.NewMockAsyncProducer(ctrl)
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()

		factory.EXPECT().AsyncProducer(gomock.Any()).Return(asyncProducer, nil)
		factory.EXPECT().SyncProducer(gomock.Any()).Return(nil, cause)
		gomock.InOrder(
			asyncProducer.EXPECT().Close(),
			adminClient.EXPECT().Close(),
			topicManager.EXPECT().Close(),
		)

		kafkaSink, err := newWithComponents(
			t.Context(),
			common.NewChangefeedID4Test("test", "sync-creation-fails"),
			common.DefaultKeyspaceID,
			config.ProtocolOpen,
			components{factory: factory, adminClient: adminClient, topicManager: topicManager},
		)

		require.Nil(t, kafkaSink)
		require.Equal(t, cause, err)
	})

	t.Run("successful construction owns resources until close", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		factory := kafka.NewMockFactory(ctrl)
		adminClient := kafka.NewMockAdminClient(ctrl)
		topicManager := topicmanager.NewMockTopicManager(ctrl)
		asyncProducer := kafka.NewMockAsyncProducer(ctrl)
		syncProducer := kafka.NewMockSyncProducer(ctrl)
		var closeCount atomic.Int64

		factory.EXPECT().AsyncProducer(gomock.Any()).Return(asyncProducer, nil)
		factory.EXPECT().SyncProducer(gomock.Any()).Return(syncProducer, nil)
		factory.EXPECT().MetricsCollector(adminClient).Return(noopMetricsCollector{})
		gomock.InOrder(
			syncProducer.EXPECT().Close().Do(func() { closeCount.Add(1) }),
			asyncProducer.EXPECT().Close().Do(func() { closeCount.Add(1) }),
			adminClient.EXPECT().Close().Do(func() { closeCount.Add(1) }),
			topicManager.EXPECT().Close().Do(func() { closeCount.Add(1) }),
		)

		kafkaSink, err := newWithComponents(
			t.Context(),
			common.NewChangefeedID4Test("test", "successful-construction"),
			common.DefaultKeyspaceID,
			config.ProtocolOpen,
			components{factory: factory, adminClient: adminClient, topicManager: topicManager},
		)

		require.NoError(t, err)
		require.Zero(t, closeCount.Load())
		require.True(t, kafkaSink.IsNormal())

		kafkaSink.Close()
		require.Equal(t, int64(4), closeCount.Load())
		require.False(t, kafkaSink.IsNormal())
		kafkaSink.AddDMLEvent(&commonEvent.DMLEvent{})
		require.Zero(t, kafkaSink.eventChan.Len())

		_, ok, err := kafkaSink.eventChan.GetWithContext(t.Context())
		require.NoError(t, err)
		require.False(t, ok)
		_, ok, err = kafkaSink.rowChan.GetWithContext(t.Context())
		require.NoError(t, err)
		require.False(t, ok)
	})
}

func TestKafkaSinkDML(t *testing.T) {
	eventHelper := commonEvent.NewEventTestHelper(t)
	defer eventHelper.Close()
	eventHelper.Tk().MustExec("use test")
	require.NotNil(t, eventHelper.DDL2Job("create table t (id int primary key, name varchar(32))"))

	t.Run("routes DML event and forwards producer callback", func(t *testing.T) {
		var callbackCount atomic.Int64
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (1, 'one')")
		dmlEvent.PostTxnFlushed = []func(){func() { callbackCount.Add(1) }}

		sent := make(chan *codecCommon.Message, 1)
		ctx, cancel := context.WithCancelCause(t.Context())
		kafkaSink, topicManager, asyncProducer, _ := newKafkaSinkForTest(
			t, ctx, config.ProtocolOpen, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(1), nil)
		asyncProducer.EXPECT().AsyncSend(gomock.Any(), kafkaSinkTestTopic, int32(0), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ int32, message *codecCommon.Message) error {
				sent <- message
				return nil
			})

		runDone := make(chan error, 1)
		go func() { runDone <- kafkaSink.sendDMLEvent(ctx) }()
		kafkaSink.AddDMLEvent(dmlEvent)

		select {
		case message := <-sent:
			require.NotEmpty(t, message.Key)
			require.NotEmpty(t, message.Value)
			require.Equal(t, 1, message.GetRowsCount())
			require.NotNil(t, message.Callback)
			require.Zero(t, callbackCount.Load())
			message.Callback()
			require.Equal(t, int64(1), callbackCount.Load())
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for Kafka Sink to send the DML message")
		}

		cause := errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		cancel(cause)
		select {
		case err := <-runDone:
			require.Equal(t, cause, err)
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for Kafka Sink workers to exit")
		}
		require.Equal(t, int64(1), callbackCount.Load())
	})

	t.Run("returns AsyncSend error unchanged", func(t *testing.T) {
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (2, 'two')")
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		kafkaSink, topicManager, asyncProducer, _ := newKafkaSinkForTest(
			t, ctx, config.ProtocolCanalJSON, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(1), nil)
		asyncProducer.EXPECT().AsyncSend(gomock.Any(), kafkaSinkTestTopic, int32(0), gomock.Any()).Return(cause)

		kafkaSink.AddDMLEvent(dmlEvent)
		err := kafkaSink.sendDMLEvent(ctx)

		require.Equal(t, cause, err)
	})

	t.Run("returns topic manager error unchanged", func(t *testing.T) {
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (3, 'three')")
		kafkaSink, topicManager, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		cause := context.DeadlineExceeded
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(0), cause)
		kafkaSink.AddDMLEvent(dmlEvent)

		require.Equal(t, cause, kafkaSink.calculateKeyPartitions(t.Context()))
	})

	t.Run("canceled after topic lookup", func(t *testing.T) {
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (4, 'four')")
		ctx, cancel := context.WithCancelCause(t.Context())
		kafkaSink, topicManager, _, _ := newKafkaSinkForTest(
			t, ctx, config.ProtocolOpen, &config.SinkConfig{})
		cause := errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).
			DoAndReturn(func(context.Context, string) (int32, error) {
				cancel(cause)
				return 1, nil
			})
		kafkaSink.AddDMLEvent(dmlEvent)
		require.Equal(t, cause, kafkaSink.calculateKeyPartitions(ctx))
		require.Zero(t, kafkaSink.rowChan.Len())
	})
}

func TestKafkaSinkDDL(t *testing.T) {
	ddlEvent := &commonEvent.DDLEvent{
		Type:       byte(model.ActionCreateTable),
		SchemaName: "test",
		TableName:  "t",
		Query:      "create table test.t (id int primary key)",
		FinishedTs: 1,
	}

	t.Run("all partitions", func(t *testing.T) {
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(4), nil)
		syncProducer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(4), gomock.Any()).
			DoAndReturn(func(_ string, _ int32, message *codecCommon.Message) error {
				require.NotEmpty(t, message.Key)
				require.NotEmpty(t, message.Value)
				return nil
			})

		require.NoError(t, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("partition zero", func(t *testing.T) {
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolCanalJSON, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(4), nil)
		syncProducer.EXPECT().SendMessage(kafkaSinkTestTopic, int32(0), gomock.Any()).
			DoAndReturn(func(_ string, _ int32, message *codecCommon.Message) error {
				require.NotEmpty(t, message.Value)
				return nil
			})

		require.NoError(t, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("topic manager error", func(t *testing.T) {
		kafkaSink, topicManager, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		cause := context.DeadlineExceeded
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(0), cause)

		require.Equal(t, cause, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("producer error marks sink abnormal", func(t *testing.T) {
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(2), nil)
		syncProducer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(2), gomock.Any()).Return(cause)

		require.Equal(t, cause, kafkaSink.WriteBlockEvent(ddlEvent))
		require.False(t, kafkaSink.IsNormal())
	})

	t.Run("nil encoded message", func(t *testing.T) {
		kafkaSink, _, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolDebezium, &config.SinkConfig{})
		unsupportedDDL := &commonEvent.DDLEvent{Type: byte(model.ActionNone), Query: "unsupported"}

		require.NoError(t, kafkaSink.sendDDLEvent(unsupportedDDL))
	})

	t.Run("unsupported block event", func(t *testing.T) {
		kafkaSink, _, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		syncPoint := commonEvent.NewSyncPointEvent(common.NewDispatcherID(), 1, 1, 1)

		require.ErrorIs(t, kafkaSink.WriteBlockEvent(syncPoint), errors.ErrInvalidEventType)
	})
}

func TestKafkaSinkCheckpoint(t *testing.T) {
	t.Run("default topic without tables", func(t *testing.T) {
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(3), nil)
		syncProducer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(3), gomock.Any()).
			DoAndReturn(func(_ string, _ int32, message *codecCommon.Message) error {
				require.NotEmpty(t, message.Key)
				return nil
			})
		kafkaSink.checkpointChan <- 100
		close(kafkaSink.checkpointChan)

		require.NoError(t, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("all active topics", func(t *testing.T) {
		sinkConfig := &config.SinkConfig{DispatchRules: []*config.DispatchRule{
			{Matcher: []string{"db1.t1"}, PartitionRule: "table", TopicRule: "topic-a"},
			{Matcher: []string{"db2.t2"}, PartitionRule: "table", TopicRule: "topic-b"},
		}}
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, sinkConfig)
		kafkaSink.SetTableSchemaStore(commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{
			{SchemaName: "db1", Tables: []*heartbeatpb.TableInfo{{TableName: "t1"}}},
			{SchemaName: "db2", Tables: []*heartbeatpb.TableInfo{{TableName: "t2"}}},
		}, common.KafkaSinkType, false))
		// The checkpoint must be fanned out to every active topic: the two
		// rule topics and the default topic.
		partitionCounts := map[string]int32{"topic-a": 2, "topic-b": 3, kafkaSinkTestTopic: 4}
		for topic, partitionCount := range partitionCounts {
			topicManager.EXPECT().GetPartitionNum(gomock.Any(), topic).Return(partitionCount, nil)
			syncProducer.EXPECT().SendMessages(topic, partitionCount, gomock.Any()).Return(nil)
		}
		kafkaSink.checkpointChan <- 100
		close(kafkaSink.checkpointChan)

		require.NoError(t, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("topic manager error", func(t *testing.T) {
		kafkaSink, topicManager, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, &config.SinkConfig{})
		cause := context.DeadlineExceeded
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(0), cause)
		kafkaSink.checkpointChan <- 100

		require.Equal(t, cause, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("producer error stops active topic fan-out", func(t *testing.T) {
		sinkConfig := &config.SinkConfig{DispatchRules: []*config.DispatchRule{
			{Matcher: []string{"db1.t1"}, PartitionRule: "table", TopicRule: "topic-a"},
		}}
		kafkaSink, topicManager, _, syncProducer := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolOpen, sinkConfig)
		kafkaSink.SetTableSchemaStore(commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{
			{SchemaName: "db1", Tables: []*heartbeatpb.TableInfo{{TableName: "t1"}}},
		}, common.KafkaSinkType, false))
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		// Fail whichever topic the fan-out reaches first: sendCheckpoint must
		// return the error and stop, so exactly one GetPartitionNum and one
		// SendMessages call are expected regardless of the topic order.
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), gomock.Any()).Return(int32(2), nil)
		syncProducer.EXPECT().SendMessages(gomock.Any(), int32(2), gomock.Any()).Return(cause)
		kafkaSink.checkpointChan <- 100

		require.Equal(t, cause, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("nil encoded message", func(t *testing.T) {
		kafkaSink, _, _, _ := newKafkaSinkForTest(
			t, t.Context(), config.ProtocolCanalJSON, &config.SinkConfig{})
		kafkaSink.checkpointChan <- 100
		close(kafkaSink.checkpointChan)

		require.NoError(t, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		kafkaSink, _, _, _ := newKafkaSinkForTest(t, ctx, config.ProtocolOpen, &config.SinkConfig{})
		cause := errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		cancel(cause)

		require.Equal(t, cause, kafkaSink.sendCheckpoint(ctx))
	})
}

func newKafkaSinkForTest(
	t *testing.T, ctx context.Context, protocol config.Protocol, sinkConfig *config.SinkConfig,
) (*sink, *topicmanager.MockTopicManager, *kafka.MockAsyncProducer, *kafka.MockSyncProducer) {
	t.Helper()

	ctrl := gomock.NewController(t)
	changefeedID := common.NewChangefeedID4Test("test", t.Name())
	protocolName := protocol.String()
	testSinkConfig := *sinkConfig
	testSinkConfig.Protocol = &protocolName
	sinkConfig = &testSinkConfig
	router, err := eventrouter.NewEventRouter(sinkConfig, kafkaSinkTestTopic, false, false)
	require.NoError(t, err)
	columnSelector, err := columnselector.New(sinkConfig)
	require.NoError(t, err)
	encoderConfig := codecCommon.NewConfig(protocol).WithChangefeedID(changefeedID)
	encoderConfig.MaxBatchSize = 1
	encoderGroup, err := codec.NewEncoderGroup(sinkConfig, encoderConfig, nil, nil, changefeedID)
	require.NoError(t, err)
	encoder, err := codec.NewEventEncoder(encoderConfig, nil, nil)
	require.NoError(t, err)
	topicManager := topicmanager.NewMockTopicManager(ctrl)
	asyncProducer := kafka.NewMockAsyncProducer(ctrl)
	syncProducer := kafka.NewMockSyncProducer(ctrl)
	topicManager.EXPECT().Close().AnyTimes()
	asyncProducer.EXPECT().Close().AnyTimes()
	syncProducer.EXPECT().Close().AnyTimes()
	factory := kafka.NewMockFactory(ctrl)
	factory.EXPECT().AsyncProducer(gomock.Any()).Return(asyncProducer, nil)
	factory.EXPECT().SyncProducer(gomock.Any()).Return(syncProducer, nil)
	factory.EXPECT().MetricsCollector(nil).Return(noopMetricsCollector{})

	kafkaSink, err := newWithComponents(ctx, changefeedID, common.DefaultKeyspaceID, protocol, components{
		encoderGroup:   encoderGroup,
		encoder:        encoder,
		columnSelector: columnSelector,
		eventRouter:    router,
		topicManager:   topicManager,
		factory:        factory,
	})
	require.NoError(t, err)
	t.Cleanup(kafkaSink.Close)

	return kafkaSink, topicManager, asyncProducer, syncProducer
}
>>>>>>> 46132a925 (server: fence capture writes with etcd and P2P leases (#6092))
