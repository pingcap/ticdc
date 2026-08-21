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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"go.uber.org/atomic"
)

const kafkaSinkTestTopic = "mock_topic"

type noopMetricsCollector struct{}

func (noopMetricsCollector) Run(context.Context) {}

func TestSinkWorkersReturnContextError(t *testing.T) {
	contexts := []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
		cause      error
	}{
		{
			name: "canceled",
			newContext: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			cause: context.Canceled,
		},
		{
			name: "deadline exceeded",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 0)
			},
			cause: context.DeadlineExceeded,
		},
	}
	workers := []struct {
		name string
		run  func(*sink, context.Context) error
	}{
		{name: "calculate key partitions", run: (*sink).calculateKeyPartitions},
		{name: "non batch encode", run: (*sink).nonBatchEncodeRun},
		{name: "checkpoint", run: (*sink).sendCheckpoint},
	}

	for _, worker := range workers {
		for _, contextCase := range contexts {
			t.Run(worker.name+"/"+contextCase.name, func(t *testing.T) {
				ctx, cancel := contextCase.newContext()
				defer cancel()

				err := worker.run(&sink{}, ctx)

				require.ErrorIs(t, err, contextCase.cause)
			})
		}
	}
}

func TestVerifyInvalidConfig(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, kafkaSinkTestTopic))
	defer cluster.Close()

	schemaRegistry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid response", http.StatusInternalServerError)
	}))
	defer schemaRegistry.Close()

	avroProtocol := config.ProtocolAvro.String()
	sinkConfig := &config.SinkConfig{
		Protocol:       &avroProtocol,
		SchemaRegistry: &schemaRegistry.URL,
	}
	sinkURI, err := url.Parse("kafka://" + cluster.ListenAddrs()[0] + "/" + kafkaSinkTestTopic +
		"?required-acks=1&kafka-version=2.4.0")
	require.NoError(t, err)

	changefeedID := common.NewChangefeedID4Test("test", "verify-invalid-config")
	err = Verify(context.Background(), changefeedID, sinkURI, sinkConfig)
	require.ErrorContains(t, err, "ErrAvroSchemaAPIError")
}

func TestKafkaSinkRunReturnsAsyncProducerError(t *testing.T) {
	ctx := t.Context()

	producerErr := errors.ErrKafkaSendMessage.GenWithStackByArgs()
	kafkaSink, _, asyncProducer, _ := newKafkaSinkForTest(t, ctx, config.ProtocolOpen, &config.SinkConfig{})
	asyncProducer.EXPECT().AsyncRunCallback(gomock.Any()).Return(producerErr)

	err := kafkaSink.Run(ctx)

	require.ErrorIs(t, err, errors.ErrKafkaSendMessage)
	require.False(t, kafkaSink.IsNormal())
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
	kafkaSink, topicManager, asyncProducer, syncProducer := newKafkaSinkForTest(
		t, ctx, config.ProtocolOpen, &config.SinkConfig{})
	topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(1), nil).AnyTimes()
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
			return nil
		}).Times(2)
	syncProducer.EXPECT().SendMessages(gomock.Any(), int32(1), gomock.Any()).Return(nil)
	defer cancel()
	go kafkaSink.Run(ctx)

	err := kafkaSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	kafkaSink.AddDMLEvent(dmlEvent)

	ddlEvent2.PostFlush()

	require.Eventually(t,
		func() bool {
			return count.Load() == int64(3)
		}, 5*time.Second, time.Second)

	// case 2: add checkpoint ts when sink is closed and it will not block
	kafkaSink.Close()
	cancel()
	kafkaSink.AddCheckpointTs(12345)
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
		adminClient.EXPECT().Close()
		topicManager.EXPECT().Close()

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
		asyncProducer.EXPECT().Close()
		adminClient.EXPECT().Close()
		topicManager.EXPECT().Close()

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
		asyncProducer.EXPECT().Close().Do(func() { closeCount.Add(1) })
		syncProducer.EXPECT().Close().Do(func() { closeCount.Add(1) })
		adminClient.EXPECT().Close().Do(func() { closeCount.Add(1) })
		topicManager.EXPECT().Close().Do(func() { closeCount.Add(1) })

		kafkaSink, err := newWithComponents(
			t.Context(),
			common.NewChangefeedID4Test("test", "successful-construction"),
			common.DefaultKeyspaceID,
			config.ProtocolOpen,
			components{factory: factory, adminClient: adminClient, topicManager: topicManager},
		)

		require.NoError(t, err)
		require.Zero(t, closeCount.Load())
		kafkaSink.Close()
		require.Equal(t, int64(4), closeCount.Load())
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
	encoderGroup, err := codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, nil, changefeedID)
	require.NoError(t, err)
	encoder, err := codec.NewEventEncoder(ctx, encoderConfig, nil)
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
