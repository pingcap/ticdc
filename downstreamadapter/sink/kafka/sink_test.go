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
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/sink/topicmanager"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
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
	schemaRegistry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid response", http.StatusInternalServerError)
	}))
	defer schemaRegistry.Close()

	avroProtocol := config.ProtocolAvro.String()
	sinkConfig := &config.SinkConfig{
		Protocol:       &avroProtocol,
		SchemaRegistry: &schemaRegistry.URL,
	}
	sinkURI, err := url.Parse("kafka://127.0.0.1:9092/" + kafkaSinkTestTopic +
		"?required-acks=1&kafka-version=2.4.0")
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	factory := kafka.NewMockFactory(ctrl)
	gomock.InOrder(
		factory.EXPECT().AdminClient(gomock.Any()).Return(adminClient, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{kafkaSinkTestTopic}, true).Return(
			map[string]kafka.TopicDetail{kafkaSinkTestTopic: {Name: kafkaSinkTestTopic}}, nil),
		adminClient.EXPECT().Close(),
	)

	originalCreateKafkaFactory := createKafkaFactory
	createKafkaFactory = func(_ func() (kafka.Factory, error)) (kafka.Factory, error) {
		return factory, nil
	}
	t.Cleanup(func() {
		createKafkaFactory = originalCreateKafkaFactory
	})

	changefeedID := common.NewChangefeedID4Test("test", "verify-invalid-config")
	err = Verify(context.Background(), changefeedID, sinkURI, sinkConfig)
	require.ErrorContains(t, err, "ErrAvroSchemaAPIError")
}

func newKafkaSinkForTestWithProducers(ctx context.Context,
	t *testing.T,
	ctrl *gomock.Controller,
	asyncProducer kafka.AsyncProducer,
	syncProducer kafka.SyncProducer,
) (*sink, error) {
	t.Helper()

	changefeedID := common.NewChangefeedID4Test("test", "test")
	openProtocol := config.ProtocolOpen.String()
	sinkConfig := &config.SinkConfig{Protocol: &openProtocol}
	uriTemplate := "kafka://%s/%s?kafka-version=0.9.0.0&max-batch-size=1" +
		"&max-message-bytes=1048576&partition-num=1" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&protocol=open-protocol"
	uri := fmt.Sprintf(uriTemplate, "127.0.0.1:9092", kafkaSinkTestTopic)

	sinkURI, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	protocol, err := helper.GetProtocol(openProtocol)
	if err != nil {
		return nil, err
	}
	topic, err := helper.GetTopic(sinkURI)
	if err != nil {
		return nil, err
	}
	options := kafka.NewOptions()
	if err = options.Apply(changefeedID, sinkURI, sinkConfig); err != nil {
		return nil, err
	}
	options.Topic = topic

	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{kafkaSinkTestTopic}, true).Return(
		map[string]kafka.TopicDetail{
			kafkaSinkTestTopic: {
				Name:          kafkaSinkTestTopic,
				NumPartitions: 1,
			},
		}, nil)
	adminClient.EXPECT().Close().AnyTimes()

	factory := kafka.NewMockFactory(ctrl)
	factory.EXPECT().AsyncProducer(gomock.Any()).Return(asyncProducer, nil)
	factory.EXPECT().SyncProducer(gomock.Any()).Return(syncProducer, nil)
	factory.EXPECT().MetricsCollector(adminClient).Return(noopMetricsCollector{})

	eventRouter, err := eventrouter.NewEventRouter(sinkConfig, topic, false, false)
	if err != nil {
		return nil, err
	}
	columnSelector, err := columnselector.New(sinkConfig)
	if err != nil {
		return nil, err
	}
	encoderConfig, err := helper.GetEncoderConfig(
		changefeedID, sinkURI, protocol, sinkConfig,
		options.MaxMessageBytes, options.MaxBatchedBytes,
	)
	if err != nil {
		return nil, err
	}
	encoderGroup, err := codec.NewEncoderGroup(ctx, sinkConfig, encoderConfig, nil, changefeedID)
	if err != nil {
		return nil, err
	}
	encoder, err := codec.NewEventEncoder(ctx, encoderConfig, nil)
	if err != nil {
		return nil, err
	}
	topicManager, err := topicmanager.GetTopicManagerAndTryCreateTopic(
		ctx,
		changefeedID,
		topic,
		options.DeriveTopicConfig(),
		adminClient,
	)
	if err != nil {
		return nil, err
	}

	comp := components{
		encoderGroup:   encoderGroup,
		encoder:        encoder,
		columnSelector: columnSelector,
		eventRouter:    eventRouter,
		topicManager:   topicManager,
		adminClient:    adminClient,
		factory:        factory,
	}

	// We must close adminClient when this func return cause by an error
	// otherwise the adminClient will never be closed and lead to a goroutine leak.
	defer func() {
		if err != nil && comp.adminClient != nil {
			comp.close()
		}
	}()

	s, err := newWithComponents(ctx, changefeedID, common.DefaultKeyspaceID, protocol, comp)
	if err != nil {
		return nil, err
	}
	return s, nil
}

func TestKafkaSinkRunReturnsAsyncProducerError(t *testing.T) {
	ctx := t.Context()

	ctrl := gomock.NewController(t)
	producerErr := errors.ErrKafkaSendMessage.GenWithStackByArgs()
	asyncProducer := kafka.NewMockAsyncProducer(ctrl)
	syncProducer := kafka.NewMockSyncProducer(ctrl)
	asyncProducer.EXPECT().AsyncRunCallback(gomock.Any()).Return(producerErr)
	asyncProducer.EXPECT().Close().AnyTimes()
	syncProducer.EXPECT().Close().AnyTimes()

	kafkaSink, err := newKafkaSinkForTestWithProducers(ctx, t, ctrl, asyncProducer, syncProducer)
	require.NoError(t, err)
	defer kafkaSink.Close()

	err = kafkaSink.Run(ctx)

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
	ctrl := gomock.NewController(t)
	asyncProducer := kafka.NewMockAsyncProducer(ctrl)
	syncProducer := kafka.NewMockSyncProducer(ctrl)
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
	asyncProducer.EXPECT().Close().AnyTimes()
	syncProducer.EXPECT().SendMessages(gomock.Any(), int32(1), gomock.Any()).Return(nil)
	syncProducer.EXPECT().Close().AnyTimes()

	kafkaSink, err := newKafkaSinkForTestWithProducers(ctx, t, ctrl, asyncProducer, syncProducer)
	require.NoError(t, err)
	defer cancel()
	go kafkaSink.Run(ctx)

	err = kafkaSink.WriteBlockEvent(ddlEvent)
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

	t.Run("forwards routed message and waits for producer callback", func(t *testing.T) {
		var callbackCount atomic.Int64
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (1, 'one')")
		dmlEvent.PostTxnFlushed = []func(){func() { callbackCount.Add(1) }}

		ctrl := gomock.NewController(t)
		asyncProducer := kafka.NewMockAsyncProducer(ctrl)
		syncProducer := kafka.NewMockSyncProducer(ctrl)
		sent := make(chan *codecCommon.Message, 1)
		asyncProducer.EXPECT().AsyncSend(gomock.Any(), kafkaSinkTestTopic, int32(0), gomock.Any()).
			DoAndReturn(func(_ context.Context, _ string, _ int32, message *codecCommon.Message) error {
				sent <- message
				return nil
			})
		asyncProducer.EXPECT().Close()
		syncProducer.EXPECT().Close()

		ctx, cancel := context.WithCancelCause(t.Context())
		kafkaSink, err := newKafkaSinkForTestWithProducers(ctx, t, ctrl, asyncProducer, syncProducer)
		require.NoError(t, err)
		defer kafkaSink.Close()

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
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for Kafka Sink to send the DML message")
		}

		cause := errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		cancel(cause)
		require.Equal(t, cause, <-runDone)
		require.Zero(t, callbackCount.Load())
	})

	t.Run("returns AsyncSend error unchanged", func(t *testing.T) {
		dmlEvent := eventHelper.DML2Event("test", "t", "insert into t values (2, 'two')")
		ctrl := gomock.NewController(t)
		asyncProducer := kafka.NewMockAsyncProducer(ctrl)
		syncProducer := kafka.NewMockSyncProducer(ctrl)
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		asyncProducer.EXPECT().AsyncSend(gomock.Any(), kafkaSinkTestTopic, int32(0), gomock.Any()).Return(cause)
		asyncProducer.EXPECT().Close()
		syncProducer.EXPECT().Close()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		kafkaSink, err := newKafkaSinkForTestWithProducers(ctx, t, ctrl, asyncProducer, syncProducer)
		require.NoError(t, err)
		defer kafkaSink.Close()

		kafkaSink.AddDMLEvent(dmlEvent)
		err = kafkaSink.sendDMLEvent(ctx)

		require.Equal(t, cause, err)
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
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(4), nil)
		producer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(4), gomock.Any()).
			DoAndReturn(func(_ string, _ int32, message *codecCommon.Message) error {
				require.NotEmpty(t, message.Key)
				require.NotEmpty(t, message.Value)
				return nil
			})

		require.NoError(t, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("partition zero", func(t *testing.T) {
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolCanalJSON, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(4), nil)
		producer.EXPECT().SendMessage(kafkaSinkTestTopic, int32(0), gomock.Any()).
			DoAndReturn(func(_ string, _ int32, message *codecCommon.Message) error {
				require.NotEmpty(t, message.Value)
				return nil
			})

		require.NoError(t, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("topic manager error", func(t *testing.T) {
		kafkaSink, topicManager, _ := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		cause := context.DeadlineExceeded
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(0), cause)

		require.Equal(t, cause, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("producer error", func(t *testing.T) {
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(2), nil)
		producer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(2), gomock.Any()).Return(cause)

		require.Equal(t, cause, kafkaSink.sendDDLEvent(ddlEvent))
	})

	t.Run("nil encoded message", func(t *testing.T) {
		kafkaSink, _, _ := newKafkaSinkForTest(t, config.ProtocolDebezium, &config.SinkConfig{})
		unsupportedDDL := &commonEvent.DDLEvent{Type: byte(model.ActionNone), Query: "unsupported"}

		require.NoError(t, kafkaSink.sendDDLEvent(unsupportedDDL))
	})
}

func TestKafkaSinkCheckpoint(t *testing.T) {
	t.Run("default topic without tables", func(t *testing.T) {
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(3), nil)
		producer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(3), gomock.Any()).
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
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolOpen, sinkConfig)
		kafkaSink.SetTableSchemaStore(commonEvent.NewTableSchemaStore([]*heartbeatpb.SchemaInfo{
			{SchemaName: "db1", Tables: []*heartbeatpb.TableInfo{{TableName: "t1"}}},
			{SchemaName: "db2", Tables: []*heartbeatpb.TableInfo{{TableName: "t2"}}},
		}, common.KafkaSinkType, false))
		partitionCounts := map[string]int32{"topic-a": 2, "topic-b": 3, kafkaSinkTestTopic: 4}
		for topic, partitionCount := range partitionCounts {
			topicManager.EXPECT().GetPartitionNum(gomock.Any(), topic).Return(partitionCount, nil)
			producer.EXPECT().SendMessages(topic, partitionCount, gomock.Any()).Return(nil)
		}
		kafkaSink.checkpointChan <- 100
		close(kafkaSink.checkpointChan)

		require.NoError(t, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("topic manager error", func(t *testing.T) {
		kafkaSink, topicManager, _ := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		cause := context.DeadlineExceeded
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(0), cause)
		kafkaSink.checkpointChan <- 100

		require.Equal(t, cause, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("producer error", func(t *testing.T) {
		kafkaSink, topicManager, producer := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		cause := errors.ErrKafkaSendMessage.GenWithStackByArgs()
		topicManager.EXPECT().GetPartitionNum(gomock.Any(), kafkaSinkTestTopic).Return(int32(2), nil)
		producer.EXPECT().SendMessages(kafkaSinkTestTopic, int32(2), gomock.Any()).Return(cause)
		kafkaSink.checkpointChan <- 100

		require.Equal(t, cause, kafkaSink.sendCheckpoint(t.Context()))
	})

	t.Run("context cancellation", func(t *testing.T) {
		kafkaSink, _, _ := newKafkaSinkForTest(t, config.ProtocolOpen, &config.SinkConfig{})
		ctx, cancel := context.WithCancelCause(t.Context())
		cause := errors.ErrKafkaSinkClosed.GenWithStackByArgs()
		cancel(cause)

		require.Equal(t, cause, kafkaSink.sendCheckpoint(ctx))
	})
}

func newKafkaSinkForTest(
	t *testing.T,
	protocol config.Protocol,
	sinkConfig *config.SinkConfig,
) (*sink, *topicmanager.MockTopicManager, *kafka.MockSyncProducer) {
	t.Helper()

	ctrl := gomock.NewController(t)
	changefeedID := common.NewChangefeedID4Test("test", t.Name())
	router, err := eventrouter.NewEventRouter(sinkConfig, kafkaSinkTestTopic, false, false)
	require.NoError(t, err)
	encoder, err := codec.NewEventEncoder(t.Context(), codecCommon.NewConfig(protocol).WithChangefeedID(changefeedID), nil)
	require.NoError(t, err)
	statistics := metrics.NewStatistics(changefeedID, common.DefaultKeyspaceID, "sink")
	t.Cleanup(statistics.Close)
	topicManager := topicmanager.NewMockTopicManager(ctrl)
	producer := kafka.NewMockSyncProducer(ctrl)

	return &sink{
		changefeedID:  changefeedID,
		ddlProducer:   producer,
		partitionRule: helper.GetDDLDispatchRule(protocol),
		protocol:      protocol,
		comp: components{
			encoder:      encoder,
			eventRouter:  router,
			topicManager: topicManager,
		},
		statistics:     statistics,
		checkpointChan: make(chan uint64, 1),
		ctx:            t.Context(),
	}, topicManager, producer
}
