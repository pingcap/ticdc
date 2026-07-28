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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
<<<<<<< HEAD
	"github.com/pingcap/ticdc/pkg/metrics"
=======
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

<<<<<<< HEAD
=======
const kafkaSinkTestTopic = "mock_topic"

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
	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t).SetApiKeys(
			[]sarama.ApiVersionsResponseKey{
				{ApiKey: 0},
				{ApiKey: 1},
				{ApiKey: 2},
				{ApiKey: 3, MaxVersion: 9},
			}),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader(kafkaSinkTestTopic, 0, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	schemaRegistry := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid response", http.StatusInternalServerError)
	}))
	defer schemaRegistry.Close()

	avroProtocol := config.ProtocolAvro.String()
	sinkConfig := &config.SinkConfig{
		Protocol:       &avroProtocol,
		SchemaRegistry: &schemaRegistry.URL,
	}
	sinkURI, err := url.Parse("kafka://" + broker.Addr() + "/" + kafkaSinkTestTopic +
		"?required-acks=1&kafka-version=2.4.0")
	require.NoError(t, err)

	changefeedID := common.NewChangefeedID4Test("test", "verify-invalid-config")
	err = Verify(context.Background(), changefeedID, sinkURI, sinkConfig)
	require.ErrorContains(t, err, "ErrAvroSchemaAPIError")
}

>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
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
	return s, nil
}

<<<<<<< HEAD
func newKafkaSinkForTest(ctx context.Context) (*sink, error) {
	return newKafkaSinkForTestWithProducers(ctx, nil, nil)
=======
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
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
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
<<<<<<< HEAD
	kafkaSink, err := newKafkaSinkForTest(ctx)
=======
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
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
	require.NoError(t, err)
	defer cancel()
	go kafkaSink.Run(ctx)

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
	kafkaSink.Close()
	cancel()
	kafkaSink.AddCheckpointTs(12345)
}
