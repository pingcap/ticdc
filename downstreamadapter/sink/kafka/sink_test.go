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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

const kafkaSinkTestTopic = "mock_topic"

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

	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{kafkaSinkTestTopic}, true).Return(
		map[string]kafka.TopicDetail{
			kafkaSinkTestTopic: {
				Name:          kafkaSinkTestTopic,
				NumPartitions: 1,
			},
		}, nil)
	adminClient.EXPECT().Close().AnyTimes()

	metricsCollector := kafka.NewMockMetricsCollector(ctrl)
	metricsCollector.EXPECT().Run(gomock.Any()).AnyTimes()

	factory := kafka.NewMockFactory(ctrl)
	factory.EXPECT().AsyncProducer(gomock.Any()).Return(asyncProducer, nil)
	factory.EXPECT().SyncProducer(gomock.Any()).Return(syncProducer, nil)
	factory.EXPECT().MetricsCollector(adminClient).Return(metricsCollector)

	comp, protocol, err := newKafkaSinkComponentWithConfig(
		ctx, changefeedID, sinkURI, sinkConfig,
		config.ProtocolOpen, kafkaSinkTestTopic, 1048576,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        false,
			PartitionNum:      1,
			ReplicationFactor: 1,
		},
		factory, adminClient,
	)
	if err != nil {
		return nil, err
	}

	s, err := newWithComponents(ctx, changefeedID, protocol, comp)
	if err != nil {
		return nil, err
	}
	go s.Run(ctx)
	return s, nil
}

func TestKafkaSinkBasicFunctionality(t *testing.T) {
	var count atomic.Int64
	fixture := commonEvent.NewSinkTestEventFixture(t,
		"create table t (id int primary key, name varchar(32));",
		"insert into t values (1, 'test')",
		"insert into t values (2, 'test2');")
	postFlush := func() { count.Add(1) }
	ddlEvent := fixture.NewDDLEvent(1, postFlush)
	dmlEvent := fixture.NewDMLEvent(postFlush)

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
			message *codeccommon.Message,
		) error {
			if message.Callback != nil {
				message.Callback()
			}
			return nil
		}).Times(fixture.DMLCount)
	asyncProducer.EXPECT().Close().AnyTimes()
	syncProducer.EXPECT().SendMessages(gomock.Any(), int32(1), gomock.Any()).Return(nil)
	syncProducer.EXPECT().Close().AnyTimes()

	kafkaSink, err := newKafkaSinkForTestWithProducers(ctx, t, ctrl, asyncProducer, syncProducer)
	require.NoError(t, err)
	defer cancel()

	err = kafkaSink.WriteBlockEvent(ddlEvent)
	require.NoError(t, err)

	kafkaSink.AddDMLEvent(dmlEvent)

	require.Eventually(t,
		func() bool {
			return count.Load() == int64(2)
		}, 5*time.Second, 10*time.Millisecond)

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
