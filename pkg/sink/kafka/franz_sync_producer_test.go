// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestSyncProducerClosed(t *testing.T) {
	producer := &syncProducer{}
	producer.closed.Store(true)

	err := producer.SendMessage(t.Context(), "topic", 1, &codeccommon.Message{})
	require.ErrorIs(t, err, errors.ErrKafkaSinkClosed)

	err = producer.SendMessages(t.Context(), "topic", 1, &codeccommon.Message{})
	require.ErrorIs(t, err, errors.ErrKafkaSinkClosed)
}

func TestSyncProducerPartitions(t *testing.T) {
	const topic = "sync-topic"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	defer cluster.Close()
	o := testOptions(cluster.ListenAddrs())

	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &syncProducer{
		id:     common.NewChangefeedID4Test(common.DefaultKeyspaceName, "sync"),
		client: client,
	}
	defer producer.Close()

	require.NoError(t, producer.SendMessage(t.Context(), topic, 2, &codeccommon.Message{Key: []byte("key"), Value: []byte("value")}))
	require.NoError(t, producer.SendMessages(t.Context(), topic, 3, &codeccommon.Message{Value: []byte("all")}))
}

func TestSyncProducerPartialFailure(t *testing.T) {
	const topic = "partial-failure"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	defer cluster.Close()

	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		return produceResponseWithError(req, 1, kerr.InvalidTopicException.Code)
	})
	o := testOptions(cluster.ListenAddrs())

	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &syncProducer{
		id:     common.NewChangefeedID4Test(common.DefaultKeyspaceName, "partial"),
		client: client,
	}
	defer producer.Close()

	err = producer.SendMessages(t.Context(), topic, 3, &codeccommon.Message{Value: []byte("value")})
	require.ErrorIs(t, err, errors.ErrKafkaSendMessage)
	require.ErrorIs(t, err, kerr.InvalidTopicException)
}

func TestSyncProducerContext(t *testing.T) {
	o := testOptions([]string{"127.0.0.1:1"})
	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &syncProducer{
		id:     common.NewChangefeedID4Test(common.DefaultKeyspaceName, "canceled"),
		client: client,
	}
	defer producer.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err = producer.SendMessage(ctx, "topic", 0, &codeccommon.Message{})
	require.ErrorIs(t, err, errors.ErrKafkaSendMessage)
	require.ErrorIs(t, err, context.Canceled)
}

func TestSyncProducerCloseIsIdempotent(t *testing.T) {
	o := testOptions([]string{"127.0.0.1:1"})
	client, err := kgo.NewClient(append(testClientOptions(t, o), producerOptions(o)...)...)
	require.NoError(t, err)
	defer client.Close()
	producer := &syncProducer{
		id:     common.NewChangefeedID4Test(common.DefaultKeyspaceName, "close"),
		client: client,
	}

	producer.Close()
	producer.Close()
}

func produceResponseWithError(req kmsg.Request, failedPartition int32, errorCode int16) (kmsg.Response, error, bool) {
	request := req.(*kmsg.ProduceRequest)
	response := request.ResponseKind().(*kmsg.ProduceResponse)

	for _, requestTopic := range request.Topics {
		responseTopic := kmsg.NewProduceResponseTopic()
		responseTopic.Topic = requestTopic.Topic
		responseTopic.TopicID = requestTopic.TopicID

		for _, requestPartition := range requestTopic.Partitions {
			responsePartition := kmsg.NewProduceResponseTopicPartition()
			responsePartition.Partition = requestPartition.Partition
			if requestPartition.Partition == failedPartition {
				responsePartition.ErrorCode = errorCode
			}
			responseTopic.Partitions = append(responseTopic.Partitions, responsePartition)
		}

		response.Topics = append(response.Topics, responseTopic)
	}
	return response, nil, true
}
