// Copyright 2022 PingCAP, Inc.
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

package topicmanager

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
<<<<<<< HEAD
	manager := newKafkaTopicManager(ctx, kafka.DefaultMockTopicName, changefeedID, adminClient, cfg)
=======
	var gotNewTopicDetail *kafka.TopicDetail
	var gotNewTopicValidateOnly bool
	var gotFailedTopicDetail *kafka.TopicDetail
	var gotFailedTopicValidateOnly bool
	gomock.InOrder(
		adminClient.EXPECT().GetTopicsMeta([]string{kafkaTopicManagerTestTopic}, true).Return(
			map[string]kafka.TopicDetail{
				kafkaTopicManagerTestTopic: {
					Name:          kafkaTopicManagerTestTopic,
					NumPartitions: 2,
				},
			}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail *kafka.TopicDetail, validateOnly bool) error {
				gotNewTopicDetail = detail
				gotNewTopicValidateOnly = validateOnly
				return nil
			}),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(
			map[string]kafka.TopicDetail{
				"new-topic": {
					Name:          "new-topic",
					NumPartitions: 2,
				},
			}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic2"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic2"}, false).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic-failed"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic-failed"}, false).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail *kafka.TopicDetail, validateOnly bool) error {
				gotFailedTopicDetail = detail
				gotFailedTopicValidateOnly = validateOnly
				return errors.WrapError(errors.ErrKafkaAdminAPI, sarama.ErrInvalidReplicationFactor, "create-topic", detail.Name)
			}),
	)

	manager := newKafkaTopicManager(ctx, kafkaTopicManagerTestTopic, changefeedID, adminClient, cfg)
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafka.DefaultMockTopicName)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
	manager = newKafkaTopicManager(ctx, "new-topic2", changefeedID, adminClient, cfg)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)

	topic := "new-topic-failed"
	// Invalid replication factor.
	// It happens when replication-factor is greater than the number of brokers.
	cfg = &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 4,
	}
	manager = newKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, topic)
<<<<<<< HEAD
	require.Regexp(
		t,
		"kafka create topic failed: kafka server: Replication-factor is invalid",
		err,
	)
=======
	require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
	require.ErrorIs(t, err, sarama.ErrInvalidReplicationFactor)
	require.NotNil(t, gotFailedTopicDetail)
	require.Equal(t, "new-topic-failed", gotFailedTopicDetail.Name)
	require.False(t, gotFailedTopicValidateOnly)
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
}

func TestCreateTopicWithDelay(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
=======
	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	topic := "new-topic"
	gomock.InOrder(
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, true).
			Return(map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, false).
			Return(map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetBrokerConfig(kafka.MinInsyncReplicasConfigName).
			Return("2", true, nil),
	)

	manager := newKafkaTopicManager(
		context.Background(),
		topic,
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
			RequiredAcks:      kafka.WaitForAll,
		},
	)
	defer manager.Close()

	_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), topic)
	require.ErrorContains(t, err, "`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker")
}

func TestCreateTopicWaitsUntilVisible(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	topic := "new_topic"
	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	manager := newKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.createTopic(ctx, topic)
	require.NoError(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible(topic, 3)
	require.NoError(t, err)
	err = manager.waitUntilTopicVisible(ctx, topic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
}
