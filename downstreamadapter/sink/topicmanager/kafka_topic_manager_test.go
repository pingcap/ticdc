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

	"github.com/IBM/sarama"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

const kafkaTopicManagerTestTopic = "mock_topic"

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
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
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic-failed"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail *kafka.TopicDetail, validateOnly bool) error {
				gotFailedTopicDetail = detail
				gotFailedTopicValidateOnly = validateOnly
				return sarama.ErrInvalidReplicationFactor
			}),
	)

	manager := newKafkaTopicManager(ctx, kafkaTopicManagerTestTopic, changefeedID, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafkaTopicManagerTestTopic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.Equal(t, &kafka.TopicDetail{
		Name:              "new-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, gotNewTopicDetail)
	require.False(t, gotNewTopicValidateOnly)
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
	require.Regexp(
		t,
		"kafka create topic failed: kafka server: Replication-factor is invalid",
		err,
	)
	require.NotNil(t, gotFailedTopicDetail)
	require.Equal(t, "new-topic-failed", gotFailedTopicDetail.Name)
	require.False(t, gotFailedTopicValidateOnly)
}
