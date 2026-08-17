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
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

const kafkaTopicManagerTestTopic = "mock_topic"

type mockAdminClientWithDeniedDescribe struct {
	*kafka.MockClusterAdminClient
	createTopicCalled bool
	describeCount     int
}

func (m *mockAdminClientWithDeniedDescribe) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]kafka.TopicDetail, error) {
	m.describeCount++
	if ignoreTopicError {
		return map[string]kafka.TopicDetail{}, nil
	}
	return nil, sarama.ErrTopicAuthorizationFailed
}

func (m *mockAdminClientWithDeniedDescribe) CreateTopic(
	detail *kafka.TopicDetail,
) error {
	m.createTopicCalled = true
	return nil
}

type mockAdminClientWithDeniedCreate struct {
	*kafka.MockClusterAdminClient
	createTopicCalled bool
	describeCount     int
}

func (m *mockAdminClientWithDeniedCreate) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]kafka.TopicDetail, error) {
	m.describeCount++
	return map[string]kafka.TopicDetail{}, nil
}

func (m *mockAdminClientWithDeniedCreate) CreateTopic(
	detail *kafka.TopicDetail,
) error {
	m.createTopicCalled = true
	return sarama.ErrClusterAuthorizationFailed
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
		RequiredAcks:      kafka.WaitForAll,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	var gotNewTopicDetail *kafka.TopicDetail
	var gotFailedTopicDetail *kafka.TopicDetail
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
		adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
			func(detail *kafka.TopicDetail) error {
				gotNewTopicDetail = detail
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
		adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
			func(detail *kafka.TopicDetail) error {
				gotFailedTopicDetail = detail
				return errors.WrapError(errors.ErrKafkaAdminAPI, sarama.ErrInvalidReplicationFactor, "create-topic", detail.Name)
			}),
	)

	manager := newKafkaTopicManager(kafkaTopicManagerTestTopic, changefeedID, adminClient, cfg)
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafkaTopicManagerTestTopic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	cfg.RequiredAcks = kafka.WaitForLocal
	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.Equal(t, &kafka.TopicDetail{
		Name:              "new-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, gotNewTopicDetail)
	partitionsNum, err := manager.GetPartitionNum(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg = &kafka.AutoCreateTopicConfig{
		AutoCreate:        false,
		PartitionNum:      2,
		ReplicationFactor: 1,
		RequiredAcks:      kafka.WaitForAll,
	}
	manager = newKafkaTopicManager("new-topic2", changefeedID, adminClient, cfg)
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
	manager = newKafkaTopicManager(topic, changefeedID, adminClient, cfg)
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, topic)
	require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
	require.ErrorIs(t, err, sarama.ErrInvalidReplicationFactor)
	require.NotNil(t, gotFailedTopicDetail)
	require.Equal(t, "new-topic-failed", gotFailedTopicDetail.Name)
}

func TestCreateTopicValidatesReplicationFactor(t *testing.T) {
	t.Parallel()

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

	_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), topic)
	require.ErrorContains(t, err, "`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker")
}

func TestEnsureTopicExistsWaitsUntilVisible(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	topic := "delayed-topic"
	gomock.InOrder(
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			map[string]kafka.TopicDetail{}, nil),
		adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
			func(detail *kafka.TopicDetail) error {
				require.Equal(t, &kafka.TopicDetail{
					Name:              topic,
					NumPartitions:     2,
					ReplicationFactor: 1,
				}, detail)
				return nil
			}),
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			nil, sarama.ErrUnknownTopicOrPartition),
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			nil, sarama.ErrUnknownTopicOrPartition),
		adminClient.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			map[string]kafka.TopicDetail{
				topic: {
					Name:          topic,
					NumPartitions: 2,
				},
			}, nil),
	)

	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	err := EnsureTopic(ctx, changefeedID, topic, cfg, adminClient)
	require.NoError(t, err)
}

func TestGetTopicManagerStartsBackgroundRefreshAfterTopicReady(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	topic := "existing-topic"
	adminClient.EXPECT().GetTopicsMeta([]string{topic}, true).Return(
		map[string]kafka.TopicDetail{
			topic: {
				Name:          topic,
				NumPartitions: 2,
			},
		}, nil,
	)

	manager, err := GetTopicManagerAndTryCreateTopic(
		t.Context(),
		common.NewChangefeedID4Test("test", "test"),
		topic,
		&kafka.AutoCreateTopicConfig{PartitionNum: 2},
		adminClient,
	)
	require.NoError(t, err)
	defer manager.Close()
	require.NotNil(t, manager.(*kafkaTopicManager).cancel)
}

func TestCreateTopicWithTopicDescribeDenied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := &mockAdminClientWithDeniedDescribe{
		MockClusterAdminClient: kafka.NewMockClusterAdminClient(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	defaultTopic := "default-topic"
	manager := newKafkaTopicManager(defaultTopic, changefeedID, adminClient, cfg)

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, defaultTopic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.False(t, adminClient.createTopicCalled)
	require.Equal(t, 2, adminClient.describeCount)

	partitions, ok := manager.topics.Load(defaultTopic)
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}

func TestCreateTopicWithCreateDenied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := &mockAdminClientWithDeniedCreate{
		MockClusterAdminClient: kafka.NewMockClusterAdminClient(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	defaultTopic := "default-topic"
	manager := newKafkaTopicManager(defaultTopic, changefeedID, adminClient, cfg)

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, defaultTopic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.True(t, adminClient.createTopicCalled)
	require.Equal(t, 2, adminClient.describeCount)

	partitions, ok := manager.topics.Load(defaultTopic)
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}
