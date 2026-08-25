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

func topicDetail(topic string, partitionNum int32) kafka.TopicDetail {
	return kafka.TopicDetail{
		Name:          topic,
		NumPartitions: partitionNum,
	}
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangefeedID4Test("test", "test")

	t.Run("existing topic", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetTopicsMeta([]string{kafkaTopicManagerTestTopic}, true).Return(
			map[string]kafka.TopicDetail{
				kafkaTopicManagerTestTopic: topicDetail(kafkaTopicManagerTestTopic, 2),
			}, nil)
		manager := newKafkaTopicManager(
			kafkaTopicManagerTestTopic,
			changefeedID,
			adminClient,
			&kafka.AutoCreateTopicConfig{PartitionNum: 2},
		)

		partitionNum, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), kafkaTopicManagerTestTopic)

		require.NoError(t, err)
		require.Equal(t, int32(2), partitionNum)
	})

	t.Run("create missing topic", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		var createdTopic *kafka.TopicDetail
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).DoAndReturn(
			func([]string, bool) (map[string]kafka.TopicDetail, error) {
				if createdTopic == nil {
					return map[string]kafka.TopicDetail{}, nil
				}
				return map[string]kafka.TopicDetail{
					createdTopic.Name: topicDetail(createdTopic.Name, createdTopic.NumPartitions),
				}, nil
			}).Times(2)
		adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
			func(detail *kafka.TopicDetail) error {
				copy := *detail
				createdTopic = &copy
				return nil
			})
		manager := newKafkaTopicManager(
			kafkaTopicManagerTestTopic,
			changefeedID,
			adminClient,
			&kafka.AutoCreateTopicConfig{
				AutoCreate:        true,
				PartitionNum:      2,
				ReplicationFactor: 1,
				RequiredAcks:      kafka.WaitForLocal,
			},
		)

		partitionNum, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "new-topic")

		require.NoError(t, err)
		require.Equal(t, int32(2), partitionNum)
		require.Equal(t, &kafka.TopicDetail{
			Name:              "new-topic",
			NumPartitions:     2,
			ReplicationFactor: 1,
		}, createdTopic)
		partitionsNum, err := manager.GetPartitionNum(context.Background(), "new-topic")
		require.NoError(t, err)
		require.Equal(t, int32(2), partitionsNum)
	})

	t.Run("auto create disabled", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
		manager := newKafkaTopicManager(
			"new-topic",
			changefeedID,
			adminClient,
			&kafka.AutoCreateTopicConfig{
				AutoCreate:        false,
				PartitionNum:      2,
				ReplicationFactor: 1,
				RequiredAcks:      kafka.WaitForAll,
			},
		)

		_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "new-topic")

		require.ErrorContains(t, err, "`auto-create-topic` is false, and new-topic not found")
	})

	t.Run("create error", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
		adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
		var createdTopic *kafka.TopicDetail
		adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
			func(detail *kafka.TopicDetail) error {
				copy := *detail
				createdTopic = &copy
				return errors.ErrKafkaAdminAPI.GenWithStackByArgs("create-topic", detail.Name)
			})
		manager := newKafkaTopicManager(
			"new-topic",
			changefeedID,
			adminClient,
			&kafka.AutoCreateTopicConfig{
				AutoCreate:        true,
				PartitionNum:      2,
				ReplicationFactor: 4,
			},
		)

		_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "new-topic")

		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.Equal(t, "new-topic", createdTopic.Name)
	})
}

func TestCreateTopicValidatesReplicationFactor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetBrokerConfig(kafka.MinInsyncReplicasConfigName).Return("2", true, nil)
	manager := newKafkaTopicManager(
		"new-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
			RequiredAcks:      kafka.WaitForAll,
		},
	)

	_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "new-topic")

	require.ErrorContains(t, err, "`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker")
}

func TestEnsureTopicExistsWaitsUntilVisible(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	created := false
	postCreateDescribeCount := 0
	var manager *kafkaTopicManager
	adminClient.EXPECT().GetTopicsMeta([]string{"delayed-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"delayed-topic"}, false).DoAndReturn(
		func([]string, bool) (map[string]kafka.TopicDetail, error) {
			if !created {
				return map[string]kafka.TopicDetail{}, nil
			}
			postCreateDescribeCount++
			_, cached := manager.topics.Load("delayed-topic")
			require.False(t, cached)
			switch postCreateDescribeCount {
			case 1:
				return nil, errors.WrapError(
					errors.ErrKafkaAdminAPI,
					sarama.ErrUnknownTopicOrPartition,
					"describe-topic",
					"delayed-topic",
				)
			case 2:
				return map[string]kafka.TopicDetail{}, nil
			}
			return map[string]kafka.TopicDetail{
				"delayed-topic": topicDetail("delayed-topic", 2),
			}, nil
		}).Times(4)
	adminClient.EXPECT().CreateTopic(gomock.Any()).DoAndReturn(
		func(detail *kafka.TopicDetail) error {
			require.Equal(t, &kafka.TopicDetail{
				Name:              "delayed-topic",
				NumPartitions:     2,
				ReplicationFactor: 1,
			}, detail)
			created = true
			return nil
		})

	manager = newKafkaTopicManager(
		"delayed-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
		},
	)

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "delayed-topic")

	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.Equal(t, 3, postCreateDescribeCount)
	cachedPartitionNum, cached := manager.topics.Load("delayed-topic")
	require.True(t, cached)
	require.Equal(t, int32(2), cachedPartitionNum)
}

func TestCreateTopicDoesNotCacheBeforeVisibilityRetryExhausted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"never-visible-topic"}, true).
		Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"never-visible-topic"}, false).
		Return(map[string]kafka.TopicDetail{}, nil).
		Times(7)
	adminClient.EXPECT().CreateTopic(&kafka.TopicDetail{
		Name:              "never-visible-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
	}).Return(nil)
	manager := newKafkaTopicManager(
		"never-visible-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
			RequiredAcks:      kafka.WaitForLocal,
		},
	)

	_, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "never-visible-topic")

	require.ErrorIs(t, err, errors.ErrReachMaxTry)
	require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
	_, cached := manager.topics.Load("never-visible-topic")
	require.False(t, cached)
}

func TestWaitUntilTopicVisibleHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	adminClient.EXPECT().GetTopicsMeta([]string{"cancelled-topic"}, false).DoAndReturn(
		func([]string, bool) (map[string]kafka.TopicDetail, error) {
			cancel()
			return nil, errors.WrapError(
				errors.ErrKafkaAdminAPI,
				sarama.ErrUnknownTopicOrPartition,
				"describe-topic",
				"cancelled-topic",
			)
		})
	manager := newKafkaTopicManager(
		"cancelled-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{PartitionNum: 2},
	)

	err := manager.waitUntilTopicVisible(ctx, "cancelled-topic")

	require.ErrorIs(t, err, context.Canceled)
}

func TestWaitUntilTopicVisibleStopsOnNonRetryableError(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"invalid-topic"}, false).Return(
		nil,
		errors.WrapError(
			errors.ErrKafkaAdminAPI,
			sarama.ErrInvalidTopic,
			"describe-topic",
			"invalid-topic",
		),
	).Times(1)
	manager := newKafkaTopicManager(
		"invalid-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{PartitionNum: 2},
	)

	err := manager.waitUntilTopicVisible(context.Background(), "invalid-topic")

	require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
	require.ErrorIs(t, err, sarama.ErrInvalidTopic)
}

func TestGetTopicManagerStartsBackgroundRefreshAfterTopicReady(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"existing-topic"}, true).Return(
		map[string]kafka.TopicDetail{
			"existing-topic": topicDetail("existing-topic", 2),
		}, nil)

	manager, err := GetTopicManagerAndTryCreateTopic(
		t.Context(),
		common.NewChangefeedID4Test("test", "test"),
		"existing-topic",
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
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"default-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"default-topic"}, false).Return(
		nil, errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs("describe-topic", "default-topic"))
	manager := newKafkaTopicManager(
		"default-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
		},
	)

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "default-topic")

	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitions, ok := manager.topics.Load("default-topic")
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}

func TestCreateTopicWithCreateDenied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta([]string{"default-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"default-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().CreateTopic(&kafka.TopicDetail{
		Name:              "default-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
	}).Return(errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs("create-topic", "default-topic"))
	manager := newKafkaTopicManager(
		"default-topic",
		common.NewChangefeedID4Test("test", "test"),
		adminClient,
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
		},
	)

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(context.Background(), "default-topic")

	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitions, ok := manager.topics.Load("default-topic")
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}
