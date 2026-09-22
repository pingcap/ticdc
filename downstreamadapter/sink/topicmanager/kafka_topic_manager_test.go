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
	"github.com/twmb/franz-go/pkg/kerr"
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
<<<<<<< HEAD
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
=======

	t.Run("existing topic", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{kafkaTopicManagerTestTopic}, false).Return(
			map[string]kafka.TopicDetail{
				kafkaTopicManagerTestTopic: {Name: kafkaTopicManagerTestTopic, NumPartitions: 2},
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
		postCreateDescribeCount := 0
		var manager *kafkaTopicManager
		adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"new-topic"}, false).DoAndReturn(
			func(context.Context, []string, bool) (map[string]kafka.TopicDetail, error) {
				if createdTopic == nil {
					return nil, errors.WrapError(errors.ErrKafkaAdminAPI, sarama.ErrUnknownTopicOrPartition, "describe-topic", "new-topic")
				}
				postCreateDescribeCount++
				_, cached := manager.topics.Load("new-topic")
				require.False(t, cached)
				if postCreateDescribeCount == 1 {
					return nil, errors.WrapError(errors.ErrKafkaAdminAPI, io.EOF, "describe-topic", "new-topic")
				}
				return map[string]kafka.TopicDetail{
					createdTopic.Name: {Name: createdTopic.Name, NumPartitions: createdTopic.NumPartitions},
				}, nil
			}).Times(3)
		adminClient.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, detail *kafka.TopicDetail) error {
				copy := *detail
				createdTopic = &copy
				return nil
			})
		manager = newKafkaTopicManager(
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
		require.Equal(t, 2, postCreateDescribeCount)
		partitionsNum, err := manager.GetPartitionNum(context.Background(), "new-topic")
		require.NoError(t, err)
		require.Equal(t, int32(2), partitionsNum)
	})

	t.Run("auto create disabled", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		adminClient := kafka.NewMockAdminClient(ctrl)
		adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
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
		adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
		var createdTopic *kafka.TopicDetail
		adminClient.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, detail *kafka.TopicDetail) error {
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
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))

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
<<<<<<< HEAD
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

=======
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"new-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetBrokerConfig(gomock.Any(), kafka.MinInsyncReplicasConfigName).Return("2", true, nil)
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))
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

<<<<<<< HEAD
	ctrl := gomock.NewController(t)
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	created := false
	postCreateDescribeCount := 0
	adminClient.EXPECT().GetTopicsMeta([]string{"delayed-topic"}, true).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().GetTopicsMeta([]string{"delayed-topic"}, false).DoAndReturn(
		func([]string, bool) (map[string]kafka.TopicDetail, error) {
			if !created {
				return map[string]kafka.TopicDetail{}, nil
			}
			postCreateDescribeCount++
			if postCreateDescribeCount == 1 {
				return map[string]kafka.TopicDetail{}, nil
			}
			return map[string]kafka.TopicDetail{
				"delayed-topic": {
					Name:          "delayed-topic",
					NumPartitions: 2,
				},
			}, nil
		}).Times(3)
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

	err := EnsureTopic(
		context.Background(),
		common.NewChangefeedID4Test("test", "test"),
		"delayed-topic",
		&kafka.AutoCreateTopicConfig{
			AutoCreate:        true,
			PartitionNum:      2,
			ReplicationFactor: 1,
		},
		adminClient,
	)

	require.NoError(t, err)
	require.Equal(t, 2, postCreateDescribeCount)
=======
	for _, test := range []struct {
		name  string
		cause error
	}{
		{name: "sarama", cause: sarama.ErrInvalidTopic},
		{name: "franz-go", cause: kerr.InvalidTopicException},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			adminClient := kafka.NewMockAdminClient(ctrl)
			adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"invalid-topic"}, false).Return(
				nil,
				errors.WrapError(errors.ErrKafkaAdminAPI, test.cause, "describe-topic", "invalid-topic"),
			).Times(1)
			manager := newKafkaTopicManager(
				"invalid-topic",
				common.NewChangefeedID4Test("test", "test"),
				adminClient,
				&kafka.AutoCreateTopicConfig{PartitionNum: 2},
			)

			err := manager.waitUntilTopicVisible(context.Background(), "invalid-topic")

			require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
			require.ErrorIs(t, err, test.cause)
		})
	}
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))
}

func TestGetTopicManagerStartsBackgroundRefreshAfterTopicReady(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
<<<<<<< HEAD
	adminClient := kafka.NewMockClusterAdminClient(ctrl)
	topic := "existing-topic"
	adminClient.EXPECT().GetTopicsMeta([]string{topic}, true).Return(
=======
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"existing-topic"}, false).Return(
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))
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
<<<<<<< HEAD
	adminClient := &mockAdminClientWithDeniedDescribe{
		MockClusterAdminClient: kafka.NewMockClusterAdminClient(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}
=======
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"default-topic"}, false).Return(
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
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))

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
<<<<<<< HEAD
	adminClient := &mockAdminClientWithDeniedCreate{
		MockClusterAdminClient: kafka.NewMockClusterAdminClient(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
=======
	adminClient := kafka.NewMockAdminClient(ctrl)
	adminClient.EXPECT().GetTopicsMeta(gomock.Any(), []string{"default-topic"}, false).Return(map[string]kafka.TopicDetail{}, nil)
	adminClient.EXPECT().CreateTopic(gomock.Any(), &kafka.TopicDetail{
		Name:              "default-topic",
		NumPartitions:     2,
>>>>>>> 884f10974 (kafka: introduce franz-go as the kafka client (#4167))
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
