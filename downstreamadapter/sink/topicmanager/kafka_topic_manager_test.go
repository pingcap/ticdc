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

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

const kafkaTopicManagerTestTopic = "mock_topic"

type mockAdminWithDeniedDescribe struct {
	*kafka.MockAdmin
	createTopicCalled bool
	describeCount     int
}

func (m *mockAdminWithDeniedDescribe) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]kafka.TopicDetail, error) {
	m.describeCount++
	return nil, errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs()
}

func (m *mockAdminWithDeniedDescribe) CreateTopic(
	detail kafka.TopicDetail,
	validateOnly bool,
) error {
	m.createTopicCalled = true
	return nil
}

type mockAdminWithDeniedCreate struct {
	*kafka.MockAdmin
	createTopicCalled bool
	describeCount     int
}

func (m *mockAdminWithDeniedCreate) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]kafka.TopicDetail, error) {
	m.describeCount++
	return map[string]kafka.TopicDetail{}, nil
}

func (m *mockAdminWithDeniedCreate) CreateTopic(
	detail kafka.TopicDetail,
	validateOnly bool,
) error {
	m.createTopicCalled = true
	return errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs()
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	admin := kafka.NewMockAdmin(ctrl)
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
		RequiredAcks:      kafka.WaitForAll,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	var gotNewTopicDetail kafka.TopicDetail
	var gotNewTopicValidateOnly bool
	var gotFailedTopicDetail kafka.TopicDetail
	var gotFailedTopicValidateOnly bool
	gomock.InOrder(
		admin.EXPECT().GetTopicsMeta([]string{kafkaTopicManagerTestTopic}, true).Return(
			map[string]kafka.TopicDetail{
				kafkaTopicManagerTestTopic: {
					Name:          kafkaTopicManagerTestTopic,
					NumPartitions: 2,
				},
			}, nil),
		admin.EXPECT().GetTopicsMeta([]string{"new-topic"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		admin.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail kafka.TopicDetail, validateOnly bool) error {
				gotNewTopicDetail = detail
				gotNewTopicValidateOnly = validateOnly
				return nil
			}),
		admin.EXPECT().GetTopicsMeta([]string{"new-topic"}, false).Return(
			map[string]kafka.TopicDetail{
				"new-topic": {
					Name:          "new-topic",
					NumPartitions: 2,
				},
			}, nil),
		admin.EXPECT().GetTopicsMeta([]string{"new-topic2"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		admin.EXPECT().GetTopicsMeta([]string{"new-topic-failed"}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		admin.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail kafka.TopicDetail, validateOnly bool) error {
				gotFailedTopicDetail = detail
				gotFailedTopicValidateOnly = validateOnly
				return errors.New("invalid replication factor")
			}),
	)

	manager := newKafkaTopicManager(ctx, kafkaTopicManagerTestTopic, changefeedID, admin, cfg)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafkaTopicManagerTestTopic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	cfg.RequiredAcks = kafka.WaitForLocal
	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.Equal(t, kafka.TopicDetail{
		Name:              "new-topic",
		NumPartitions:     2,
		ReplicationFactor: 1,
	}, gotNewTopicDetail)
	require.False(t, gotNewTopicValidateOnly)
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
	manager = newKafkaTopicManager(ctx, "new-topic2", changefeedID, admin, cfg)
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
	manager = newKafkaTopicManager(ctx, topic, changefeedID, admin, cfg)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, topic)
	require.Regexp(
		t,
		"kafka create topic failed: invalid replication factor",
		err,
	)
	require.Equal(t, "new-topic-failed", gotFailedTopicDetail.Name)
	require.False(t, gotFailedTopicValidateOnly)
}

func TestCreateTopicValidatesReplicationFactor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	admin := kafka.NewMockAdmin(ctrl)
	topic := "new-topic"
	gomock.InOrder(
		admin.EXPECT().GetTopicsMeta([]string{topic}, true).
			Return(map[string]kafka.TopicDetail{}, nil),
		admin.EXPECT().GetBrokerConfig(kafka.MinInsyncReplicasConfigName).
			Return("2", nil),
	)

	manager := newKafkaTopicManager(
		context.Background(),
		topic,
		common.NewChangefeedID4Test("test", "test"),
		admin,
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
	admin := kafka.NewMockAdmin(ctrl)
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	topic := "delayed-topic"
	gomock.InOrder(
		admin.EXPECT().GetTopicsMeta([]string{topic}, true).Return(
			map[string]kafka.TopicDetail{}, nil),
		admin.EXPECT().CreateTopic(gomock.Any(), false).DoAndReturn(
			func(detail kafka.TopicDetail, validateOnly bool) error {
				require.Equal(t, kafka.TopicDetail{
					Name:              topic,
					NumPartitions:     2,
					ReplicationFactor: 1,
				}, detail)
				require.False(t, validateOnly)
				return nil
			}),
		admin.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			nil, errors.ErrKafkaTopicNotFound.GenWithStackByArgs(topic)),
		admin.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			nil, errors.ErrKafkaTopicNotFound.GenWithStackByArgs(topic)),
		admin.EXPECT().GetTopicsMeta([]string{topic}, false).Return(
			map[string]kafka.TopicDetail{
				topic: {
					Name:          topic,
					NumPartitions: 2,
				},
			}, nil),
	)

	ctx := context.Background()
	changefeedID := common.NewChangefeedID4Test("test", "test")
	manager := newKafkaTopicManager(ctx, topic, changefeedID, admin, cfg)
	defer manager.Close()

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, topic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
}

func TestCreateTopicWithTopicDescribeDenied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	admin := &mockAdminWithDeniedDescribe{
		MockAdmin: kafka.NewMockAdmin(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	manager := newKafkaTopicManager(ctx, "precreated-topic", changefeedID, admin, cfg)
	defer manager.Close()

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, "precreated-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.False(t, admin.createTopicCalled)
	require.Equal(t, 1, admin.describeCount)

	partitions, ok := manager.topics.Load("precreated-topic")
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}

func TestCreateTopicWithCreateDenied(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	admin := &mockAdminWithDeniedCreate{
		MockAdmin: kafka.NewMockAdmin(ctrl),
	}
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	manager := newKafkaTopicManager(ctx, "precreated-topic", changefeedID, admin, cfg)
	defer manager.Close()

	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, "precreated-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	require.True(t, admin.createTopicCalled)
	require.Equal(t, 1, admin.describeCount)

	partitions, ok := manager.topics.Load("precreated-topic")
	require.True(t, ok)
	require.Equal(t, int32(2), partitions)
}
