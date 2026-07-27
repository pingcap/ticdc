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
		RequiredAcks:      kafka.WaitForAll,
	}

	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx := context.Background()
	manager := newKafkaTopicManager(ctx, kafka.DefaultMockTopicName, changefeedID, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafka.DefaultMockTopicName)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	cfg.RequiredAcks = kafka.WaitForLocal
	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
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
}

<<<<<<< HEAD
func TestCreateTopicWithDelay(t *testing.T) {
=======
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
			Return("2", nil),
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
>>>>>>> 0d4929739 (kafka: verify replication-factor when need to create the topic (#5715))
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
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
