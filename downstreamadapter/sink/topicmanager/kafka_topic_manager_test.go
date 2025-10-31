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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

// mockAdminClientForHeartbeat is used to count the calls to Heartbeat.
type mockAdminClientForHeartbeat struct {
	kafka.ClusterAdminClientMockImpl
	heartbeatCount int
	mu             sync.Mutex
}

func (m *mockAdminClientForHeartbeat) Heartbeat() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
}

func (m *mockAdminClientForHeartbeat) GetHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

func TestKafkaTopicManagerHeartbeat(t *testing.T) {
	t.Parallel()

	adminClient := &mockAdminClientForHeartbeat{}
	cfg := &kafka.AutoCreateTopicConfig{AutoCreate: false}
	changefeedID := common.NewChangefeedID4Test("test", "test")
	ctx, cancel := context.WithCancel(context.Background())

	heartbeatInterval := 5 * time.Second
	manager := newKafkaTopicManager(ctx, "topic", changefeedID, adminClient, cfg)

	// Ensure the context is canceled at the end of the test.
	defer cancel()

	// Wait for a sufficient amount of time to ensure the heartbeat ticker triggers several times.
	// Waiting for 11 seconds to allow for at least two heartbeats.
	// Use Eventually to avoid test flakiness.
	require.Eventually(t, func() bool {
		return adminClient.GetHeartbeatCount() >= 2
	}, 11*time.Second, 150*time.Millisecond, "Heartbeat should be called periodically")

	// Verify that closing the manager stops the heartbeat.
	countBeforeClose := adminClient.GetHeartbeatCount()
	manager.Close()
	// Wait for a short period to ensure no new heartbeats occur.
	time.Sleep(heartbeatInterval * 2)
	require.Equal(t, countBeforeClose, adminClient.GetHeartbeatCount(), "Heartbeat should stop after manager is closed")
}

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
	manager := newKafkaTopicManager(ctx, kafka.DefaultMockTopicName, changefeedID, adminClient, cfg)
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
	require.Regexp(
		t,
		"kafka create topic failed: kafka server: Replication-factor is invalid",
		err,
	)
}

func TestCreateTopicWithDelay(t *testing.T) {
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
