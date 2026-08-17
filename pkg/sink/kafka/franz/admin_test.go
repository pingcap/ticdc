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

package franz

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestTopicDetailsFromMetadata(t *testing.T) {
	t.Parallel()

	const topic = "topic"
	testCases := []struct {
		name             string
		metadata         kadm.Metadata
		ignoreTopicError bool
		expected         map[string]TopicDetail
		expectedError    error
		expectedCause    error
	}{
		{
			name: "success",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Partitions: kadm.PartitionDetails{0: {}, 1: {}}},
			}},
			expected: map[string]TopicDetail{
				topic: {Name: topic, NumPartitions: 2},
			},
		},
		{
			name: "ignore unknown topic",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.UnknownTopicOrPartition},
			}},
			ignoreTopicError: true,
			expected:         map[string]TopicDetail{},
		},
		{
			name: "strict unknown topic",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.UnknownTopicOrPartition},
			}},
			expectedError: errors.ErrKafkaAdminAPI,
			expectedCause: kerr.UnknownTopicOrPartition,
		},
		{
			name:          "strict missing topic",
			metadata:      kadm.Metadata{Topics: kadm.TopicDetails{}},
			expectedError: errors.ErrKafkaAdminAPI,
			expectedCause: kerr.UnknownTopicOrPartition,
		},
		{
			name: "return authorization failure",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.TopicAuthorizationFailed},
			}},
			expectedError: errors.ErrKafkaAuthorizationFailed,
			expectedCause: kerr.TopicAuthorizationFailed,
		},
		{
			name: "do not ignore authorization failure",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.TopicAuthorizationFailed},
			}},
			ignoreTopicError: true,
			expectedError:    errors.ErrKafkaAuthorizationFailed,
			expectedCause:    kerr.TopicAuthorizationFailed,
		},
		{
			name: "do not ignore general failure",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.InvalidTopicException},
			}},
			ignoreTopicError: true,
			expectedError:    errors.ErrKafkaAdminAPI,
			expectedCause:    kerr.InvalidTopicException,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			actual, err := topicDetailsFromMetadata(tc.metadata, []string{topic}, tc.ignoreTopicError)
			if tc.expectedError != nil {
				require.ErrorIs(t, err, tc.expectedError)
				if tc.expectedCause != nil {
					require.ErrorIs(t, err, tc.expectedCause)
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestIsAuthorizationFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "TiCDC authorization error",
			err:      errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs("describe-topic", "test-topic"),
			expected: true,
		},
		{name: "topic authorization error", err: kerr.TopicAuthorizationFailed, expected: true},
		{name: "cluster authorization error", err: kerr.ClusterAuthorizationFailed, expected: true},
		{name: "general error", err: kerr.InvalidTopicException},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, isAuthorizationFailed(test.err))
		})
	}
}

func TestAdminOperations(t *testing.T) {
	const existingTopic = "existing-topic"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, existingTopic))
	defer cluster.Close()

	admin, err := NewAdmin(
		context.Background(),
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
		testConfig(cluster.ListenAddrs()),
	)
	require.NoError(t, err)
	defer admin.Close()

	require.Len(t, admin.GetAllBrokers(), 1)

	value, found, err := admin.GetBrokerConfig("message.max.bytes")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "1048588", value)

	_, found, err = admin.GetBrokerConfig("missing")
	require.NoError(t, err)
	require.False(t, found)

	value, found, err = admin.GetTopicConfig(existingTopic, "max.message.bytes")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "1048588", value)

	_, found, err = admin.GetTopicConfig(existingTopic, "missing")
	require.NoError(t, err)
	require.False(t, found)

	partitions, err := admin.GetTopicsPartitionsNum([]string{existingTopic})
	require.NoError(t, err)
	require.Equal(t, map[string]int32{existingTopic: 3}, partitions)

	const topic = "test-topic"
	topics, err := admin.GetTopicsMeta([]string{topic}, true)
	require.NoError(t, err)
	require.Empty(t, topics)

	err = admin.CreateTopic(&TopicDetail{
		Name:              topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		topics, err = admin.GetTopicsMeta([]string{topic}, false)
		return err == nil && topics[topic].NumPartitions == 3
	}, time.Second, 20*time.Millisecond)

	require.NoError(t, admin.CreateTopic(&TopicDetail{Name: topic, NumPartitions: 3, ReplicationFactor: 1}))
}

func TestCreateTopicErrors(t *testing.T) {
	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	defer cluster.Close()

	admin, err := NewAdmin(
		context.Background(),
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "create-errors"),
		testConfig(cluster.ListenAddrs()),
	)
	require.NoError(t, err)
	defer admin.Close()

	detail := &TopicDetail{Name: "topic", NumPartitions: 1, ReplicationFactor: 1}

	cluster.ControlKey(int16(kmsg.CreateTopics), func(req kmsg.Request) (kmsg.Response, error, bool) {
		return req.ResponseKind(), nil, true
	})
	require.ErrorIs(t, admin.CreateTopic(detail), errors.ErrKafkaAdminAPI)

	for _, test := range []struct {
		name     string
		code     int16
		expected error
	}{
		{
			name:     "invalid replication factor",
			code:     kerr.InvalidReplicationFactor.Code,
			expected: errors.ErrKafkaInvalidConfig,
		},
		{
			name:     "authorization",
			code:     kerr.TopicAuthorizationFailed.Code,
			expected: errors.ErrKafkaAuthorizationFailed,
		},
		{
			name:     "admin API",
			code:     kerr.InvalidTopicException.Code,
			expected: errors.ErrKafkaAdminAPI,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cluster.ControlKey(int16(kmsg.CreateTopics), func(req kmsg.Request) (kmsg.Response, error, bool) {
				response := req.ResponseKind().(*kmsg.CreateTopicsResponse)
				topic := kmsg.NewCreateTopicsResponseTopic()
				topic.Topic, topic.ErrorCode = detail.Name, test.code
				response.Topics = append(response.Topics, topic)

				return response, nil, true
			})

			require.ErrorIs(t, admin.CreateTopic(detail), test.expected)
		})
	}
}
