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
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
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
			name: "return unknown topic",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.UnknownTopicOrPartition},
			}},
			expectedError: errors.ErrKafkaAdminAPI,
			expectedCause: kerr.UnknownTopicOrPartition,
		},
		{
			name:          "return missing topic",
			metadata:      kadm.Metadata{Topics: kadm.TopicDetails{}},
			expectedError: errors.ErrKafkaAdminAPI,
			expectedCause: kerr.UnknownTopicOrPartition,
		},
		{
			name: "do not ignore authorization failure",
			metadata: kadm.Metadata{Topics: kadm.TopicDetails{
				topic: {Topic: topic, Err: kerr.TopicAuthorizationFailed},
			}},
			ignoreTopicError: true,
			expectedError:    errors.ErrKafkaAdminAPI,
			expectedCause:    kerr.TopicAuthorizationFailed,
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

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	cluster := kfake.MustCluster(kfake.NumBrokers(1))
	defer cluster.Close()

	options := NewOptions()
	options.BrokerEndpoints = cluster.ListenAddrs()
	admin, err := newAdmin(
		context.Background(),
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "test"),
		options,
		nil,
	)
	require.NoError(t, err)
	defer admin.Close()

	const topic = "test-topic"
	err = admin.CreateTopic(TopicDetail{
		Name:              topic,
		NumPartitions:     3,
		ReplicationFactor: 1,
	})
	require.NoError(t, err)

	topics, err := admin.GetTopicsMeta([]string{topic}, false)
	require.NoError(t, err)
	require.Equal(t, int32(3), topics[topic].NumPartitions)
}
