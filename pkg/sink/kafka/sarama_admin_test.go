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
	"io"
	"testing"

	"github.com/IBM/sarama"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestGetBrokerConfig(t *testing.T) {
	t.Parallel()

	t.Run("found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeCluster().Return(nil, int32(1), nil)
		admin.EXPECT().DescribeConfig(sarama.ConfigResource{
			Type:        sarama.BrokerResource,
			Name:        "1",
			ConfigNames: []string{"message.max.bytes"},
		}).Return([]sarama.ConfigEntry{
			{Name: "unrelated", Value: "value"},
			{Name: "message.max.bytes", Value: "1048576"},
		}, nil)

		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}
		value, found, err := client.GetBrokerConfig("message.max.bytes")

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "1048576", value)
	})

	t.Run("not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeCluster().Return(nil, int32(1), nil)
		admin.EXPECT().DescribeConfig(gomock.Any()).Return([]sarama.ConfigEntry{}, nil)

		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}
		value, found, err := client.GetBrokerConfig("missing")

		require.NoError(t, err)
		require.False(t, found)
		require.Empty(t, value)
	})

	t.Run("admin error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		cause := io.ErrUnexpectedEOF
		admin.EXPECT().DescribeCluster().Return(nil, int32(0), cause)

		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}
		_, _, err := client.GetBrokerConfig("missing")

		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, cause)
	})
}

func TestGetTopicConfig(t *testing.T) {
	t.Parallel()

	t.Run("found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeConfig(sarama.ConfigResource{
			Type:        sarama.TopicResource,
			Name:        "test-topic",
			ConfigNames: []string{"max.message.bytes"},
		}).Return([]sarama.ConfigEntry{
			{Name: "max.message.bytes", Value: "1048576"},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		value, found, err := client.GetTopicConfig("test-topic", "max.message.bytes")

		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, "1048576", value)
	})

	t.Run("not found", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeConfig(gomock.Any()).Return([]sarama.ConfigEntry{}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		value, found, err := client.GetTopicConfig("test-topic", "missing")

		require.NoError(t, err)
		require.False(t, found)
		require.Empty(t, value)
	})

	t.Run("admin error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeConfig(gomock.Any()).Return(nil, context.DeadlineExceeded)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		_, _, err := client.GetTopicConfig("test-topic", "missing")

		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.False(t, IsAuthorizationFailed(err))
	})
}

func TestGetTopicsMeta(t *testing.T) {
	t.Parallel()

	t.Run("returns unknown topic error when topic errors are not ignored", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"valid-topic", "missing-topic"}).Return([]*sarama.TopicMetadata{
			{
				Name:       "valid-topic",
				Partitions: []*sarama.PartitionMetadata{{}, {}},
			},
			{
				Name: "missing-topic",
				Err:  sarama.ErrUnknownTopicOrPartition,
			},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		topics, err := client.GetTopicsMeta([]string{"valid-topic", "missing-topic"}, false)

		require.Nil(t, topics)
		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, sarama.ErrUnknownTopicOrPartition)
	})

	t.Run("ignores unknown topic error and returns valid topics", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"valid-topic", "missing-topic"}).Return([]*sarama.TopicMetadata{
			{
				Name:       "valid-topic",
				Partitions: []*sarama.PartitionMetadata{{}, {}},
			},
			{
				Name: "missing-topic",
				Err:  sarama.ErrUnknownTopicOrPartition,
			},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		topics, err := client.GetTopicsMeta([]string{"valid-topic", "missing-topic"}, true)

		require.NoError(t, err)
		require.Equal(t, map[string]TopicDetail{
			"valid-topic": {
				Name:          "valid-topic",
				NumPartitions: 2,
			},
		}, topics)
	})

	t.Run("missing response", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"missing-topic"}).Return(nil, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		topics, err := client.GetTopicsMeta([]string{"missing-topic"}, false)

		require.NoError(t, err)
		require.Empty(t, topics)
	})

	t.Run("topic error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"test-topic"}).Return([]*sarama.TopicMetadata{
			{Name: "test-topic", Err: sarama.ErrInvalidTopic},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		_, err := client.GetTopicsMeta([]string{"test-topic"}, false)

		require.ErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, sarama.ErrInvalidTopic)
		require.False(t, IsAuthorizationFailed(err))
	})

	t.Run("topic authorization error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"test-topic"}).Return([]*sarama.TopicMetadata{
			{Name: "test-topic", Err: sarama.ErrTopicAuthorizationFailed},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		_, err := client.GetTopicsMeta([]string{"test-topic"}, false)

		require.ErrorIs(t, err, errors.ErrKafkaAuthorizationFailed)
		require.NotErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, sarama.ErrTopicAuthorizationFailed)
		require.True(t, IsAuthorizationFailed(err))
		code, ok := errors.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, errors.ErrKafkaAuthorizationFailed.RFCCode(), code)
	})

	t.Run("cluster authorization error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"test-topic"}).Return(nil, sarama.ErrClusterAuthorizationFailed)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		_, err := client.GetTopicsMeta([]string{"test-topic"}, false)

		require.ErrorIs(t, err, errors.ErrKafkaAuthorizationFailed)
		require.NotErrorIs(t, err, errors.ErrKafkaAdminAPI)
		require.ErrorIs(t, err, sarama.ErrClusterAuthorizationFailed)
		require.True(t, IsAuthorizationFailed(err))
	})

	t.Run("ignores non-unknown topic error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		admin := NewMocksaramaClusterAdmin(ctrl)
		admin.EXPECT().DescribeTopics([]string{"test-topic"}).Return([]*sarama.TopicMetadata{
			{Name: "test-topic", Err: sarama.ErrInvalidTopic},
		}, nil)
		client := &saramaAdminClient{
			changefeed: common.NewChangeFeedIDWithName("test", "default"),
			admin:      admin,
		}

		topics, err := client.GetTopicsMeta([]string{"test-topic"}, true)

		require.NoError(t, err)
		require.Empty(t, topics)
	})
}

func TestIsAuthorizationFailed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{name: "TiCDC authorization error", err: errors.ErrKafkaAuthorizationFailed.GenWithStackByArgs("describe-topic", "test-topic"), expected: true},
		{name: "topic authorization error", err: sarama.ErrTopicAuthorizationFailed, expected: true},
		{name: "cluster authorization error", err: sarama.ErrClusterAuthorizationFailed, expected: true},
		{name: "general error", err: sarama.ErrInvalidTopic},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.expected, IsAuthorizationFailed(test.err))
		})
	}
}

func TestIsUnretryableTopicMetadataError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		unretryable bool
	}{
		{name: "unknown topic", err: sarama.ErrUnknownTopicOrPartition},
		{name: "leader unavailable", err: sarama.ErrLeaderNotAvailable},
		{name: "request timeout", err: sarama.ErrRequestTimedOut},
		{name: "network exception", err: sarama.ErrNetworkException},
		{name: "controller changed", err: sarama.ErrNotController},
		{name: "no broker available", err: sarama.ErrOutOfBrokers},
		{name: "EOF", err: io.EOF},
		{name: "unknown broker error", err: sarama.ErrUnknown},
		{
			name: "wrapped unknown topic",
			err: errors.WrapError(
				errors.ErrKafkaAdminAPI,
				sarama.ErrUnknownTopicOrPartition,
				"describe-topic",
				"test-topic",
			),
		},
		{name: "context cancellation", err: context.Canceled},
		{name: "TiCDC invalid config", err: errors.ErrKafkaInvalidConfig.GenWithStack("invalid config"), unretryable: true},
		{name: "topic authorization failure", err: sarama.ErrTopicAuthorizationFailed, unretryable: true},
		{name: "cluster authorization failure", err: sarama.ErrClusterAuthorizationFailed, unretryable: true},
		{name: "invalid topic", err: sarama.ErrInvalidTopic, unretryable: true},
		{name: "invalid config", err: sarama.ErrInvalidConfig, unretryable: true},
		{name: "SASL authentication failure", err: sarama.ErrSASLAuthenticationFailed, unretryable: true},
		{name: "unsupported SASL mechanism", err: sarama.ErrUnsupportedSASLMechanism, unretryable: true},
		{name: "illegal SASL state", err: sarama.ErrIllegalSASLState, unretryable: true},
		{name: "unsupported version", err: sarama.ErrUnsupportedVersion, unretryable: true},
		{name: "invalid request", err: sarama.ErrInvalidRequest, unretryable: true},
		{name: "client configuration error", err: sarama.ConfigurationError("invalid client config"), unretryable: true},
		{
			name: "wrapped invalid topic",
			err: errors.WrapError(
				errors.ErrKafkaAdminAPI,
				sarama.ErrInvalidTopic,
				"describe-topic",
				"test-topic",
			),
			unretryable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.unretryable, IsUnretryableTopicMetadataError(test.err))
		})
	}
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		adminErr      error
		expectedErr   error
		authorization bool
	}{
		{name: "success"},
		{name: "topic already exists", adminErr: sarama.ErrTopicAlreadyExists},
		{name: "authorization error", adminErr: sarama.ErrClusterAuthorizationFailed, expectedErr: errors.ErrKafkaAuthorizationFailed, authorization: true},
		{name: "general error", adminErr: sarama.ErrInvalidReplicationFactor, expectedErr: errors.ErrKafkaAdminAPI},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			admin := NewMocksaramaClusterAdmin(ctrl)
			admin.EXPECT().CreateTopic("test-topic", &sarama.TopicDetail{
				NumPartitions:     3,
				ReplicationFactor: 2,
			}, false).Return(test.adminErr)
			client := &saramaAdminClient{
				changefeed: common.NewChangeFeedIDWithName("test", "default"),
				admin:      admin,
			}

			err := client.CreateTopic(&TopicDetail{
				Name:              "test-topic",
				NumPartitions:     3,
				ReplicationFactor: 2,
			})

			if test.expectedErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, test.expectedErr)
			require.ErrorIs(t, err, test.adminErr)
			if test.authorization {
				require.NotErrorIs(t, err, errors.ErrKafkaAdminAPI)
			}
		})
	}
}

func TestAdminClientClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	admin := NewMocksaramaClusterAdmin(ctrl)
	admin.EXPECT().Close().Return(nil)
	client := &saramaAdminClient{
		changefeed: common.NewChangeFeedIDWithName("test", "default"),
		admin:      admin,
	}

	require.NotPanics(t, func() { client.Close() })
}
