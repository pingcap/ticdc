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
	"io"
	"testing"

	"github.com/IBM/sarama"
<<<<<<< HEAD
=======
	"github.com/golang/mock/gomock"
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD
type testSaramaClient struct {
	closed bool
}

func (c *testSaramaClient) Brokers() []*sarama.Broker {
	return nil
}

func (c *testSaramaClient) Partitions(string) ([]int32, error) {
	return nil, nil
}

func (c *testSaramaClient) Close() error {
	c.closed = true
	return nil
}

type testSaramaClusterAdmin struct {
	closed      bool
	closeClient *testSaramaClient
}

func (a *testSaramaClusterAdmin) DescribeCluster() ([]*sarama.Broker, int32, error) {
	return nil, 0, nil
}

func (a *testSaramaClusterAdmin) DescribeConfig(sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return nil, nil
}

func (a *testSaramaClusterAdmin) DescribeTopics([]string) ([]*sarama.TopicMetadata, error) {
	return nil, nil
}

func (a *testSaramaClusterAdmin) CreateTopic(string, *sarama.TopicDetail, bool) error {
	return nil
}

func (a *testSaramaClusterAdmin) Close() error {
	a.closed = true
	if a.closeClient != nil {
		return a.closeClient.Close()
	}
	return nil
}

func TestSaramaAdminClientCloseDelegatesClientCleanupToAdmin(t *testing.T) {
	client := &testSaramaClient{}
	admin := &testSaramaClusterAdmin{closeClient: client}
	a := &saramaAdminClient{
		changefeed: common.NewChangeFeedIDWithName("test", "default"),
		client:     client,
		admin:      admin,
	}
	a.Close()
	require.True(t, admin.closed)
	require.True(t, client.closed)
}

func TestSaramaAdminClientCloseFallsBackToClientWhenAdminIsNil(t *testing.T) {
	client := &testSaramaClient{}
	a := &saramaAdminClient{
		changefeed: common.NewChangeFeedIDWithName("test", "default"),
		client:     client,
	}
	require.NotPanics(t, func() { a.Close() })
	require.True(t, client.closed)
=======
func TestGetBrokerConfig(t *testing.T) {
	t.Parallel()

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

func TestAdminClientClose(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*gomock.Controller) *saramaAdminClient
	}{
		{
			name: "uses admin close",
			setup: func(ctrl *gomock.Controller) *saramaAdminClient {
				client := NewMocksaramaClient(ctrl)
				admin := NewMocksaramaClusterAdmin(ctrl)
				admin.EXPECT().Close().Return(nil)
				client.EXPECT().Close().Times(0)
				return &saramaAdminClient{
					changefeed: common.NewChangeFeedIDWithName("test", "default"),
					client:     client,
					admin:      admin,
				}
			},
		},
		{
			name: "falls back to client when admin is nil",
			setup: func(ctrl *gomock.Controller) *saramaAdminClient {
				client := NewMocksaramaClient(ctrl)
				client.EXPECT().Close().Return(nil)
				return &saramaAdminClient{
					changefeed: common.NewChangeFeedIDWithName("test", "default"),
					client:     client,
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			adminClient := test.setup(ctrl)

			require.NotPanics(t, func() { adminClient.Close() })
		})
	}
>>>>>>> fa340f118 (kafka: unify sink errors and replace failpoint tests (#5786))
}
