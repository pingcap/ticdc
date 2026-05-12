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
	"testing"

	"github.com/IBM/sarama"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

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
}
