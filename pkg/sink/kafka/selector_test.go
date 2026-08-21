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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kafka

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestFactorySelection(t *testing.T) {
	const topic = "factory-selection"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "factory-selection")
	for _, test := range []struct {
		client   string
		expected Factory
	}{
		{client: KafkaClientFranz, expected: &franzFactoryAdapter{}},
		{client: KafkaClientSarama, expected: &saramaFactory{}},
	} {
		t.Run(test.client, func(t *testing.T) {
			options := NewOptions()
			options.Client = test.client
			options.ClientID = "ticdc-test"
			options.BrokerEndpoints = cluster.ListenAddrs()
			options.Topic = topic

			factory, err := NewFactory(context.Background(), options, changefeedID)
			require.NoError(t, err)
			require.IsType(t, test.expected, factory)

			CleanupFactoryMetrics(factory)
		})
	}
}

func TestFranzIgnoresConfiguredKafkaVersion(t *testing.T) {
	const topic = "version-negotiation"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	options := NewOptions()
	options.ClientID = "ticdc-test"
	options.BrokerEndpoints = cluster.ListenAddrs()
	options.Topic = topic
	options.Version = "invalid"
	options.IsAssignedVersion = true

	factory, err := NewFactory(
		context.Background(),
		options,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "version-negotiation"),
	)
	require.NoError(t, err)
	require.IsType(t, &franzFactoryAdapter{}, factory)
	CleanupFactoryMetrics(factory)
}

func TestFranzAndSaramaFactoriesAreIndependent(t *testing.T) {
	const topic = "factory-independence"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	franzOptions := NewOptions()
	franzOptions.ClientID = "ticdc-franz-test"
	franzOptions.BrokerEndpoints = cluster.ListenAddrs()
	franzOptions.Topic = topic

	franzFactory, err := NewFactory(
		context.Background(),
		franzOptions,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "franz"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { CleanupFactoryMetrics(franzFactory) })

	saramaOptions := NewOptions()
	saramaOptions.Client = KafkaClientSarama
	saramaOptions.ClientID = "ticdc-sarama-test"
	saramaOptions.BrokerEndpoints = cluster.ListenAddrs()
	saramaOptions.Topic = topic

	saramaFactory, err := NewFactory(
		context.Background(),
		saramaOptions,
		common.NewChangefeedID4Test(common.DefaultKeyspaceName, "sarama"),
	)
	require.NoError(t, err)

	franzProducer, err := franzFactory.SyncProducer(context.Background())
	require.NoError(t, err)
	t.Cleanup(franzProducer.Close)

	saramaProducer, err := saramaFactory.SyncProducer(context.Background())
	require.NoError(t, err)
	t.Cleanup(saramaProducer.Close)

	message := &codeccommon.Message{Value: []byte("value")}
	require.NoError(t, franzProducer.SendMessage(topic, 0, message))
	require.NoError(t, saramaProducer.SendMessage(topic, 0, message))

	franzProducer.Close()
	require.NoError(t, saramaProducer.SendMessage(topic, 0, message))
}
