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
	"io"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
)

func TestIsUnretryableClientError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		err         error
		unretryable bool
	}{
		{name: "unknown topic", err: kerr.UnknownTopicOrPartition},
		{name: "leader unavailable", err: kerr.LeaderNotAvailable},
		{name: "request timeout", err: kerr.RequestTimedOut},
		{name: "network exception", err: kerr.NetworkException},
		{name: "controller changed", err: kerr.NotController},
		{name: "EOF", err: io.EOF},
		{name: "invalid topic", err: kerr.InvalidTopicException, unretryable: true},
		{name: "invalid config", err: kerr.InvalidConfig, unretryable: true},
		{name: "SASL authentication failure", err: kerr.SaslAuthenticationFailed, unretryable: true},
		{name: "unsupported SASL mechanism", err: kerr.UnsupportedSaslMechanism, unretryable: true},
		{name: "illegal SASL state", err: kerr.IllegalSaslState, unretryable: true},
		{name: "unsupported version", err: kerr.UnsupportedVersion, unretryable: true},
		{name: "invalid request", err: kerr.InvalidRequest, unretryable: true},
		{
			name:        "wrapped invalid topic",
			err:         errors.WrapError(errors.ErrKafkaAdminAPI, kerr.InvalidTopicException, "describe-topic", "test-topic"),
			unretryable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.unretryable, IsUnretryableKafkaError(test.err))
		})
	}
}

func TestFactorySelection(t *testing.T) {
	const topic = "factory-selection"
	cluster := kfake.MustCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	defer cluster.Close()

	changefeedID := common.NewChangefeedID4Test(common.DefaultKeyspaceName, "factory-selection")
	for _, test := range []struct {
		client   string
		expected Factory
	}{
		{client: KafkaClientFranz, expected: &franzFactory{}},
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

			factory.CleanupMetrics()
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
	require.IsType(t, &franzFactory{}, factory)
	factory.CleanupMetrics()
}
