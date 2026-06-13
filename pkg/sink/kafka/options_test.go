// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/golang/mock/gomock"
	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

const (
	defaultMockTopicName            = "mock_topic"
	defaultMockPartitionNum   int32 = 3
	defaultReplicationFactor  int16 = 3
	defaultBrokerMessageBytes       = "1048588"
	defaultTopicMessageBytes        = "1048588"
	defaultMinInsyncReplicas        = "1"
)

type kafkaAdminFixture struct {
	admin        *MockClusterAdminClient
	topics       map[string]TopicDetail
	brokerConfig map[string]string
	topicConfig  map[string]map[string]string
}

func newKafkaAdminFixture(t *testing.T) *kafkaAdminFixture {
	t.Helper()

	ctrl := gomock.NewController(t)
	fixture := &kafkaAdminFixture{
		admin:  NewMockClusterAdminClient(ctrl),
		topics: make(map[string]TopicDetail),
		brokerConfig: map[string]string{
			BrokerMessageMaxBytesConfigName: defaultBrokerMessageBytes,
			MinInsyncReplicasConfigName:     defaultMinInsyncReplicas,
		},
		topicConfig: make(map[string]map[string]string),
	}
	fixture.addTopic(defaultMockTopicName, defaultMockPartitionNum)
	fixture.topicConfig[defaultMockTopicName] = map[string]string{
		TopicMaxMessageBytesConfigName: defaultTopicMessageBytes,
		MinInsyncReplicasConfigName:    defaultMinInsyncReplicas,
	}

	fixture.admin.EXPECT().Close().AnyTimes()
	fixture.admin.EXPECT().GetTopicsMeta(gomock.Any(), gomock.Any()).
		DoAndReturn(fixture.getTopicsMeta).AnyTimes()
	fixture.admin.EXPECT().GetTopicsPartitionsNum(gomock.Any()).
		DoAndReturn(fixture.getTopicsPartitionsNum).AnyTimes()
	fixture.admin.EXPECT().GetBrokerConfig(gomock.Any()).
		DoAndReturn(fixture.getBrokerConfig).AnyTimes()
	fixture.admin.EXPECT().GetTopicConfig(gomock.Any(), gomock.Any()).
		DoAndReturn(fixture.getTopicConfig).AnyTimes()
	fixture.admin.EXPECT().CreateTopic(gomock.Any(), gomock.Any()).
		DoAndReturn(fixture.createTopic).AnyTimes()

	return fixture
}

func (f *kafkaAdminFixture) addTopic(name string, partitionNum int32) {
	f.topics[name] = TopicDetail{Name: name, NumPartitions: partitionNum}
}

func (f *kafkaAdminFixture) getTopicsMeta(
	topics []string, _ bool,
) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))
	for _, topic := range topics {
		if detail, ok := f.topics[topic]; ok {
			result[topic] = detail
		}
	}
	return result, nil
}

func (f *kafkaAdminFixture) getTopicsPartitionsNum(
	topics []string,
) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		if detail, ok := f.topics[topic]; ok {
			result[topic] = detail.NumPartitions
		}
	}
	return result, nil
}

func (f *kafkaAdminFixture) getBrokerConfig(configName string) (string, error) {
	if value, ok := f.brokerConfig[configName]; ok {
		return value, nil
	}
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the broker's configuration", configName)
}

func (f *kafkaAdminFixture) getTopicConfig(topicName string, configName string) (string, error) {
	if _, ok := f.topics[topicName]; !ok {
		return "", errors.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the topic's configuration", topicName)
	}
	if value, ok := f.topicConfig[topicName][configName]; ok {
		return value, nil
	}
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the topic's configuration", configName)
}

func (f *kafkaAdminFixture) createTopic(detail *TopicDetail, _ bool) error {
	if detail.ReplicationFactor > defaultReplicationFactor {
		return sarama.ErrInvalidReplicationFactor
	}
	if _, ok := f.brokerConfig[MinInsyncReplicasConfigName]; !ok &&
		detail.ReplicationFactor != defaultReplicationFactor {
		return sarama.ErrPolicyViolation
	}
	f.topics[detail.Name] = *detail
	return nil
}

func (f *kafkaAdminFixture) brokerMessageMaxBytes() int {
	value, _ := strconv.Atoi(f.brokerConfig[BrokerMessageMaxBytesConfigName])
	return value
}

func (f *kafkaAdminFixture) topicMaxMessageBytes(topicName string) int {
	value, _ := strconv.Atoi(f.topicConfig[topicName][TopicMaxMessageBytesConfigName])
	return value
}

func (f *kafkaAdminFixture) setMessageMaxBytes(brokerValue, topicValue string) {
	f.brokerConfig[BrokerMessageMaxBytesConfigName] = brokerValue
	f.topicConfig[defaultMockTopicName][TopicMaxMessageBytesConfigName] = topicValue
}

func expectedAdjustedMaxMessageBytes(configuredMaxMessageBytes, sourceMaxMessageBytes int) int {
	sourceMaxMessageBytes -= maxMessageBytesOverhead
	if configuredMaxMessageBytes < sourceMaxMessageBytes {
		return configuredMaxMessageBytes
	}
	return sourceMaxMessageBytes
}

func (f *kafkaAdminFixture) setMinInsyncReplicas(minInsyncReplicas string) {
	f.topicConfig[defaultMockTopicName][MinInsyncReplicasConfigName] = minInsyncReplicas
	f.brokerConfig[MinInsyncReplicasConfigName] = minInsyncReplicas
}

func (f *kafkaAdminFixture) dropBrokerConfig(configName string) {
	delete(f.brokerConfig, configName)
}

func TestCompleteOptions(t *testing.T) {
	options := NewOptions()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&required-acks=1"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)
	require.Equal(t, int16(3), options.ReplicationFactor)
	require.Equal(t, "2.6.0", options.Version)
	require.Equal(t, 4096, options.MaxMessageBytes)
	require.Equal(t, WaitForLocal, options.RequiredAcks)
	require.Equal(t, defaultMaxRetry, options.MaxRetry)

	// multiple kafka broker endpoints
	uri = "kafka://127.0.0.1:9092,127.0.0.1:9091,127.0.0.1:9090/kafka-test?"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"),
		sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Len(t, options.BrokerEndpoints, 3)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-retry.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-retry=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid partition num.*", errors.Cause(err))

	// Unknown required-acks.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&required-acks=3"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.Regexp(t, ".*invalid required acks 3.*", errors.Cause(err))

	// invalid kafka client id
	uri = "kafka://127.0.0.1:9092/abc?kafka-client-id=^invalid$"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.True(t, errors.ErrKafkaInvalidClientID.Equal(err))

	// max-retry accepts non-negative sink-uri values.
	uri = "kafka://127.0.0.1:9092/abc?max-retry=7"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, 7, options.MaxRetry)

	uri = "kafka://127.0.0.1:9092/abc?max-retry=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, 0, options.MaxRetry)

	// Negative max-retry values are ignored.
	uri = "kafka://127.0.0.1:9092/abc?max-retry=-1"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)
	require.Equal(t, defaultMaxRetry, options.MaxRetry)
}

func TestSetPartitionNum(t *testing.T) {
	options := NewOptions()
	err := options.setPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(2), options.PartitionNum)

	options.PartitionNum = 1
	err = options.setPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)

	options.PartitionNum = 3
	err = options.setPartitionNum(2)
	require.True(t, errors.ErrKafkaInvalidPartitionNum.Equal(err))
}

func TestClientID(t *testing.T) {
	testCases := []struct {
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{
			"domain:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_domain_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234?:,\"", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_____default_123-121-121-121",
		},
		{
			"中文", "123-121-121-121",
			"", true, "",
		},
		{
			"127.0.0.1:1234",
			"123-121-121-121", "cdc-changefeed-1", false,
			"cdc-changefeed-1",
		},
	}
	for _, tc := range testCases {
		id, err := NewKafkaClientID(tc.addr,
			commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, tc.changefeedID), tc.configuredID)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, id)
		}
	}
}

func TestTimeout(t *testing.T) {
	options := NewOptions()
	require.Equal(t, 10*time.Second, options.DialTimeout)
	require.Equal(t, 10*time.Second, options.ReadTimeout)
	require.Equal(t, 10*time.Second, options.WriteTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
	require.NoError(t, err)

	require.Equal(t, 5*time.Second, options.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, options.ReadTimeout)
	require.Equal(t, 2*time.Minute, options.WriteTimeout)
}

func TestAdjustConfigFallsBackToBrokerMessageMaxBytesWhenTopicConfigMissing(t *testing.T) {
	tests := []struct {
		name                      string
		configuredMaxMessageBytes func(*kafkaAdminFixture) int
	}{
		{
			name: "keeps configured value below broker limit",
			configuredMaxMessageBytes: func(*kafkaAdminFixture) int {
				return 1024
			},
		},
		{
			name: "uses broker limit when configured value is within overhead",
			configuredMaxMessageBytes: func(f *kafkaAdminFixture) int {
				return f.brokerMessageMaxBytes() - 1
			},
		},
		{
			name: "uses broker limit when configured value is above broker",
			configuredMaxMessageBytes: func(f *kafkaAdminFixture) int {
				return f.brokerMessageMaxBytes() + 1
			},
		},
	}

	topicName := "test-topic"

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adminFixture := newKafkaAdminFixture(t)
			adminClient := adminFixture.admin

			detail := &TopicDetail{
				Name:          topicName,
				NumPartitions: 3,
			}
			err := adminClient.CreateTopic(detail, false)
			require.NoError(t, err)

			options := NewOptions()
			options.BrokerEndpoints = []string{"127.0.0.1:9092"}
			options.MaxMessageBytes = test.configuredMaxMessageBytes(adminFixture)
			expectedMaxMessageBytes := expectedAdjustedMaxMessageBytes(
				options.MaxMessageBytes,
				adminFixture.brokerMessageMaxBytes(),
			)

			ctx := context.Background()
			err = adjustOptions(ctx, adminClient, options, topicName)
			require.NoError(t, err)

			saramaConfig, err := newSaramaConfig(ctx, options)
			require.NoError(t, err)

			require.Equal(t, expectedMaxMessageBytes, options.MaxMessageBytes)
			require.Equal(t, expectedMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)
		})
	}
}

func TestAdjustConfigMinInsyncReplicas(t *testing.T) {
	adminFixture := newKafkaAdminFixture(t)
	adminClient := adminFixture.admin

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	adminFixture.setMinInsyncReplicas("2")

	ctx := context.Background()
	err := adjustOptions(
		ctx,
		adminClient,
		options,
		"create-new-fail-invalid-min-insync-replicas",
	)
	require.Regexp(
		t,
		".*`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker.*",
		errors.Cause(err),
	)

	// topic not exist, and `min.insync.replicas` not found in broker's configuration
	adminFixture.dropBrokerConfig(MinInsyncReplicasConfigName)
	topicName := "no-topic-no-min-insync-replicas"
	err = adjustOptions(ctx, adminClient, options, "no-topic-no-min-insync-replicas")
	require.Nil(t, err)
	err = adminClient.CreateTopic(&TopicDetail{
		Name:              topicName,
		ReplicationFactor: 1,
	}, false)
	require.ErrorIs(t, err, sarama.ErrPolicyViolation)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.

	// topic exist, but `min.insync.replicas` not found in topic and broker configuration
	topicName = "topic-no-options-entry"
	err = adminClient.CreateTopic(&TopicDetail{
		Name:              topicName,
		ReplicationFactor: 3,
		NumPartitions:     3,
	}, false)
	require.Nil(t, err)
	err = adjustOptions(ctx, adminClient, options, topicName)
	require.Nil(t, err)

	// topic found, and have `min.insync.replicas`, but set to 2, larger than `replication-factor`.
	adminFixture.setMinInsyncReplicas("2")
	err = adjustOptions(ctx, adminClient, options, defaultMockTopicName)
	require.Regexp(t,
		".*`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of topic.*",
		errors.Cause(err),
	)
}

func TestSkipAdjustConfigMinInsyncReplicasWhenRequiredAcksIsNotWailAll(t *testing.T) {
	adminFixture := newKafkaAdminFixture(t)
	adminClient := adminFixture.admin

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}
	options.RequiredAcks = WaitForLocal

	// Do not report an error if the replication-factor is less than min.insync.replicas(1<2).
	adminFixture.setMinInsyncReplicas("2")
	err := adjustOptions(
		context.Background(),
		adminClient,
		options,
		"skip-check-min-insync-replicas",
	)
	require.Nil(t, err, "Should not report an error when `required-acks` is not `all`")
}

func TestCreateProducerFailed(t *testing.T) {
	options := NewOptions()
	options.Version = "invalid"
	options.IsAssignedVersion = true
	saramaConfig, err := newSaramaConfig(context.Background(), options)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	require.Nil(t, saramaConfig)
}

func TestConfigurationCombinations(t *testing.T) {
	combinations := []struct {
		name                  string
		uriTemplate           string
		uriParams             []any
		brokerMessageMaxBytes string
		topicMaxMessageBytes  string
	}{
		{
			"new topic default limited by broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"not-exist-topic"},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"new topic default equals broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"not-exist-topic"},
			strconv.Itoa(config.DefaultMaxMessageBytes),
			defaultTopicMessageBytes,
		},
		{
			"new topic default below broker",
			"kafka://127.0.0.1:9092/%s",
			[]any{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			defaultTopicMessageBytes,
		},
		{
			"new topic user below broker and default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(1024*1024 - 1)},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"new topic user below default below broker",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			defaultTopicMessageBytes,
		},
		{
			"new topic broker overhead below user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(1024*1024 + 1)},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"new topic broker below default and user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"new topic user below broker above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			defaultTopicMessageBytes,
		},
		{
			"new topic broker below user above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			defaultTopicMessageBytes,
		},
		{
			"existing topic default limited by topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"existing topic default equals topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			defaultBrokerMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		{
			"existing topic default below topic",
			"kafka://127.0.0.1:9092/%s",
			[]any{defaultMockTopicName},
			defaultBrokerMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		{
			"existing topic user below topic and default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{defaultMockTopicName, strconv.Itoa(1024*1024 - 1)},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"existing topic user below default below topic",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			},
			defaultBrokerMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		{
			"existing topic topic overhead below user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{defaultMockTopicName, strconv.Itoa(1024*1024 + 1)},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"existing topic topic below default and user",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			defaultBrokerMessageBytes,
			defaultTopicMessageBytes,
		},
		{
			"existing topic user below topic above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			defaultBrokerMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
		},
		{
			"existing topic topic below user above default",
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]any{
				defaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			},
			defaultBrokerMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
	}

	for _, a := range combinations {
		t.Run(a.name, func(t *testing.T) {
			adminFixture := newKafkaAdminFixture(t)
			adminFixture.setMessageMaxBytes(a.brokerMessageMaxBytes, a.topicMaxMessageBytes)
			adminClient := adminFixture.admin

			uri := fmt.Sprintf(a.uriTemplate, a.uriParams...)
			sinkURI, err := url.Parse(uri)
			require.Nil(t, err)

			ctx := context.Background()
			options := NewOptions()
			err = options.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, config.GetDefaultReplicaConfig().Sink)
			require.Nil(t, err)

			topic, ok := a.uriParams[0].(string)
			require.True(t, ok)
			require.NotEqual(t, "", topic)

			sourceMaxMessageBytes := adminFixture.brokerMessageMaxBytes()
			if _, exists := adminFixture.topics[topic]; exists {
				sourceMaxMessageBytes = adminFixture.topicMaxMessageBytes(topic)
			}
			expectedMaxMessageBytes := expectedAdjustedMaxMessageBytes(options.MaxMessageBytes, sourceMaxMessageBytes)

			err = adjustOptions(ctx, adminClient, options, topic)
			require.Nil(t, err)
			require.Equal(t, expectedMaxMessageBytes, options.MaxMessageBytes)

			saramaConfig, err := newSaramaConfig(ctx, options)
			require.Nil(t, err)
			require.Equal(t, expectedMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

			encoderConfig := common.NewConfig(config.ProtocolOpen)
			err = encoderConfig.Apply(sinkURI, &config.SinkConfig{
				KafkaConfig: &config.KafkaConfig{
					LargeMessageHandle: config.NewDefaultLargeMessageHandleConfig(),
				},
			})
			require.Nil(t, err)
			encoderConfig.WithMaxMessageBytes(options.MaxMessageBytes)

			err = encoderConfig.Validate()
			require.Nil(t, err)

			// producer's `MaxMessageBytes` = encoder's `MaxMessageBytes`.
			require.Equal(t, expectedMaxMessageBytes, encoderConfig.MaxMessageBytes)

			adminClient.Close()
		})
	}
}

func TestMerge(t *testing.T) {
	uri := "kafka://topic/prefix"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(12),
		ReplicationFactor:         aws.Int16(5),
		KafkaVersion:              aws.String("3.1.2"),
		MaxMessageBytes:           aws.Int(1024 * 1024),
		Compression:               aws.String("gzip"),
		KafkaClientID:             aws.String("test-id"),
		AutoCreateTopic:           aws.Bool(true),
		DialTimeout:               aws.String("1m1s"),
		WriteTimeout:              aws.String("2m1s"),
		RequiredAcks:              aws.Int(1),
		SASLUser:                  aws.String("abc"),
		SASLPassword:              aws.String("123"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("keytab"),
		SASLGssAPIKeytabPath:      aws.String("SASLGssAPIKeytabPath"),
		SASLGssAPIServiceName:     aws.String("service"),
		SASLGssAPIUser:            aws.String("user"),
		SASLGssAPIPassword:        aws.String("pass"),
		SASLGssAPIRealm:           aws.String("realm"),
		SASLGssAPIDisablePafxfast: aws.Bool(true),
		EnableTLS:                 aws.Bool(true),
		CA:                        aws.String("ca.pem"),
		Cert:                      aws.String("cert.pem"),
		Key:                       aws.String("key.pem"),
	}
	c := NewOptions()
	err = c.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, replicaConfig.Sink)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.SASL.SASLUser)
	require.Equal(t, "123", c.SASL.SASLPassword)
	require.Equal(t, "plain", strings.ToLower(string(c.SASL.SASLMechanism)))
	require.Equal(t, 2, int(c.SASL.GSSAPI.AuthType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.SASL.GSSAPI.KeyTabPath)
	require.Equal(t, "service", c.SASL.GSSAPI.ServiceName)
	require.Equal(t, "user", c.SASL.GSSAPI.Username)
	require.Equal(t, "pass", c.SASL.GSSAPI.Password)
	require.Equal(t, "realm", c.SASL.GSSAPI.Realm)
	require.Equal(t, true, c.SASL.GSSAPI.DisablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)

	// test override
	uri = "kafka://topic?partition-num=12" +
		"&replication-factor=5" +
		"&kafka-version=3.1.2" +
		"&max-message-bytes=1048576" +
		"&compression=gzip" +
		"&kafka-client-id=test-id" +
		"&auto-create-topic=true" +
		"&dial-timeout=1m1s" +
		"&write-timeout=2m1s" +
		"&required-acks=1" +
		"&sasl-user=abc" +
		"&sasl-password=123" +
		"&sasl-mechanism=plain" +
		"&sasl-gssapi-auth-type=keytab" +
		"&sasl-gssapi-keytab-path=SASLGssAPIKeytabPath" +
		"&sasl-gssapi-service-name=service" +
		"&sasl-gssapi-user=user" +
		"&sasl-gssapi-password=pass" +
		"&sasl-gssapi-realm=realm" +
		"&sasl-gssapi-disable-pafxfast=true" +
		"&enable-tls=true" +
		"&ca=ca.pem" +
		"&cert=cert.pem" +
		"&key=key.pem"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(11),
		ReplicationFactor:         aws.Int16(3),
		KafkaVersion:              aws.String("3.2.2"),
		MaxMessageBytes:           aws.Int(1023 * 1024),
		Compression:               aws.String("none"),
		KafkaClientID:             aws.String("test2-id"),
		AutoCreateTopic:           aws.Bool(false),
		DialTimeout:               aws.String("1m2s"),
		WriteTimeout:              aws.String("2m3s"),
		RequiredAcks:              aws.Int(-1),
		SASLUser:                  aws.String("abcd"),
		SASLPassword:              aws.String("1234"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("user"),
		SASLGssAPIKeytabPath:      aws.String("path"),
		SASLGssAPIServiceName:     aws.String("service2"),
		SASLGssAPIUser:            aws.String("usera"),
		SASLGssAPIPassword:        aws.String("pass2"),
		SASLGssAPIRealm:           aws.String("realm2"),
		SASLGssAPIDisablePafxfast: aws.Bool(false),
		EnableTLS:                 aws.Bool(false),
		CA:                        aws.String("ca2.pem"),
		Cert:                      aws.String("cert2.pem"),
		Key:                       aws.String("key2.pem"),
	}
	c = NewOptions()
	err = c.Apply(commonType.NewChangefeedID4Test(commonType.DefaultKeyspaceName, "test"), sinkURI, replicaConfig.Sink)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.SASL.SASLUser)
	require.Equal(t, "123", c.SASL.SASLPassword)
	require.Equal(t, "plain", strings.ToLower(string(c.SASL.SASLMechanism)))
	require.Equal(t, 2, int(c.SASL.GSSAPI.AuthType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.SASL.GSSAPI.KeyTabPath)
	require.Equal(t, "service", c.SASL.GSSAPI.ServiceName)
	require.Equal(t, "user", c.SASL.GSSAPI.Username)
	require.Equal(t, "pass", c.SASL.GSSAPI.Password)
	require.Equal(t, "realm", c.SASL.GSSAPI.Realm)
	require.Equal(t, true, c.SASL.GSSAPI.DisablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)
}
