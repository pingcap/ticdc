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
	"strconv"
	"strings"

	"github.com/IBM/sarama"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type saramaAdminClient struct {
	changefeed common.ChangeFeedID

	// client is the underlying sarama client created for this admin wrapper.
	// It must be closed to stop background goroutines (e.g. metadata updater) and release memory.
	client saramaClient
	admin  saramaClusterAdmin
}

type saramaClient interface {
	Brokers() []*sarama.Broker
	Partitions(topic string) ([]int32, error)
	Close() error
}

type saramaClusterAdmin interface {
	DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error)
	DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error)
	DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error)
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	Close() error
}

func (a *saramaAdminClient) GetAllBrokers() []Broker {
	brokers := a.client.Brokers()
	result := make([]Broker, 0, len(brokers))
	for _, broker := range brokers {
		result = append(result, Broker{
			ID: broker.ID(),
		})
	}
	return result
}

func (a *saramaAdminClient) GetBrokerConfig(configName string) (string, bool, error) {
	_, controller, err := a.admin.DescribeCluster()
	if err != nil {
		if IsAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-cluster", "cluster")
		}
		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-cluster", "cluster")
	}

	configEntries, err := a.admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.BrokerResource,
		Name:        strconv.Itoa(int(controller)),
		ConfigNames: []string{configName},
	})
	if err != nil {
		if IsAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", configName)
		}
		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", configName)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range configEntries {
		if entry.Name == configName {
			return entry.Value, true, nil
		}
	}
	return "", false, nil
}

func (a *saramaAdminClient) GetTopicConfig(topicName string, configName string) (string, bool, error) {
	configEntries, err := a.admin.DescribeConfig(sarama.ConfigResource{
		Type:        sarama.TopicResource,
		Name:        topicName,
		ConfigNames: []string{configName},
	})
	if err != nil {
		if IsAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", topicName)
		}
		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", topicName)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range configEntries {
		if entry.Name == configName {
			return entry.Value, true, nil
		}
	}
	return "", false, nil
}

func (a *saramaAdminClient) GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))

	metaList, err := a.admin.DescribeTopics(topics)
	if err != nil {
		resource := strings.Join(topics, ",")
		if IsAuthorizationFailed(err) {
			return nil, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-topics", resource)
		}
		return nil, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-topics", resource)
	}

	for _, meta := range metaList {
		if meta.Err != sarama.ErrNoError {
			if !ignoreTopicError {
				if IsAuthorizationFailed(meta.Err) {
					return nil, errors.WrapError(errors.ErrKafkaAuthorizationFailed, meta.Err, "describe-topic", meta.Name)
				}
				return nil, errors.WrapError(errors.ErrKafkaAdminAPI, meta.Err, "describe-topic", meta.Name)
			}
			log.Warn("kafka topic metadata refresh failed",
				zap.String("keyspace", a.changefeed.Keyspace()),
				zap.String("changefeed", a.changefeed.Name()),
				zap.String("topic", meta.Name),
				zap.Error(meta.Err))
			continue
		}
		result[meta.Name] = TopicDetail{
			Name:          meta.Name,
			NumPartitions: int32(len(meta.Partitions)),
		}
	}
	return result, nil
}

// IsAuthorizationFailed checks whether err is a Kafka authorization failure.
func IsAuthorizationFailed(err error) bool {
	return errors.Is(err, errors.ErrKafkaAuthorizationFailed) ||
		errors.Is(err, sarama.ErrTopicAuthorizationFailed) ||
		errors.Is(err, sarama.ErrClusterAuthorizationFailed)
}

// IsUnretryableKafkaError reports whether err is not retryable.
// See Apache Kafka protocol error definitions:
// https://kafka.apache.org/38/generated/protocol_errors.html
func IsUnretryableKafkaError(err error) bool {
	if IsAuthorizationFailed(err) ||
		errors.Is(err, errors.ErrKafkaInvalidConfig) ||
		errors.Is(err, sarama.ErrInvalidTopic) ||
		errors.Is(err, sarama.ErrInvalidConfig) ||
		errors.Is(err, sarama.ErrSASLAuthenticationFailed) ||
		errors.Is(err, sarama.ErrUnsupportedSASLMechanism) ||
		errors.Is(err, sarama.ErrIllegalSASLState) ||
		errors.Is(err, sarama.ErrUnsupportedVersion) ||
		errors.Is(err, sarama.ErrInvalidRequest) {
		return true
	}

	var configErr sarama.ConfigurationError
	return errors.As(err, &configErr)
}

func (a *saramaAdminClient) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		partition, err := a.client.Partitions(topic)
		if err != nil {
			if IsAuthorizationFailed(err) {
				return nil, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "list-partitions", topic)
			}
			return nil, errors.WrapError(errors.ErrKafkaAdminAPI, err, "list-partitions", topic)
		}
		result[topic] = int32(len(partition))
	}

	return result, nil
}

func (a *saramaAdminClient) CreateTopic(detail *TopicDetail) error {
	request := &sarama.TopicDetail{
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	}

	err := a.admin.CreateTopic(detail.Name, request, false)
	// Ignore the already exists error because it's not harmful.
	if err != nil && !strings.Contains(err.Error(), sarama.ErrTopicAlreadyExists.Error()) {
		if IsAuthorizationFailed(err) {
			return errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "create-topic", detail.Name)
		}
		return errors.WrapError(errors.ErrKafkaAdminAPI, err, "create-topic", detail.Name)
	}
	return nil
}

func (a *saramaAdminClient) Close() {
	// For admins created via sarama.NewClusterAdminFromClient, admin.Close() takes care
	// of closing the underlying client as well. Fall back to closing the client directly
	// only when admin is unexpectedly nil.
	if a.admin != nil {
		if err := a.admin.Close(); err != nil {
			log.Warn("kafka admin client close failed",
				zap.String("keyspace", a.changefeed.Keyspace()),
				zap.String("changefeed", a.changefeed.Name()),
				zap.Error(err))
		}
		return
	}
	if a.client != nil {
		if err := a.client.Close(); err != nil {
			log.Warn("kafka client close failed",
				zap.String("keyspace", a.changefeed.Keyspace()),
				zap.String("changefeed", a.changefeed.Name()),
				zap.Error(err))
		}
	}
}
