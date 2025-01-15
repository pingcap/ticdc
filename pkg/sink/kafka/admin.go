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
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type admin struct {
	ClusterAdminClient
	client       *kafka.AdminClient
	changefeedID common.ChangeFeedID
	timeoutMs    int
}

func newClusterAdminClient(
	client *kafka.AdminClient,
	changefeedID common.ChangeFeedID,
	timeoutMs int,
) ClusterAdminClient {
	return &admin{
		client:       client,
		changefeedID: changefeedID,
		timeoutMs:    timeoutMs,
	}
}

func (a *admin) clusterMetadata(_ context.Context) (*kafka.Metadata, error) {
	// request is not set, so it will return all metadata
	return a.client.GetMetadata(nil, true, a.timeoutMs)
}

func (a *admin) GetAllBrokers(ctx context.Context) ([]Broker, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make([]Broker, 0, len(response.Brokers))
	for _, broker := range response.Brokers {
		result = append(result, Broker{
			ID: broker.ID,
		})
	}
	return result, nil
}

func (a *admin) GetBrokerConfig(ctx context.Context, configName string) (string, error) {
	response, err := a.clusterMetadata(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	controllerID := response.Brokers[0].ID

	resources := []kafka.ConfigResource{
		{
			Type:   kafka.ResourceBroker,
			Name:   strconv.Itoa(int(controllerID)),
			Config: []kafka.ConfigEntry{{Name: configName}},
		},
	}

	results, err := a.client.DescribeConfigs(ctx, resources)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(results) == 0 || len(results[0].Config) == 0 {
		log.Warn("Kafka config item not found",
			zap.String("configName", configName))
		return "", errors.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range results[0].Config {
		if entry.Name == configName {
			return entry.Value, nil
		}
	}

	log.Warn("Kafka config item not found",
		zap.String("configName", configName))
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the broker's configuration", configName)
}

func (a *admin) GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error) {
	resources := []kafka.ConfigResource{
		{
			Type:   kafka.ResourceTopic,
			Name:   topicName,
			Config: []kafka.ConfigEntry{{Name: configName}},
		},
	}

	results, err := a.client.DescribeConfigs(ctx, resources)
	if err != nil {
		return "", errors.Trace(err)
	}

	if len(results) == 0 || len(results[0].Config) == 0 {
		log.Warn("Kafka config item not found",
			zap.String("configName", configName))
		return "", errors.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the topic's configuration", configName)
	}

	// For compatibility with KOP, we checked all return values.
	// 1. Kafka only returns requested configs.
	// 2. Kop returns all configs.
	for _, entry := range results[0].Config {
		if entry.Name == configName {
			log.Info("Kafka config item found",
				zap.String("namespace", a.changefeedID.Namespace()),
				zap.String("changefeed", a.changefeedID.Name()),
				zap.String("configName", configName),
				zap.String("configValue", entry.Value))
			return entry.Value, nil
		}
	}
	log.Warn("Kafka config item not found",
		zap.String("configName", configName))
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the topic's configuration", configName)
}

func (a *admin) GetTopicsMeta(
	ctx context.Context,
	topics []string,
	ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	resp, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]TopicDetail, len(resp.Topics))

	for _, topic := range topics {
		msg, ok := resp.Topics[topic]
		if !ok {
			log.Warn("fetch topic meta failed",
				zap.String("topic", topic), zap.Error(msg.Error))
			continue
		}
		if msg.Error.Code() != kafka.ErrNoError {
			if !ignoreTopicError {
				return nil, errors.Trace(msg.Error)
			}
			log.Warn("fetch topic meta failed",
				zap.String("topic", topic), zap.Error(msg.Error))
			continue
		}
		result[topic] = TopicDetail{
			Name:          topic,
			NumPartitions: int32(len(msg.Partitions)),
		}
	}
	return result, nil
}

func (a *admin) GetTopicsPartitionsNum(
	ctx context.Context, topics []string,
) (map[string]int32, error) {
	resp, err := a.clusterMetadata(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		msg, ok := resp.Topics[topic]
		if !ok {
			log.Warn("fetch topic meta failed",
				zap.String("topic", topic), zap.Error(msg.Error))
			continue
		}
		result[topic] = int32(len(msg.Partitions))
	}
	return result, nil
}

func (a *admin) CreateTopic(
	ctx context.Context,
	detail *TopicDetail,
	validateOnly bool,
) error {
	topics := []kafka.TopicSpecification{
		{
			Topic:             detail.Name,
			NumPartitions:     int(detail.NumPartitions),
			ReplicationFactor: int(detail.ReplicationFactor),
		},
	}
	results, err := a.client.CreateTopics(ctx, topics)
	if err != nil {
		return errors.Trace(err)
	}

	for _, res := range results {
		if res.Error.Code() == kafka.ErrTopicAlreadyExists {
			return errors.Trace(err)
		}
	}
	return nil
}

func (a *admin) Close() {
	log.Info("admin client start closing",
		zap.String("namespace", a.changefeedID.Namespace()),
		zap.String("changefeed", a.changefeedID.Name()))
	a.client.Close()
	log.Info("kafka admin client is fully closed",
		zap.String("namespace", a.changefeedID.Namespace()),
		zap.String("changefeed", a.changefeedID.Name()))
}
