// Copyright 2025 PingCAP, Inc.
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
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type franzAdminClient struct {
	changefeed common.ChangeFeedID

	client  *kgo.Client
	admin   *kadm.Client
	timeout time.Duration
}

func newFranzAdminClient(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	o *options,
	hook kgo.Hook,
) (ClusterAdminClient, error) {
	baseOpts, err := buildFranzBaseOptions(ctx, o, hook)
	if err != nil {
		return nil, errors.Trace(err)
	}

	client, err := kgo.NewClient(baseOpts...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	timeout := o.ReadTimeout
	if o.WriteTimeout > timeout {
		timeout = o.WriteTimeout
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	return &franzAdminClient{
		changefeed: changefeedID,
		client:     client,
		admin:      kadm.NewClient(client),
		timeout:    timeout,
	}, nil
}

func (a *franzAdminClient) newRequestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(a.client.Context(), a.timeout)
}

func (a *franzAdminClient) GetAllBrokers() []Broker {
	ctx, cancel := a.newRequestContext()
	defer cancel()

	meta, err := a.admin.BrokerMetadata(ctx)
	if err != nil {
		log.Warn("Kafka admin client fetch broker metadata failed",
			zap.String("keyspace", a.changefeed.Keyspace()),
			zap.String("changefeed", a.changefeed.Name()),
			zap.Error(err))
		return nil
	}

	result := make([]Broker, 0, len(meta.Brokers))
	for _, broker := range meta.Brokers {
		if broker.NodeID < 0 {
			continue
		}
		result = append(result, Broker{ID: broker.NodeID})
	}
	return result
}

func (a *franzAdminClient) GetBrokerConfig(configName string) (string, error) {
	ctx, cancel := a.newRequestContext()
	defer cancel()

	meta, err := a.admin.BrokerMetadata(ctx)
	if err != nil {
		return "", errors.Trace(err)
	}
	if meta.Controller < 0 {
		return "", errors.ErrKafkaInvalidConfig.GenWithStack("kafka controller is not available")
	}

	configs, err := a.admin.DescribeBrokerConfigs(ctx, meta.Controller)
	if err != nil {
		return "", errors.Trace(err)
	}

	controllerName := strconv.Itoa(int(meta.Controller))
	resource, err := configs.On(controllerName, nil)
	if err != nil {
		return "", errors.Trace(err)
	}
	if resource.Err != nil {
		return "", errors.Trace(resource.Err)
	}

	for _, entry := range resource.Configs {
		if entry.Key == configName {
			return entry.MaybeValue(), nil
		}
	}

	log.Warn("Kafka config item not found",
		zap.String("keyspace", a.changefeed.Keyspace()),
		zap.String("changefeed", a.changefeed.Name()),
		zap.String("configName", configName))
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the broker's configuration", configName)
}

func (a *franzAdminClient) GetTopicConfig(topicName string, configName string) (string, error) {
	ctx, cancel := a.newRequestContext()
	defer cancel()

	configs, err := a.admin.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		return "", errors.Trace(err)
	}

	resource, err := configs.On(topicName, nil)
	if err != nil {
		return "", errors.Trace(err)
	}
	if resource.Err != nil {
		return "", errors.Trace(resource.Err)
	}

	for _, entry := range resource.Configs {
		if entry.Key == configName {
			log.Info("Kafka config item found",
				zap.String("keyspace", a.changefeed.Keyspace()),
				zap.String("changefeed", a.changefeed.Name()),
				zap.String("configName", configName),
				zap.String("configValue", entry.MaybeValue()))
			return entry.MaybeValue(), nil
		}
	}

	log.Warn("Kafka config item not found",
		zap.String("keyspace", a.changefeed.Keyspace()),
		zap.String("changefeed", a.changefeed.Name()),
		zap.String("configName", configName))
	return "", errors.ErrKafkaConfigNotFound.GenWithStack(
		"cannot find the `%s` from the topic's configuration", configName)
}

func (a *franzAdminClient) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	if len(topics) == 0 {
		return make(map[string]TopicDetail), nil
	}

	ctx, cancel := a.newRequestContext()
	defer cancel()

	meta, err := a.admin.Metadata(ctx, topics...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]TopicDetail, len(topics))
	for _, topic := range topics {
		detail, ok := meta.Topics[topic]
		if !ok {
			continue
		}
		if detail.Err != nil {
			if errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
				continue
			}
			if !ignoreTopicError {
				return nil, errors.Trace(detail.Err)
			}
			log.Warn("fetch topic meta failed",
				zap.String("keyspace", a.changefeed.Keyspace()),
				zap.String("changefeed", a.changefeed.Name()),
				zap.String("topic", topic),
				zap.Error(detail.Err))
			continue
		}

		result[topic] = TopicDetail{
			Name:          topic,
			NumPartitions: int32(len(detail.Partitions)),
		}
	}
	return result, nil
}

func (a *franzAdminClient) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	if len(topics) == 0 {
		return make(map[string]int32), nil
	}

	ctx, cancel := a.newRequestContext()
	defer cancel()

	meta, err := a.admin.Metadata(ctx, topics...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		detail, ok := meta.Topics[topic]
		if !ok {
			return nil, errors.Trace(kerr.UnknownTopicOrPartition)
		}
		if detail.Err != nil {
			return nil, errors.Trace(detail.Err)
		}
		result[topic] = int32(len(detail.Partitions))
	}
	return result, nil
}

func (a *franzAdminClient) CreateTopic(detail *TopicDetail, validateOnly bool) error {
	ctx, cancel := a.newRequestContext()
	defer cancel()

	var (
		responses kadm.CreateTopicResponses
		err       error
	)
	if validateOnly {
		responses, err = a.admin.ValidateCreateTopics(ctx, detail.NumPartitions, detail.ReplicationFactor, nil, detail.Name)
	} else {
		responses, err = a.admin.CreateTopics(ctx, detail.NumPartitions, detail.ReplicationFactor, nil, detail.Name)
	}
	if err != nil {
		return errors.Trace(err)
	}

	resp, ok := responses[detail.Name]
	if !ok {
		return errors.ErrKafkaCreateTopic.GenWithStack("kafka topic create response is missing")
	}
	if resp.Err != nil {
		if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return nil
		}
		return errors.Trace(resp.Err)
	}
	return nil
}

func (a *franzAdminClient) Heartbeat() {}

func (a *franzAdminClient) Close() {
	if a.admin != nil {
		a.admin.Close()
	}
}
