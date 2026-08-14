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

package franz

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Broker struct{ ID int32 }

type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

type Admin struct {
	changefeed common.ChangeFeedID

	client  *kgo.Client
	admin   *kadm.Client
	timeout time.Duration
}

func NewAdmin(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	cfg Config,
) (*Admin, error) {
	opts, err := newClientOptions(ctx, changefeedID, "admin", cfg, nil)
	if err != nil {
		return nil, err
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.WrapError(errors.ErrNewKafkaSink, err)
	}

	return &Admin{
		changefeed: changefeedID,
		client:     client,
		admin:      kadm.NewClient(client),
		timeout:    cfg.requestTimeout(),
	}, nil
}

func (a *Admin) GetAllBrokers() []Broker {
	ctx, cancel := context.WithTimeout(a.client.Context(), a.timeout)
	defer cancel()

	meta, err := a.admin.BrokerMetadata(ctx)
	if err != nil {
		return nil
	}

	brokers := make([]Broker, 0, len(meta.Brokers))
	for id := range meta.Brokers {
		brokers = append(brokers, Broker{ID: int32(id)})
	}

	return brokers
}

func (a *Admin) GetBrokerConfig(configName string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(a.client.Context(), a.timeout)
	defer cancel()

	meta, err := a.admin.BrokerMetadata(ctx)
	if err != nil {
		if isAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-cluster", "cluster")
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-cluster", "cluster")
	}

	if meta.Controller < 0 {
		return "", false, errors.ErrKafkaAdminAPI.GenWithStackByArgs("describe-cluster", "cluster")
	}

	configs, err := a.admin.DescribeBrokerConfigs(ctx, meta.Controller)
	if err != nil {
		if isAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", configName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", configName)
	}

	controllerName := strconv.Itoa(int(meta.Controller))
	resource, err := configs.On(controllerName, nil)
	if err != nil {
		if isAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", configName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", configName)
	}

	if resource.Err != nil {
		if isAuthorizationFailed(resource.Err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, resource.Err, "describe-config", configName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, resource.Err, "describe-config", configName)
	}

	for _, entry := range resource.Configs {
		if entry.Key == configName {
			return entry.MaybeValue(), true, nil
		}
	}

	return "", false, nil
}

func (a *Admin) GetTopicConfig(topicName string, configName string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(a.client.Context(), a.timeout)
	defer cancel()

	configs, err := a.admin.DescribeTopicConfigs(ctx, topicName)
	if err != nil {
		if isAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", topicName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", topicName)
	}

	resource, err := configs.On(topicName, nil)
	if err != nil {
		if isAuthorizationFailed(err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-config", topicName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-config", topicName)
	}

	if resource.Err != nil {
		if isAuthorizationFailed(resource.Err) {
			return "", false, errors.WrapError(errors.ErrKafkaAuthorizationFailed, resource.Err, "describe-config", topicName)
		}

		return "", false, errors.WrapError(errors.ErrKafkaAdminAPI, resource.Err, "describe-config", topicName)
	}

	for _, entry := range resource.Configs {
		if entry.Key == configName {
			return entry.MaybeValue(), true, nil
		}
	}

	return "", false, nil
}

func (a *Admin) GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]TopicDetail, error) {
	if len(topics) == 0 {
		return make(map[string]TopicDetail), nil
	}

	ctx, cancel := context.WithTimeout(a.client.Context(), a.timeout)
	defer cancel()

	meta, err := a.admin.Metadata(ctx, topics...)
	if err != nil {
		resource := strings.Join(topics, ",")
		if isAuthorizationFailed(err) {
			return nil, errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "describe-topics", resource)
		}

		return nil, errors.WrapError(errors.ErrKafkaAdminAPI, err, "describe-topics", resource)
	}

	return topicDetailsFromMetadata(meta, topics, ignoreTopicError)
}

func topicDetailsFromMetadata(meta kadm.Metadata, topics []string, ignoreTopicError bool) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))
	for _, topic := range topics {
		detail, ok := meta.Topics[topic]
		if !ok {
			if ignoreTopicError {
				continue
			}

			return nil, errors.WrapError(errors.ErrKafkaAdminAPI, kerr.UnknownTopicOrPartition, "describe-topic", topic)
		}

		if detail.Err == nil {
			result[topic] = TopicDetail{
				Name:          topic,
				NumPartitions: int32(len(detail.Partitions)),
			}
			continue
		}

		if ignoreTopicError && errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			continue
		}

		if isAuthorizationFailed(detail.Err) {
			return nil, errors.WrapError(errors.ErrKafkaAuthorizationFailed, detail.Err, "describe-topic", topic)
		}

		return nil, errors.WrapError(errors.ErrKafkaAdminAPI, detail.Err, "describe-topic", topic)
	}

	return result, nil
}

func isAuthorizationFailed(err error) bool {
	return errors.Is(err, errors.ErrKafkaAuthorizationFailed) ||
		errors.Is(err, kerr.TopicAuthorizationFailed) ||
		errors.Is(err, kerr.ClusterAuthorizationFailed)
}

func (a *Admin) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	details, err := a.GetTopicsMeta(topics, false)
	if err != nil {
		return nil, err
	}

	partitions := make(map[string]int32, len(details))
	for topic, detail := range details {
		partitions[topic] = detail.NumPartitions
	}

	return partitions, nil
}

func (a *Admin) CreateTopic(detail *TopicDetail) error {
	ctx, cancel := context.WithTimeout(a.client.Context(), a.timeout)
	defer cancel()

	responses, err := a.admin.CreateTopics(ctx, detail.NumPartitions, detail.ReplicationFactor, nil, detail.Name)
	if err != nil {
		if isAuthorizationFailed(err) {
			return errors.WrapError(errors.ErrKafkaAuthorizationFailed, err, "create-topic", detail.Name)
		}

		return errors.WrapError(errors.ErrKafkaAdminAPI, err, "create-topic", detail.Name)
	}

	resp, ok := responses[detail.Name]
	if !ok {
		return errors.ErrKafkaAdminAPI.GenWithStackByArgs("create-topic", detail.Name)
	}

	if resp.Err == nil {
		return nil
	}

	if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return nil
	}

	if errors.Is(resp.Err, kerr.InvalidReplicationFactor) {
		return errors.WrapError(errors.ErrKafkaInvalidConfig, resp.Err)
	}

	if isAuthorizationFailed(resp.Err) {
		return errors.WrapError(errors.ErrKafkaAuthorizationFailed, resp.Err, "create-topic", detail.Name)
	}

	return errors.WrapError(errors.ErrKafkaAdminAPI, resp.Err, "create-topic", detail.Name)
}

func (a *Admin) Close() {
	a.admin.Close()
}
