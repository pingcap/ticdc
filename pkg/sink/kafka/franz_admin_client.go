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
	"github.com/pingcap/ticdc/pkg/errors"
	kafkafranz "github.com/pingcap/ticdc/pkg/sink/kafka/franz"
	"github.com/twmb/franz-go/pkg/kadm"
)

// franzAdminClientAdapter adapts the franz-go admin client implementation to kafka.ClusterAdminClient.
// It intentionally lives in the kafka package to reuse existing option adjustment logic without
// introducing an import cycle (kafka -> franz -> kafka).
type franzAdminClientAdapter struct {
	inner *kafkafranz.AdminClient
}

func (a *franzAdminClientAdapter) GetAllBrokers() []Broker {
	brokers := a.inner.GetAllBrokers()
	result := make([]Broker, 0, len(brokers))
	for _, b := range brokers {
		result = append(result, Broker{ID: b.ID})
	}
	return result
}

func (a *franzAdminClientAdapter) GetBrokerConfig(configName string) (string, error) {
	return a.inner.GetBrokerConfig(configName)
}

func (a *franzAdminClientAdapter) GetTopicConfig(topicName string, configName string) (string, error) {
	return a.inner.GetTopicConfig(topicName, configName)
}

func (a *franzAdminClientAdapter) GetTopicsMeta(
	topics []string,
	ignoreTopicError bool,
) (map[string]TopicDetail, error) {
	meta, err := a.inner.GetTopicsMeta(topics, ignoreTopicError)
	if err != nil {
		return nil, err
	}

	result := make(map[string]TopicDetail, len(meta))
	for topic, detail := range meta {
		result[topic] = TopicDetail{
			Name:              detail.Name,
			NumPartitions:     detail.NumPartitions,
			ReplicationFactor: detail.ReplicationFactor,
		}
	}
	return result, nil
}

func (a *franzAdminClientAdapter) GetTopicsPartitionsNum(topics []string) (map[string]int32, error) {
	return a.inner.GetTopicsPartitionsNum(topics)
}

func (a *franzAdminClientAdapter) CreateTopic(detail *TopicDetail, validateOnly bool) error {
	if detail == nil {
		return errors.ErrKafkaInvalidConfig.GenWithStack("topic detail must not be nil")
	}
	franzDetail := &kadm.TopicDetail{
		Name:              detail.Name,
		NumPartitions:     detail.NumPartitions,
		ReplicationFactor: detail.ReplicationFactor,
	}
	return a.inner.CreateTopic(franzDetail, validateOnly)
}

func (a *franzAdminClientAdapter) Heartbeat() {
	a.inner.Heartbeat()
}

func (a *franzAdminClientAdapter) Close() {
	if a.inner != nil {
		a.inner.Close()
	}
}
