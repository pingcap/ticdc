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

// TopicDetail represent a topic's detail information.
type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// Broker represents a Kafka broker.
type Broker struct {
	ID int32
}

// ClusterAdminClient is the administrative client for Kafka,
// which supports managing and inspecting topics, brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// GetAllBrokers return all brokers among the cluster
	GetAllBrokers() []Broker

	// GetBrokerConfig return the broker level configuration with the `configName`
	GetBrokerConfig(configName string) (string, error)

	// GetTopicConfig return the topic level configuration with the `configName`
	GetTopicConfig(topicName string, configName string) (string, error)

	// GetTopicsMeta return all target topics' metadata
	// if `ignoreTopicError` is true, ignore the topic error and return the metadata of valid topics
	GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]TopicDetail, error)

	// GetTopicsPartitionsNum return the number of partitions of each topic.
	GetTopicsPartitionsNum(topics []string) (map[string]int32, error)

	// CreateTopic creates a new topic.
	CreateTopic(detail *TopicDetail, validateOnly bool) error

	Heartbeat()

	// Close shuts down the admin client.
	Close()
}
