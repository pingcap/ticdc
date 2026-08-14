// Copyright 2022 PingCAP, Inc.
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

package topicmanager

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/sink/kafka"
	"go.uber.org/zap"
)

const (
	// metaRefreshInterval is the interval of refreshing metadata.
	// We can't get the metadata too frequently, because it may cause
	// the kafka cluster to be overloaded. Especially when there are
	// many topics in the cluster or there are many TiCDC changefeeds.
	metaRefreshInterval = 10 * time.Minute
)

// kafkaTopicManager is a manager for kafka topics.
type kafkaTopicManager struct {
	changefeedID common.ChangeFeedID

	defaultTopic string

	admin kafka.AdminClient
	cfg   *kafka.AutoCreateTopicConfig

	topics sync.Map
	// cancel is used to cancel the background goroutine.
	cancel context.CancelFunc
}

// newKafkaTopicManager creates a topic manager without starting background work.
func newKafkaTopicManager(
	defaultTopic string,
	changefeedID common.ChangeFeedID,
	admin kafka.AdminClient,
	cfg *kafka.AutoCreateTopicConfig,
) *kafkaTopicManager {
	return &kafkaTopicManager{
		defaultTopic: defaultTopic,
		changefeedID: changefeedID,
		admin:        admin,
		cfg:          cfg,
	}
}

// EnsureTopic creates the topic if needed and waits until it is visible.
func EnsureTopic(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	topic string,
	topicCfg *kafka.AutoCreateTopicConfig,
	adminClient kafka.AdminClient,
) error {
	topicManager := newKafkaTopicManager(topic, changefeedID, adminClient, topicCfg)
	_, err := topicManager.CreateTopicAndWaitUntilVisible(ctx, topic)
	return err
}

// GetTopicManagerAndTryCreateTopic returns the topic manager and try to create the topic.
func GetTopicManagerAndTryCreateTopic(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	topic string,
	topicCfg *kafka.AutoCreateTopicConfig,
	adminClient kafka.AdminClient,
) (TopicManager, error) {
	topicManager := newKafkaTopicManager(topic, changefeedID, adminClient, topicCfg)

	if _, err := topicManager.CreateTopicAndWaitUntilVisible(ctx, topic); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	topicManager.cancel = cancel
	go topicManager.backgroundRefreshMeta(ctx)

	return topicManager, nil
}

// GetPartitionNum returns the number of partitions of the topic.
// It may also try to update the topics' information maintained by manager.
func (m *kafkaTopicManager) GetPartitionNum(
	ctx context.Context,
	topic string,
) (int32, error) {
	if partitions, ok := m.topics.Load(topic); ok {
		return partitions.(int32), nil
	}

	// If the topic is not in the metadata, we try to create the topic.
	partitionNum, err := m.CreateTopicAndWaitUntilVisible(ctx, topic)
	if err != nil {
		return 0, err
	}

	return partitionNum, nil
}

func (m *kafkaTopicManager) backgroundRefreshMeta(ctx context.Context) {
	ticker := time.NewTicker(metaRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// We ignore the error here, because the error may be caused by the
			// network problem, and we can try to get the metadata next time.
			topicPartitionNums, _ := m.fetchAllTopicsPartitionsNum()
			for topic, partitionNum := range topicPartitionNums {
				m.tryUpdatePartitionsAndLogging(topic, partitionNum)
			}
		}
	}
}

// tryUpdatePartitionsAndLogging try to update the partitions of the topic.
func (m *kafkaTopicManager) tryUpdatePartitionsAndLogging(topic string, partitions int32) {
	oldPartitions, ok := m.topics.Load(topic)
	if ok {
		if oldPartitions.(int32) != partitions {
			m.topics.Store(topic, partitions)
			log.Info(
				"kafka topic partition count changed",
				zap.String("keyspace", m.changefeedID.Keyspace()),
				zap.String("changefeed", m.changefeedID.Name()),
				zap.String("topic", topic),
				zap.Int32("oldPartitionNum", oldPartitions.(int32)),
				zap.Int32("newPartitionNum", partitions),
			)
		}
	} else {
		m.topics.Store(topic, partitions)
	}
}

// fetchAllTopicsPartitionsNum fetches all topics' partitions number.
// The error returned by this method could be a transient error that is fixable by the underlying logic.
// When handling this error, please be cautious.
// If you simply throw the error to the caller, it may impact the robustness of your program.
func (m *kafkaTopicManager) fetchAllTopicsPartitionsNum() (map[string]int32, error) {
	var topics []string
	m.topics.Range(func(key, _ any) bool {
		topics = append(topics, key.(string))
		return true
	})

	start := time.Now()
	numPartitions, err := m.admin.GetTopicsPartitionsNum(topics)
	if err != nil {
		log.Warn(
			"kafka topic metadata refresh failed",
			zap.String("keyspace", m.changefeedID.Keyspace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err),
		)
		return nil, err
	}

	// it may happen the following case:
	// 1. user create the default topic with partition number set as 3 manually
	// 2. set the partition-number as 2 in the sink-uri.
	// in the such case, we should use 2 instead of 3 as the partition number.
	_, ok := numPartitions[m.defaultTopic]
	if ok {
		numPartitions[m.defaultTopic] = m.cfg.PartitionNum
	}
	return numPartitions, nil
}

// waitUntilTopicVisible is called after CreateTopic to make sure the topic
// can be safely written to. The reason is that it may take several seconds after
// CreateTopic returns success for all the brokers to become aware that the
// topics have been created.
// See https://kafka.apache.org/23/javadoc/org/apache/kafka/clients/admin/AdminClient.html
func (m *kafkaTopicManager) waitUntilTopicVisible(
	ctx context.Context,
	topicName string,
) error {
	start := time.Now()
	topics := []string{topicName}
	err := retry.Do(ctx, func() error {
		// ignoreTopicError is set to false since we just create the topic,
		// make sure the topic is visible.
		meta, err := m.admin.GetTopicsMeta(topics, false)
		if err != nil {
			return err
		}
		_, ok := meta[topicName]
		if !ok {
			return errors.ErrKafkaAdminAPI.GenWithStackByArgs("describe-topic", topicName)
		}
		return nil
	}, retry.WithBackoffBaseDelay(500),
		retry.WithBackoffMaxDelay(1000),
		retry.WithMaxTries(6),
	)
	if err != nil {
		log.Warn("kafka topic metadata refresh failed",
			zap.String("keyspace", m.changefeedID.Keyspace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("topic", topicName),
			zap.Duration("duration", time.Since(start)),
			zap.Error(err))
	}
	return err
}

// createTopic creates a topic with the given name
// and returns the number of partitions.
func (m *kafkaTopicManager) createTopic(
	_ context.Context,
	topicName string,
) (int32, error) {
	if !m.cfg.AutoCreate {
		return 0, errors.ErrKafkaInvalidConfig.GenWithStack("`auto-create-topic` is false, and %s not found", topicName)
	}

	if err := m.cfg.ValidateReplicationFactor(m.admin); err != nil {
		return 0, err
	}

	start := time.Now()
	err := m.admin.CreateTopic(&kafka.TopicDetail{
		Name:              topicName,
		NumPartitions:     m.cfg.PartitionNum,
		ReplicationFactor: m.cfg.ReplicationFactor,
	})
	if err != nil {
		log.Error(
			"kafka topic creation failed",
			zap.String("keyspace", m.changefeedID.Keyspace()),
			zap.String("changefeed", m.changefeedID.Name()),
			zap.String("topic", topicName),
			zap.Int32("partitionNum", m.cfg.PartitionNum),
			zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
			zap.Error(err),
			zap.Duration("duration", time.Since(start)),
		)
		return 0, err
	}

	m.tryUpdatePartitionsAndLogging(topicName, m.cfg.PartitionNum)

	return m.cfg.PartitionNum, nil
}

// CreateTopicAndWaitUntilVisible wraps createTopic and waitUntilTopicVisible together.
// If topic creation fails due to insufficient permissions, allow the changefeed
// to be created, the error will be returned later by other operations such as send messages.
// The topic can be created or modified externally later to fix the error.
func (m *kafkaTopicManager) CreateTopicAndWaitUntilVisible(
	ctx context.Context, topicName string,
) (int32, error) {
	// If the topic is not in the cache, we try to get the metadata of the topic.
	// ignoreTopicErr is set to true to ignore the error if the topic is not found,
	// which means we should create the topic later.
	topicDetails, err := m.admin.GetTopicsMeta([]string{topicName}, true)
	if err != nil {
		if kafka.IsAuthorizationFailed(err) {
			return m.useConfiguredPartitionNum(topicName, err), nil
		}
		return 0, err
	}
	if numPartition, ok := m.tryStoreTopicMeta(topicName, topicDetails); ok {
		return numPartition, nil
	}

	topicDetails, err = m.admin.GetTopicsMeta([]string{topicName}, false)
	if err != nil {
		if kafka.IsAuthorizationFailed(err) {
			return m.useConfiguredPartitionNum(topicName, err), nil
		}
	} else if numPartition, ok := m.tryStoreTopicMeta(topicName, topicDetails); ok {
		return numPartition, nil
	}

	start := time.Now()
	partitionNum, err := m.createTopic(ctx, topicName)
	if err != nil {
		if kafka.IsAuthorizationFailed(err) {
			return m.useConfiguredPartitionNum(topicName, err), nil
		}
		return 0, err
	}

	err = m.waitUntilTopicVisible(ctx, topicName)
	if err != nil {
		return 0, err
	}

	log.Info(
		"kafka topic created",
		zap.String("keyspace", m.changefeedID.Keyspace()),
		zap.String("changefeed", m.changefeedID.Name()),
		zap.String("topic", topicName),
		zap.Int32("partitionNum", partitionNum),
		zap.Int16("replicationFactor", m.cfg.ReplicationFactor),
		zap.Duration("duration", time.Since(start)),
	)

	return partitionNum, nil
}

func (m *kafkaTopicManager) tryStoreTopicMeta(
	topicName string, topicDetails map[string]kafka.TopicDetail,
) (int32, bool) {
	detail, ok := topicDetails[topicName]
	if !ok {
		return 0, false
	}
	numPartition := detail.NumPartitions
	if topicName == m.defaultTopic {
		numPartition = m.cfg.PartitionNum
	}
	m.tryUpdatePartitionsAndLogging(topicName, numPartition)
	return numPartition, true
}

func (m *kafkaTopicManager) useConfiguredPartitionNum(topicName string, cause error) int32 {
	log.Warn("kafka topic creation skipped due to authorization failure",
		zap.String("keyspace", m.changefeedID.Keyspace()),
		zap.String("changefeed", m.changefeedID.Name()),
		zap.String("topic", topicName),
		zap.Int32("partitionNum", m.cfg.PartitionNum),
		zap.Error(cause))
	m.tryUpdatePartitionsAndLogging(topicName, m.cfg.PartitionNum)
	return m.cfg.PartitionNum
}

// Close exits the background goroutine.
func (m *kafkaTopicManager) Close() {
	m.cancel()
}
