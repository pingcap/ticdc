// Copyright 2024 PingCAP, Inc.
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

package eventrouter

import (
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/topic"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	tableFilter "github.com/pingcap/tidb/pkg/util/table-filter"
)

type Rule struct {
	partitionDispatcher partition.Generator
	topicGenerator      topic.Generator
	tableFilter.Filter
}

// EventRouter is a router, it determines which topic and which partition
// an event should be dispatched to.
type EventRouter struct {
	defaultTopic string
	rules        []Rule
}

// NewEventRouter creates a new EventRouter.
func NewEventRouter(
	sinkConfig *config.SinkConfig, defaultTopic string, isPulsar bool, isAvro bool,
) (*EventRouter, error) {
	// If an event does not match any dispatching rules in the config file,
	// it will be dispatched by the default partition dispatcher and
	// static topic dispatcher because it matches *.* rule.
	ruleConfigs := append(sinkConfig.DispatchRules, &config.DispatchRule{
		Matcher:       []string{"*.*"},
		PartitionRule: "default",
		TopicRule:     "",
	})

	rules := make([]Rule, 0, len(ruleConfigs))
	for _, ruleConfig := range ruleConfigs {
		f, err := tableFilter.Parse(ruleConfig.Matcher)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, ruleConfig.Matcher)
		}
		if !sinkConfig.CaseSensitive {
			f = tableFilter.CaseInsensitive(f)
		}
		d := partition.NewGenerator(ruleConfig.PartitionRule, isPulsar, ruleConfig.IndexName, ruleConfig.Columns)
		topicGenerator, err := topic.GetTopicGenerator(ruleConfig.TopicRule, defaultTopic, isPulsar, isAvro)
		if err != nil {
			return nil, err
		}
		rules = append(rules, Rule{
			partitionDispatcher: d,
			topicGenerator:      topicGenerator,
			Filter:              f,
		})
	}

	return &EventRouter{
		defaultTopic: defaultTopic,
		rules:        rules,
	}, nil
}

// GetTopicForRowChange returns the target topic for row changes.
func (s *EventRouter) GetTopicForRowChange(schema, table string) string {
	topicGenerator := s.matchTopicGenerator(schema, table)
	return topicGenerator.Substitute(schema, table)
}

// GetTopicForDDL returns the target topic for DDL.
func (s *EventRouter) GetTopicForDDL(ddl *commonEvent.DDLEvent) string {
	var schema, table string

	if ddl.GetExtraSchemaName() != "" {
		if ddl.GetExtraTableName() == "" {
			return s.defaultTopic
		}
		schema = ddl.GetExtraSchemaName()
		table = ddl.GetExtraTableName()
	} else {
		if ddl.GetTableName() == "" {
			return s.defaultTopic
		}
		schema = ddl.GetSchemaName()
		table = ddl.GetTableName()
	}

	topicGenerator := s.matchTopicGenerator(schema, table)
	return topicGenerator.Substitute(schema, table)
}

// GetActiveTopics returns a list of the corresponding topics
// for the tables that are actively synchronized.
func (s *EventRouter) GetActiveTopics(activeTables []*commonEvent.SchemaTableName) []string {
	topics := make([]string, 0, len(activeTables))
	topicsMap := make(map[string]bool, len(activeTables))
	for _, tableName := range activeTables {
		topicDispatcher := s.matchTopicGenerator(tableName.SchemaName, tableName.TableName)
		topicName := topicDispatcher.Substitute(tableName.SchemaName, tableName.TableName)
		if !topicsMap[topicName] {
			topicsMap[topicName] = true
			topics = append(topics, topicName)
		}
	}

	// We also need to add the default topic.
	if !topicsMap[s.defaultTopic] {
		topics = append(topics, s.defaultTopic)
	}

	return topics
}

// GetPartitionGenerator returns the target partition by the table information.
func (s *EventRouter) GetPartitionGenerator(schema, table string) partition.Generator {
	for _, rule := range s.rules {
		if rule.MatchTable(schema, table) {
			return rule.partitionDispatcher
		}
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}

// GetDefaultTopic returns the default topic name.
func (s *EventRouter) GetDefaultTopic() string {
	return s.defaultTopic
}

func (s *EventRouter) matchTopicGenerator(schema, table string) topic.Generator {
	for _, rule := range s.rules {
		if rule.MatchTable(schema, table) {
			return rule.topicGenerator
		}
	}
	log.Panic("the dispatch rule must cover all tables")
	return nil
}

// VerifyTables return error if any one table route rule is invalid.
func (s *EventRouter) VerifyTables(infos []*common.TableInfo) error {
	for _, table := range infos {
		partitionDispatcher := s.GetPartitionGenerator(table.TableName.Schema, table.TableName.Table)
		switch v := partitionDispatcher.(type) {
		case *partition.IndexValuePartitionGenerator:
			if v.IndexName != "" {
				index := table.GetIndex(v.IndexName)
				if index == nil {
					return cerror.ErrDispatcherFailed.GenWithStack(
						"index not found when verify the table, table: %v, index: %s", table.TableName, v.IndexName)
				}
				// only allow the unique index to be set.
				// For the non-unique index, if any column belongs to the index is updated,
				// the event is not split, it may cause incorrect data consumption.
				if !index.Unique {
					return cerror.ErrDispatcherFailed.GenWithStack(
						"index is not unique when verify the table, table: %v, index: %s", table.TableName, v.IndexName)
				}
			}
		case *partition.ColumnsPartitionGenerator:
			_, err := table.OffsetsByNames(v.Columns)
			if err != nil {
				return err
			}
		default:
		}
	}
	return nil
}
