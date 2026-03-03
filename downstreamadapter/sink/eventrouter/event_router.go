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
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/topic"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	tableFilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

type Rule struct {
	partitionDispatcher partition.Generator
	topicGenerator      topic.Generator
	tableFilter.Filter
}

// EventRouter is a router, it determines which topic and which partition
// an event should be dispatched to.
type EventRouter struct {
	defaultTopic          string
	rules                 []Rule
	outboxRequiredColumns []string
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
		if !util.GetOrZero(sinkConfig.CaseSensitive) {
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

	var outboxRequiredColumns []string
	protocol, _ := config.ParseSinkProtocolFromString(util.GetOrZero(sinkConfig.Protocol))
	if protocol == config.ProtocolOutboxJSON && sinkConfig.Outbox != nil {
		outboxRequiredColumns = []string{
			sinkConfig.Outbox.IDColumn,
			sinkConfig.Outbox.KeyColumn,
			sinkConfig.Outbox.ValueColumn,
		}
		for _, column := range sinkConfig.Outbox.HeaderColumns {
			outboxRequiredColumns = append(outboxRequiredColumns, column)
		}
	}

	return &EventRouter{
		defaultTopic:          defaultTopic,
		rules:                 rules,
		outboxRequiredColumns: outboxRequiredColumns,
	}, nil
}

// GetTopicForRowChange returns the target topic for a row change after applying
// any row-aware topic placeholders.
func (s *EventRouter) GetTopicForRowChange(row *commonEvent.RowEvent) (string, error) {
	schema := row.TableInfo.GetSchemaName()
	table := row.TableInfo.GetTableName()
	topicGenerator := s.matchTopicGenerator(schema, table)
	if !topicGenerator.UsesColumnPlaceholders() {
		return topicGenerator.Substitute(schema, table), nil
	}

	columnValues, err := s.extractColumnValuesForTopic(row, topicGenerator.ReferencedColumns())
	if err != nil {
		return "", err
	}
	return topicGenerator.SubstituteWithValues(schema, table, columnValues)
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
	if topicGenerator.UsesColumnPlaceholders() {
		log.Warn("column based topic rule falls back to default topic for ddl",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.String("defaultTopic", s.defaultTopic))
		return s.defaultTopic
	}
	return topicGenerator.Substitute(schema, table)
}

// GetActiveTopics returns a list of the corresponding topics
// for the tables that are actively synchronized.
func (s *EventRouter) GetActiveTopics(activeTables []*commonEvent.SchemaTableName) []string {
	topics := make([]string, 0, len(activeTables))
	topicsMap := make(map[string]bool, len(activeTables))
	for _, tableName := range activeTables {
		topicDispatcher := s.matchTopicGenerator(tableName.SchemaName, tableName.TableName)
		// Row dependent topics are runtime discovered and are not statically derivable here.
		if topicDispatcher.UsesColumnPlaceholders() {
			continue
		}
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

// GetTopicDispatchColumns returns all row column names referenced by topic
// placeholders in the matched dispatch rule for the given table.
func (s *EventRouter) GetTopicDispatchColumns(schema, table string) []string {
	return s.matchTopicGenerator(schema, table).ReferencedColumns()
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

		// Verify topic dispatch column placeholders are valid and CDC visible.
		topicColumns := s.GetTopicDispatchColumns(table.TableName.Schema, table.TableName.Table)
		if len(topicColumns) > 0 {
			if _, err := table.OffsetsByNames(topicColumns); err != nil {
				return err
			}
		}

		if len(s.outboxRequiredColumns) > 0 {
			if _, err := table.OffsetsByNames(s.outboxRequiredColumns); err != nil {
				return err
			}
		}
	}
	return nil
}

// extractColumnValuesForTopic collects the row values needed to resolve column
// placeholders in a topic expression.
func (s *EventRouter) extractColumnValuesForTopic(
	row *commonEvent.RowEvent, columns []string,
) (map[string]string, error) {
	if len(columns) == 0 {
		return map[string]string{}, nil
	}

	values := make(map[string]string, len(columns))
	selectedRow := row.GetRows()
	if row.IsDelete() {
		selectedRow = row.GetPreRows()
	}
	for _, colName := range columns {
		offset, ok := row.TableInfo.GetColumnOffsetByName(colName)
		if !ok {
			return nil, cerror.ErrDispatcherFailed.GenWithStack(
				"topic dispatch column not found, table: %s, column: %s", row.TableInfo.TableName, colName)
		}
		columnInfo := row.TableInfo.GetColumns()[offset]
		value := common.ExtractColVal(selectedRow, columnInfo, offset)
		if value == nil {
			return nil, cerror.ErrDispatcherFailed.GenWithStack(
				"topic dispatch column value is null, table: %s, column: %s", row.TableInfo.TableName, colName)
		}

		stringValue := ""
		switch v := value.(type) {
		case []byte:
			stringValue = string(v)
		default:
			stringValue = fmt.Sprint(v)
		}
		if strings.TrimSpace(stringValue) == "" {
			return nil, cerror.ErrDispatcherFailed.GenWithStack(
				"topic dispatch column value is empty, table: %s, column: %s", row.TableInfo.TableName, colName)
		}
		values[strings.ToLower(colName)] = stringValue
	}
	return values, nil
}
