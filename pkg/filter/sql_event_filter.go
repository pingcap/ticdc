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

package filter

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

const (
	// binlogFilterSchemaPlaceholder is a place holder for schema name in binlog filter.
	// Since we use table filter in rule as a matcher to match a dml/ddl event's schema and table,
	// so we don't need to care about schema name when we calling binlog filter's method,
	// we just use this place holder to call binlog filter's method whenever we need pass a schema.
	binlogFilterSchemaPlaceholder = "binlogFilterSchema"
	// binlogFilterTablePlaceholder is a place holder for table name in binlog filter.
	// The reason we need it is the same as binlogFilterSchemaPlaceholder.
	binlogFilterTablePlaceholder = "binlogFilterTable"
	// dmlQuery is a place holder to call binlog filter to filter dml event.
	dmlQuery = ""
)

// sqlEventRule only be used by sqlEventFilter.
type sqlEventRule struct {
	// we use table filter to match a dml/ddl event's schema and table.
	// since binlog filter does not support syntax like `!test.t1`,
	// which means not match `test.t1`.
	tf tfilter.Filter
	bf *bf.BinlogEvent
}

func newSQLEventFilterRule(cfg *config.EventFilterRule, caseSensitive bool) (*sqlEventRule, error) {
	tf, err := tfilter.Parse(cfg.Matcher)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, cfg.Matcher)
	}

	if !caseSensitive {
		tf = tfilter.CaseInsensitive(tf)
	}
	res := &sqlEventRule{
		tf: tf,
	}

	if err := verifyIgnoreEvents(cfg.IgnoreEvent); err != nil {
		return nil, err
	}

	bfRule := &bf.BinlogEventRule{
		SchemaPattern: binlogFilterSchemaPlaceholder,
		TablePattern:  binlogFilterTablePlaceholder,
		Events:        cfg.IgnoreEvent,
		SQLPattern:    cfg.IgnoreSQL,
		Action:        bf.Ignore,
	}

	res.bf, err = bf.NewBinlogEvent(caseSensitive, []*bf.BinlogEventRule{bfRule})
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrFilterRuleInvalid, err, "failed to create binlog event filter")
	}

	return res, nil
}

func verifyIgnoreEvents(types []bf.EventType) error {
	typesMap := make(map[bf.EventType]struct{}, len(SupportedEventTypes()))
	for _, et := range SupportedEventTypes() {
		typesMap[et] = struct{}{}
	}
	for _, et := range types {
		if _, ok := typesMap[et]; !ok {
			return cerror.ErrInvalidIgnoreEventType.GenWithStackByArgs(string(et))
		}
	}
	return nil
}

// sqlEventFilter is a filter that filters DDL/DML event by its type or query.
type sqlEventFilter struct {
	rules []*sqlEventRule
}

func newSQLEventFilter(cfg *config.FilterConfig, caseSensitive bool) (*sqlEventFilter, error) {
	res := &sqlEventFilter{}
	for _, rule := range cfg.EventFilters {
		if err := res.addRule(rule, caseSensitive); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return res, nil
}

func (f *sqlEventFilter) addRule(cfg *config.EventFilterRule, caseSensitive bool) error {
	rule, err := newSQLEventFilterRule(cfg, caseSensitive)
	if err != nil {
		return errors.Trace(err)
	}
	f.rules = append(f.rules, rule)
	return nil
}

func (f *sqlEventFilter) getRules(schema, table string) []*sqlEventRule {
	res := make([]*sqlEventRule, 0)
	for _, rule := range f.rules {
		if len(table) == 0 {
			if rule.tf.MatchSchema(schema) {
				res = append(res, rule)
			}
		} else {
			if rule.tf.MatchTable(schema, table) {
				res = append(res, rule)
			}
		}
	}
	return res
}

// skipDDLEvent skips ddl event by its type and query.
func (f *sqlEventFilter) shouldSkipDDL(schema, table, query string, ddlType model.ActionType) (skip bool, err error) {
	if len(f.rules) == 0 {
		return false, nil
	}
	evenType := ddlToEventType(ddlType)
	if evenType == bf.NullEvent {
		log.Warn("sql event filter unsupported ddl type, do nothing",
			zap.String("type", ddlType.String()),
			zap.String("query", query))
		return false, nil
	}

	rules := f.getRules(schema, table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(
			binlogFilterSchemaPlaceholder,
			binlogFilterTablePlaceholder,
			evenType, query)
		if err != nil {
			return false, errors.Trace(err)
		}
		if action == bf.Ignore {
			return true, nil
		}

		// If the ddl is alter table's subtype,
		// we need try to filter it by bf.AlterTable.
		if isAlterTable(ddlType) {
			action, err = rule.bf.Filter(
				binlogFilterSchemaPlaceholder,
				binlogFilterTablePlaceholder,
				bf.AlterTable, query)
			if err != nil {
				return false, errors.Trace(err)
			}
			if action == bf.Ignore {
				return true, nil
			}
		}
	}
	return false, nil
}

// shouldSkipDML skips dml event by its type.
func (f *sqlEventFilter) shouldSkipDML(schema, table string, dmlType common.RowType) (bool, error) {
	if len(f.rules) == 0 {
		return false, nil
	}

	var et bf.EventType
	switch dmlType {
	case common.RowTypeInsert:
		et = bf.InsertEvent
	case common.RowTypeUpdate:
		et = bf.UpdateEvent
	case common.RowTypeDelete:
		et = bf.DeleteEvent
	default:
		// It should never happen.
		log.Warn("unknown row changed event type")
		return false, nil
	}
	rules := f.getRules(schema, table)
	for _, rule := range rules {
		action, err := rule.bf.Filter(binlogFilterSchemaPlaceholder, binlogFilterTablePlaceholder, et, dmlQuery)
		if err != nil {
			return false, cerror.WrapError(cerror.ErrFailedToFilterDML, err, fmt.Sprintf("%s.%s, %d", schema, table, dmlType))
		}
		if action == bf.Ignore {
			return true, nil
		}
	}
	return false, nil
}
