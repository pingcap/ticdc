// Copyright 2026 PingCAP, Inc.
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

package routing

import (
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	tfilter "github.com/pingcap/tidb/pkg/util/table-filter"
	"go.uber.org/zap"
)

// Routing expression placeholders that can be used in TargetSchema and TargetTable.
const (
	// SchemaPlaceholder is replaced with the source schema name in routing expressions.
	SchemaPlaceholder = "{schema}"
	// TablePlaceholder is replaced with the source table name in routing expressions.
	TablePlaceholder = "{table}"
)

// rule represents a single routing rule.
type rule struct {
	filter           tfilter.Filter
	targetSchemaExpr string
	targetTableExpr  string
}

// Router is used to support the table route functionality,
// which map the origin schema/table names to target schema/table names based on the given rules.
type Router struct {
	changefeedID common.ChangeFeedID
	rules        []rule
}

// NewRouter creates a new Router from dispatch rules.
// When multiple rules match the same table, the first matching rule wins.
func NewRouter(
	changefeedID common.ChangeFeedID, caseSensitive bool, rules []*config.DispatchRule,
) (Router, error) {
	routingRules := make([]rule, 0, len(rules))
	for _, r := range rules {
		if r.TargetSchema == "" && r.TargetTable == "" {
			continue
		}

		f, err := tfilter.Parse(r.Matcher)
		if err != nil {
			log.Warn("router failed to initialize",
				zap.String("keyspace", changefeedID.Keyspace()),
				zap.String("changefeed", changefeedID.Name()),
				zap.Strings("matcher", r.Matcher),
				zap.Error(err))
			return Router{}, errors.WrapError(errors.ErrInvalidTableRoutingRule, err)
		}
		if !caseSensitive {
			f = tfilter.CaseInsensitive(f)
		}

		routingRules = append(routingRules, rule{
			filter:           f,
			targetSchemaExpr: r.TargetSchema,
			targetTableExpr:  r.TargetTable,
		})
	}

	return Router{
		changefeedID: changefeedID,
		rules:        routingRules,
	}, nil
}

// ApplyToTableInfo returns the original TableInfo unless routing changes the target name.
// When routing changes the target, it clones the TableInfo so the caller can safely reuse
// routed metadata without mutating the shared source TableInfo.
func (r Router) ApplyToTableInfo(tableInfo *common.TableInfo) (*common.TableInfo, error) {
	if len(r.rules) == 0 || tableInfo == nil {
		return tableInfo, nil
	}

	targetSchema, targetTable, changed, err := r.route(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if err != nil {
		return nil, err
	}
	if !changed {
		return tableInfo, nil
	}
	return tableInfo.CloneWithRouting(targetSchema, targetTable), nil
}

// ApplyToDDLEvent returns the original DDL event unless routing changes the DDL query;
// when query changes, it also routes related metadata.
func (r Router) ApplyToDDLEvent(ddl *commonEvent.DDLEvent) (*commonEvent.DDLEvent, error) {
	if len(r.rules) == 0 || ddl == nil {
		return ddl, nil
	}

	switch model.ActionType(ddl.Type) {
	case cdcfilter.ActionAddFullTextIndex, cdcfilter.ActionCreateHybridIndex:
		return nil, errors.ErrTableRoutingFailed.GenWithStack(
			"table routing does not support ddl type %d, query: %s", ddl.Type, ddl.Query)
	}

	targetSchemaName, targetTableName, _, err := r.route(ddl.GetSchemaName(), ddl.GetTableName())
	if err != nil {
		return nil, err
	}

	newQuery, err := r.rewriteParserBackedDDLQuery(ddl)
	if err != nil {
		return nil, err
	}

	// In CDC DDL events, routed DDL metadata should correspond to names in Query.
	// If Query is unchanged, no routed DDL event is needed.
	if newQuery == ddl.Query {
		return ddl, nil
	}

	targetExtraSchemaName, targetExtraTableName, _, err := r.route(ddl.GetExtraSchemaName(), ddl.GetExtraTableName())
	if err != nil {
		return nil, err
	}

	tableInfo, err := r.ApplyToTableInfo(ddl.TableInfo)
	if err != nil {
		return nil, err
	}
	multipleTableInfos, err := r.applyToMultipleTableInfos(ddl.MultipleTableInfos)
	if err != nil {
		return nil, err
	}
	blockedTableNames, err := r.applyToBlockedTableNames(ddl.BlockedTableNames)
	if err != nil {
		return nil, err
	}

	if multipleTableInfos == nil {
		multipleTableInfos = ddl.MultipleTableInfos
	}
	if blockedTableNames == nil {
		blockedTableNames = ddl.BlockedTableNames
	}

	log.Info("ddl query rewritten with routing",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("originalQuery", ddl.Query),
		zap.String("newQuery", newQuery))

	return commonEvent.NewRoutedDDLEvent(
		ddl,
		newQuery,
		targetSchemaName,
		targetTableName,
		targetExtraSchemaName,
		targetExtraTableName,
		tableInfo,
		multipleTableInfos,
		blockedTableNames,
	), nil
}

// route returns the target schema/table names and whether routing changed them.
func (r Router) route(originSchema, originTable string) (targetSchema, targetTable string, changed bool, err error) {
	// In CDC runtime, table names should always carry schema.
	// Empty schema means this name pair is absent, so keep it unchanged.
	// This also prevents wildcard rules like *.* from matching it.
	if originSchema == "" {
		return originSchema, originTable, false, nil
	}

	rule, err := r.matchRule(originSchema, originTable)
	if err != nil {
		return "", "", false, err
	}
	if rule == nil {
		return originSchema, originTable, false, nil
	}

	targetSchema = substituteExpression(rule.targetSchemaExpr, originSchema, originTable, originSchema)
	// Schema-level DDLs can rewrite the target schema, but must not synthesize a target table.
	if originTable == "" {
		return targetSchema, "", targetSchema != originSchema, nil
	}

	targetTable = substituteExpression(rule.targetTableExpr, originSchema, originTable, originTable)
	return targetSchema, targetTable, targetSchema != originSchema || targetTable != originTable, nil
}

// matchRule finds the first rule that matches the given schema/table.
// Table routing keeps first-match semantics. Schema-only routing rejects fan-out
// to different target schemas because there is no table name to choose a rule.
func (r Router) matchRule(schema, table string) (*rule, error) {
	if table == "" {
		var (
			matched      *rule
			targetSchema string
		)
		for i := range r.rules {
			if !r.rules[i].filter.MatchSchema(schema) {
				continue
			}

			currentTargetSchema := substituteExpression(r.rules[i].targetSchemaExpr, schema, "", schema)
			if matched == nil {
				matched = &r.rules[i]
				targetSchema = currentTargetSchema
				continue
			}
			if currentTargetSchema != targetSchema {
				return nil, errors.ErrTableRoutingFailed.GenWithStack(
					"ambiguous schema routing for schema %s: target schema %s conflicts with %s",
					schema, currentTargetSchema, targetSchema)
			}
		}
		return matched, nil
	}

	for i := range r.rules {
		if r.rules[i].filter.MatchTable(schema, table) {
			return &r.rules[i], nil
		}
	}
	return nil, nil
}

// applyToMultipleTableInfos returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original references and the source event slice is left untouched.
func (r Router) applyToMultipleTableInfos(tableInfos []*common.TableInfo) ([]*common.TableInfo, error) {
	if len(tableInfos) == 0 {
		return nil, nil
	}

	var result []*common.TableInfo
	for i, tableInfo := range tableInfos {
		routedTableInfo, err := r.ApplyToTableInfo(tableInfo)
		if err != nil {
			return nil, err
		}
		if routedTableInfo == tableInfo {
			continue
		}
		if result == nil {
			result = append([]*common.TableInfo(nil), tableInfos...)
		}
		result[i] = routedTableInfo
	}
	return result, nil
}

// applyToBlockedTableNames returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original values and the source event slice is left untouched.
func (r Router) applyToBlockedTableNames(tableNames []commonEvent.SchemaTableName) ([]commonEvent.SchemaTableName, error) {
	if len(tableNames) == 0 {
		return nil, nil
	}

	var result []commonEvent.SchemaTableName
	for i, tableName := range tableNames {
		targetSchema, targetTable, changed, err := r.route(tableName.SchemaName, tableName.TableName)
		if err != nil {
			return nil, err
		}
		if !changed {
			continue
		}
		if result == nil {
			result = append([]commonEvent.SchemaTableName(nil), tableNames...)
		}
		result[i] = commonEvent.SchemaTableName{
			SchemaName: targetSchema,
			TableName:  targetTable,
		}
	}
	return result, nil
}

// substituteExpression replaces {schema} and {table} placeholders with actual values.
// If expr is empty, returns defaultValue (typically sourceSchema for schema expressions,
// sourceTable for table expressions).
func substituteExpression(expr, sourceSchema, sourceTable, defaultValue string) string {
	if expr == "" {
		return defaultValue
	}

	result := expr
	result = strings.ReplaceAll(result, SchemaPlaceholder, sourceSchema)
	result = strings.ReplaceAll(result, TablePlaceholder, sourceTable)
	return result
}
