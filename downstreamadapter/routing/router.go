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
// When multiple rules match the same schema/table, the first matching rule wins.
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
func (r Router) ApplyToTableInfo(tableInfo *common.TableInfo) *common.TableInfo {
	if len(r.rules) == 0 || tableInfo == nil {
		return tableInfo
	}

	targetSchema, targetTable, changed := r.route(tableInfo.GetSchemaName(), tableInfo.GetTableName())
	if !changed {
		return tableInfo
	}
	return tableInfo.CloneWithRouting(targetSchema, targetTable)
}

// ApplyToDDLEvent returns the original DDL event unless routing changes the DDL query;
// when query changes, it also routes related metadata.
func (r Router) ApplyToDDLEvent(ddl *commonEvent.DDLEvent) (*commonEvent.DDLEvent, error) {
	if len(r.rules) == 0 || ddl == nil {
		return ddl, nil
	}

	targetSchemaName, targetTableName, nameChanged := r.route(ddl.GetSchemaName(), ddl.GetTableName())

	var err error
	newQuery := ddl.Query
	switch model.ActionType(ddl.Type) {
	case cdcfilter.ActionAddFullTextIndex:
		if nameChanged {
			newQuery, err = r.rewriteAddFullTextIndexQuery(ddl.Query, targetSchemaName, targetTableName)
		}
	case cdcfilter.ActionCreateHybridIndex:
		if nameChanged {
			newQuery, err = r.rewriteCreateHybridIndexQuery(ddl.Query, targetSchemaName, targetTableName)
		}
	default:
		newQuery, err = r.rewriteParserBackedDDLQuery(ddl)
	}
	if err != nil {
		return nil, err
	}

	if newQuery == ddl.Query {
		return ddl, nil
	}

	log.Info("ddl query rewritten with routing",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("originalQuery", ddl.Query),
		zap.String("newQuery", newQuery))

	targetExtraSchemaName, targetExtraTableName, _ := r.route(ddl.GetExtraSchemaName(), ddl.GetExtraTableName())

	tableInfo := r.ApplyToTableInfo(ddl.TableInfo)
	multipleTableInfos := r.applyToMultipleTableInfos(ddl.MultipleTableInfos)
	blockedTableNames := r.applyToBlockedTableNames(ddl.BlockedTableNames)

	if multipleTableInfos == nil {
		multipleTableInfos = ddl.MultipleTableInfos
	}
	if blockedTableNames == nil {
		blockedTableNames = ddl.BlockedTableNames
	}

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
func (r Router) route(originSchema, originTable string) (targetSchema, targetTable string, changed bool) {
	// In CDC runtime, table names should always carry schema.
	// Empty schema means this name pair is absent, so keep it unchanged.
	// This also prevents wildcard rules like *.* from matching it.
	if originSchema == "" {
		return originSchema, originTable, false
	}

	rule := r.matchRule(originSchema, originTable)
	if rule == nil {
		return originSchema, originTable, false
	}

	targetSchema = substituteExpression(rule.targetSchemaExpr, originSchema, originTable, originSchema)
	// Schema-level DDLs can rewrite the target schema, but must not synthesize a target table.
	if originTable == "" {
		return targetSchema, "", targetSchema != originSchema
	}

	targetTable = substituteExpression(rule.targetTableExpr, originSchema, originTable, originTable)
	return targetSchema, targetTable, targetSchema != originSchema || targetTable != originTable
}

// matchRule finds the first rule that matches the given schema/table.
// Schema-only routing uses MatchSchema so schema DDLs follow the same first-match
// semantics as table DDLs and DMLs.
func (r Router) matchRule(schema, table string) *rule {
	for i := range r.rules {
		if table == "" {
			if r.rules[i].filter.MatchSchema(schema) {
				return &r.rules[i]
			}
			continue
		}
		if r.rules[i].filter.MatchTable(schema, table) {
			return &r.rules[i]
		}
	}
	return nil
}

// applyToMultipleTableInfos returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original references and the source event slice is left untouched.
func (r Router) applyToMultipleTableInfos(tableInfos []*common.TableInfo) []*common.TableInfo {
	if len(tableInfos) == 0 {
		return nil
	}

	var result []*common.TableInfo
	for i, tableInfo := range tableInfos {
		routedTableInfo := r.ApplyToTableInfo(tableInfo)
		if routedTableInfo == tableInfo {
			continue
		}
		if result == nil {
			result = append([]*common.TableInfo(nil), tableInfos...)
		}
		result[i] = routedTableInfo
	}
	return result
}

// applyToBlockedTableNames returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original values and the source event slice is left untouched.
func (r Router) applyToBlockedTableNames(tableNames []commonEvent.SchemaTableName) []commonEvent.SchemaTableName {
	if len(tableNames) == 0 {
		return nil
	}

	var result []commonEvent.SchemaTableName
	for i, tableName := range tableNames {
		targetSchema, targetTable, changed := r.route(tableName.SchemaName, tableName.TableName)
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
	return result
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
