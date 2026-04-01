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

// Router routes source schema/table names to target schema/table names.
type Router struct {
	rules []*rule
}

// rule represents a single routing rule.
type rule struct {
	filter     tfilter.Filter
	schemaExpr string
	tableExpr  string
}

// NewRouter creates a new Router from dispatch rules.
// Returns nil if no routing rules are configured.
func NewRouter(caseSensitive bool, rules []*config.DispatchRule) (*Router, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	routingRules := make([]*rule, 0, len(rules))
	for _, r := range rules {
		if r.TargetSchema == "" && r.TargetTable == "" {
			continue
		}

		f, err := tfilter.Parse(r.Matcher)
		if err != nil {
			log.Warn("router failed to initialize", zap.Strings("matcher", r.Matcher), zap.Error(err))
			return nil, errors.WrapError(errors.ErrInvalidTableRoutingRule, err)
		}
		if !caseSensitive {
			f = tfilter.CaseInsensitive(f)
		}

		routingRules = append(routingRules, &rule{
			filter:     f,
			schemaExpr: r.TargetSchema,
			tableExpr:  r.TargetTable,
		})
	}

	if len(routingRules) == 0 {
		return nil, nil
	}

	return &Router{rules: routingRules}, nil
}

// Route returns the target schema and table names for the given source schema/table.
func (r *Router) Route(sourceSchema, sourceTable string) (targetSchema, targetTable string) {
	if r == nil || len(r.rules) == 0 {
		return sourceSchema, sourceTable
	}

	rule := r.matchRule(sourceSchema, sourceTable)
	if rule == nil {
		return sourceSchema, sourceTable
	}

	targetSchema = substituteExpression(rule.schemaExpr, sourceSchema, sourceTable, sourceSchema)
	targetTable = substituteExpression(rule.tableExpr, sourceSchema, sourceTable, sourceTable)
	return targetSchema, targetTable
}

// ApplyToTableInfo returns the original TableInfo unless routing changes the target name.
// When routing changes the target, it clones the TableInfo so the caller can safely reuse
// routed metadata without mutating the shared source TableInfo.
func (r *Router) ApplyToTableInfo(tableInfo *common.TableInfo) *common.TableInfo {
	if tableInfo == nil {
		return nil
	}

	sourceSchema := tableInfo.TableName.Schema
	sourceTable := tableInfo.TableName.Table
	targetSchema, targetTable := r.Route(sourceSchema, sourceTable)
	if targetSchema == sourceSchema && targetTable == sourceTable {
		return tableInfo
	}

	return tableInfo.CloneWithRouting(targetSchema, targetTable)
}

// ApplyToDDLEvent returns the original DDL event unless routing changes the query or related
// table metadata. When routing applies, it clones the DDL event once and rewrites all relevant
// routing-aware fields on the clone.
func (r *Router) ApplyToDDLEvent(ddl *commonEvent.DDLEvent, changefeedID common.ChangeFeedID) (*commonEvent.DDLEvent, error) {
	if ddl == nil {
		return nil, nil
	}

	newQuery, queryChanged, err := rewriteDDLQueryWithRouting(r, ddl, changefeedID)
	if err != nil {
		return nil, err
	}

	routedSchemaName, routedTableName, schemaTableChanged := routeSchemaTable(
		r, ddl.GetSchemaName(), ddl.GetTableName(),
	)
	routedExtraSchemaName, routedExtraTableName, extraSchemaTableChanged := routeSchemaTable(
		r, ddl.GetExtraSchemaName(), ddl.GetExtraTableName(),
	)
	routedTableInfo := r.ApplyToTableInfo(ddl.TableInfo)
	tableInfoChanged := routedTableInfo != ddl.TableInfo
	routedMultipleTableInfos, multipleTableInfosChanged := applyToMultipleTableInfos(r, ddl.MultipleTableInfos)
	routedBlockedTableNames, blockedTableNamesChanged := applyToBlockedTableNames(r, ddl.BlockedTableNames)

	if !queryChanged &&
		!schemaTableChanged &&
		!extraSchemaTableChanged &&
		!tableInfoChanged &&
		!multipleTableInfosChanged &&
		!blockedTableNamesChanged {
		return ddl, nil
	}

	cloned := ddl.CloneForRouting()
	if queryChanged {
		cloned.Query = newQuery
	}
	if schemaTableChanged {
		cloned.TargetSchemaName = routedSchemaName
		cloned.TargetTableName = routedTableName
	}
	if extraSchemaTableChanged {
		cloned.TargetExtraSchemaName = routedExtraSchemaName
		cloned.TargetExtraTableName = routedExtraTableName
	}
	if tableInfoChanged {
		cloned.TableInfo = routedTableInfo
	}
	if multipleTableInfosChanged {
		cloned.MultipleTableInfos = routedMultipleTableInfos
	}
	if blockedTableNamesChanged {
		cloned.BlockedTableNames = routedBlockedTableNames
	}

	return cloned, nil
}

// matchRule finds the first rule that matches the given schema/table.
func (r *Router) matchRule(schema, table string) *rule {
	for _, rule := range r.rules {
		if rule.filter.MatchTable(schema, table) {
			return rule
		}
	}
	return nil
}

func applyToMultipleTableInfos(r *Router, tableInfos []*common.TableInfo) ([]*common.TableInfo, bool) {
	if len(tableInfos) == 0 {
		return tableInfos, false
	}

	var (
		changed          bool
		routedTableInfos []*common.TableInfo
	)
	for i, tableInfo := range tableInfos {
		routedTableInfo := r.ApplyToTableInfo(tableInfo)
		if routedTableInfo != tableInfo {
			if !changed {
				routedTableInfos = append([]*common.TableInfo(nil), tableInfos...)
				changed = true
			}
			routedTableInfos[i] = routedTableInfo
		}
	}

	if !changed {
		return tableInfos, false
	}
	return routedTableInfos, true
}

func applyToBlockedTableNames(r *Router, tableNames []commonEvent.SchemaTableName) ([]commonEvent.SchemaTableName, bool) {
	if len(tableNames) == 0 {
		return tableNames, false
	}

	var (
		changed          bool
		routedTableNames []commonEvent.SchemaTableName
	)
	for i, tableName := range tableNames {
		targetSchema, targetTable := r.Route(tableName.SchemaName, tableName.TableName)
		if targetSchema != tableName.SchemaName || targetTable != tableName.TableName {
			if !changed {
				routedTableNames = append([]commonEvent.SchemaTableName(nil), tableNames...)
				changed = true
			}
			routedTableNames[i] = commonEvent.SchemaTableName{
				SchemaName: targetSchema,
				TableName:  targetTable,
			}
		}
	}

	if !changed {
		return tableNames, false
	}
	return routedTableNames, true
}

func routeSchemaOnly(r *Router, schema string) (string, bool) {
	if schema == "" {
		return "", false
	}
	targetSchema, _ := r.Route(schema, "")
	return targetSchema, targetSchema != schema
}

func routeSchemaTable(r *Router, schema, table string) (string, string, bool) {
	if schema == "" && table == "" {
		return "", "", false
	}
	if table == "" {
		targetSchema, changed := routeSchemaOnly(r, schema)
		return targetSchema, "", changed
	}
	targetSchema, targetTable := r.Route(schema, table)
	return targetSchema, targetTable, targetSchema != schema || targetTable != table
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
