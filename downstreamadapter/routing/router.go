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
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/filter"
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
	rules []rule
}

// rule represents a single routing rule.
type rule struct {
	filter     tfilter.Filter
	schemaExpr string
	tableExpr  string
}

// NewRouter creates a new Router from dispatch rules.
// When multiple rules match the same schema/table, the first matching rule wins.
func NewRouter(caseSensitive bool, rules []*config.DispatchRule) (Router, error) {
	routingRules := make([]rule, 0, len(rules))
	for _, r := range rules {
		if r.TargetSchema == "" && r.TargetTable == "" {
			continue
		}

		f, err := tfilter.Parse(r.Matcher)
		if err != nil {
			log.Warn("router failed to initialize", zap.Strings("matcher", r.Matcher), zap.Error(err))
			return Router{}, errors.WrapError(errors.ErrInvalidTableRoutingRule, err)
		}
		if !caseSensitive {
			f = tfilter.CaseInsensitive(f)
		}

		routingRules = append(routingRules, rule{
			filter:     f,
			schemaExpr: r.TargetSchema,
			tableExpr:  r.TargetTable,
		})
	}

	return Router{rules: routingRules}, nil
}

// Route returns the target schema and table names for the given source schema/table.
// When multiple rules match the same schema/table, the first matching rule wins.
func (r Router) Route(originSchema, originTable string) (targetSchema, targetTable string) {
	// Empty schema can appear on ExtraSchemaName/ExtraTableName for non-rename DDLs.
	// Keep it unchanged and skip table route matching.
	if len(r.rules) == 0 || originSchema == "" {
		return originSchema, originTable
	}

	rule := r.matchRule(originSchema, originTable)
	if rule == nil {
		return originSchema, originTable
	}

	targetSchema = substituteExpression(rule.schemaExpr, originSchema, originTable, originSchema)
	// Schema-level DDLs can rewrite the target schema, but must not synthesize a target table.
	if originTable == "" {
		return targetSchema, ""
	}

	targetTable = substituteExpression(rule.tableExpr, originSchema, originTable, originTable)
	return targetSchema, targetTable
}

// ApplyToTableInfo returns the original TableInfo unless routing changes the target name.
// When routing changes the target, it clones the TableInfo so the caller can safely reuse
// routed metadata without mutating the shared source TableInfo.
func (r Router) ApplyToTableInfo(tableInfo *common.TableInfo) *common.TableInfo {
	if len(r.rules) == 0 || tableInfo == nil {
		return tableInfo
	}

	originSchema := tableInfo.TableName.Schema
	originTable := tableInfo.TableName.Table
	targetSchema, targetTable := r.Route(originSchema, originTable)
	if targetSchema == originSchema && targetTable == originTable {
		return tableInfo
	}

	return tableInfo.CloneWithRouting(targetSchema, targetTable)
}

// ApplyToDDLEvent returns the original DDL event unless routing changes the query or related
// table metadata. When routing applies, it builds a routed DDL event once.
func (r Router) ApplyToDDLEvent(ddl *commonEvent.DDLEvent, changefeedID common.ChangeFeedID) (*commonEvent.DDLEvent, error) {
	if len(r.rules) == 0 || ddl == nil {
		return ddl, nil
	}

	originSchema := ddl.GetSchemaName()
	originTable := ddl.GetTableName()
	originExtraSchema := ddl.GetExtraSchemaName()
	originExtraTable := ddl.GetExtraTableName()

	query, err := rewriteDDLQueryWithRouting(r, ddl, changefeedID)
	if err != nil {
		return nil, err
	}

	targetSchemaName, targetTableName := r.Route(originSchema, originTable)
	targetExtraSchemaName, targetExtraTableName := r.Route(originExtraSchema, originExtraTable)
	tableInfoCache := make(map[*common.TableInfo]*common.TableInfo, 1)
	tableInfo := routeTableInfoWithCache(r, ddl.TableInfo, tableInfoCache)
	multipleTableInfos := applyToMultipleTableInfos(r, ddl.MultipleTableInfos, tableInfoCache)
	blockedTableNames := applyToBlockedTableNames(r, ddl.BlockedTableNames)
	if query == ddl.Query &&
		targetSchemaName == originSchema &&
		targetTableName == originTable &&
		targetExtraSchemaName == originExtraSchema &&
		targetExtraTableName == originExtraTable &&
		tableInfo == ddl.TableInfo &&
		multipleTableInfos == nil &&
		blockedTableNames == nil {
		return ddl, nil
	}

	if multipleTableInfos == nil {
		multipleTableInfos = ddl.MultipleTableInfos
	}
	if blockedTableNames == nil {
		blockedTableNames = ddl.BlockedTableNames
	}

	return commonEvent.NewRoutedDDLEvent(
		ddl,
		query,
		targetSchemaName,
		targetTableName,
		targetExtraSchemaName,
		targetExtraTableName,
		tableInfo,
		multipleTableInfos,
		blockedTableNames,
	), nil
}

// rewriteDDLQueryWithRouting rewrites a DDL query by applying routing rules
// to transform source table names to target table names.
//
// It returns the final query string.
// Canonical DDL schema/table fields are rewritten separately by ApplyToDDLEvent.
func rewriteDDLQueryWithRouting(
	router Router, ddl *commonEvent.DDLEvent, changefeedID common.ChangeFeedID,
) (string, error) {
	if len(router.rules) == 0 || ddl.Query == "" {
		return ddl.Query, nil
	}

	defaultSchema := ""
	if ddl.TableInfo != nil {
		defaultSchema = ddl.TableInfo.GetSchemaName()
	}

	if len(ddl.MultipleTableInfos) > 1 && strings.Contains(ddl.Query, ";") {
		queries, err := commonEvent.SplitQueries(ddl.Query)
		if err != nil {
			log.Error("rewrite ddl failed due to split ddl queries",
				zap.String("keyspace", changefeedID.Keyspace()),
				zap.String("changefeed", changefeedID.Name()),
				zap.String("query", ddl.Query), zap.Error(err))
			return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
		}
		if len(queries) > 1 {
			defaultSchemas := make([]string, len(queries))
			for i := range defaultSchemas {
				defaultSchemas[i] = defaultSchema
			}
			if len(ddl.MultipleTableInfos) == len(queries) {
				for i, info := range ddl.MultipleTableInfos {
					if info != nil {
						defaultSchemas[i] = info.GetSchemaName()
					}
				}
			}

			var (
				builder strings.Builder
				routed  bool
			)
			for i, query := range queries {
				newQuery, changed, err := rewriteSingleDDLQueryWithRouting(
					router, timodel.ActionType(ddl.Type), query, defaultSchemas[i], changefeedID,
				)
				if err != nil {
					return "", err
				}
				if changed {
					routed = true
				}
				builder.WriteString(newQuery)
				if changed && !strings.HasSuffix(newQuery, ";") {
					builder.WriteByte(';')
				}
			}
			if routed {
				newQuery := builder.String()
				log.Info("DDL query rewritten with routing",
					zap.String("keyspace", changefeedID.Keyspace()),
					zap.String("changefeed", changefeedID.Name()),
					zap.String("originalQuery", ddl.Query),
					zap.String("newQuery", newQuery))
				return newQuery, nil
			}
			return ddl.Query, nil
		}
	}

	newQuery, _, err := rewriteSingleDDLQueryWithRouting(
		router, timodel.ActionType(ddl.Type), ddl.Query, defaultSchema, changefeedID,
	)
	return newQuery, err
}

func rewriteSingleDDLQueryWithRouting(
	router Router, actionType timodel.ActionType, query string, defaultSchema string, changefeedID common.ChangeFeedID,
) (string, bool, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		if fallbackQuery, changed, ok := rewriteDDLQueryWithoutParser(router, actionType, query, defaultSchema); ok {
			return fallbackQuery, changed, nil
		}
		log.Error("rewrite ddl failed due to parse ddl query",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", query), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	sourceTables, err := fetchDDLTables(defaultSchema, stmt)
	if err != nil {
		log.Error("rewrite ddl failed due to fetch ddl tables",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", query), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	if len(sourceTables) == 0 {
		return query, false, nil
	}

	var (
		routed       bool
		targetTables = make([]*filter.Table, 0, len(sourceTables))
	)
	for _, srcTable := range sourceTables {
		targetSchema, targetTable := router.Route(srcTable.Schema, srcTable.Name)
		if targetSchema != srcTable.Schema || targetTable != srcTable.Name {
			routed = true
		}
		targetTables = append(targetTables, &filter.Table{
			Schema: targetSchema,
			Name:   targetTable,
		})
	}

	if !routed {
		return query, false, nil
	}

	newQuery, err := rewriteDDLQuery(stmt, targetTables)
	if err != nil {
		log.Error("rewrite ddl failed due to rewrite ddl query",
			zap.String("keyspace", changefeedID.Keyspace()),
			zap.String("changefeed", changefeedID.Name()),
			zap.String("query", query), zap.Any("targetTables", targetTables), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	return newQuery, true, nil
}

func rewriteDDLQueryWithoutParser(
	router Router, actionType timodel.ActionType, query string, defaultSchema string,
) (string, bool, bool) {
	switch actionType {
	case cdcfilter.ActionCreateHybridIndex:
		trimmedQuery := strings.TrimSpace(query)
		upperQuery := strings.ToUpper(trimmedQuery)
		onIndex := strings.Index(upperQuery, " ON ")
		if !strings.HasPrefix(upperQuery, "CREATE HYBRID INDEX ") || onIndex == -1 {
			return "", false, false
		}
		prefix := trimmedQuery[:onIndex+4]
		rest := trimmedQuery[onIndex+4:]
		openParenIndex := strings.Index(rest, "(")
		if openParenIndex == -1 {
			return "", false, false
		}
		tableExpr := strings.TrimSpace(rest[:openParenIndex])
		suffix := rest[openParenIndex:]

		schema, table, ok := splitQualifiedIdentifier(tableExpr, defaultSchema)
		if !ok {
			return "", false, false
		}
		targetSchema, targetTable := router.Route(schema, table)
		if targetSchema == schema && targetTable == table {
			return query, false, true
		}

		var rewrittenTable string
		if targetSchema == "" {
			rewrittenTable = quoteIdentifier(targetTable)
		} else {
			rewrittenTable = quoteIdentifier(targetSchema) + "." + quoteIdentifier(targetTable)
		}
		return prefix + rewrittenTable + suffix, true, true
	default:
		return "", false, false
	}
}

func splitQualifiedIdentifier(name string, defaultSchema string) (schema string, table string, ok bool) {
	parts := make([]string, 0, 2)
	last := 0
	inBackticks := false
	for i := 0; i < len(name); i++ {
		switch name[i] {
		case '`':
			inBackticks = !inBackticks
		case '.':
			if !inBackticks {
				parts = append(parts, strings.TrimSpace(name[last:i]))
				last = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(name[last:]))

	switch len(parts) {
	case 1:
		return defaultSchema, unquoteIdentifier(parts[0]), true
	case 2:
		return unquoteIdentifier(parts[0]), unquoteIdentifier(parts[1]), true
	default:
		return "", "", false
	}
}

func unquoteIdentifier(ident string) string {
	if len(ident) >= 2 && ident[0] == '`' && ident[len(ident)-1] == '`' {
		return strings.ReplaceAll(ident[1:len(ident)-1], "``", "`")
	}
	return ident
}

func quoteIdentifier(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
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

func routeTableInfoWithCache(
	r Router, tableInfo *common.TableInfo, cache map[*common.TableInfo]*common.TableInfo,
) *common.TableInfo {
	if tableInfo == nil {
		return nil
	}
	if routedTableInfo, ok := cache[tableInfo]; ok {
		return routedTableInfo
	}
	routedTableInfo := r.ApplyToTableInfo(tableInfo)
	cache[tableInfo] = routedTableInfo
	return routedTableInfo
}

// applyToMultipleTableInfos returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original references and the source event slice is left untouched.
func applyToMultipleTableInfos(
	r Router, tableInfos []*common.TableInfo, cache map[*common.TableInfo]*common.TableInfo,
) []*common.TableInfo {
	if len(tableInfos) == 0 {
		return nil
	}

	var routedTableInfos []*common.TableInfo
	for i, tableInfo := range tableInfos {
		routedTableInfo := routeTableInfoWithCache(r, tableInfo, cache)
		if routedTableInfo == tableInfo {
			continue
		}
		if routedTableInfos == nil {
			routedTableInfos = append([]*common.TableInfo(nil), tableInfos...)
		}
		routedTableInfos[i] = routedTableInfo
	}
	return routedTableInfos
}

// applyToBlockedTableNames returns nil when no entry changes.
// On the first routed entry, it clones the original slice once and only rewrites changed items,
// so unchanged entries keep their original values and the source event slice is left untouched.
func applyToBlockedTableNames(r Router, tableNames []commonEvent.SchemaTableName) []commonEvent.SchemaTableName {
	if len(tableNames) == 0 {
		return nil
	}

	var routedTableNames []commonEvent.SchemaTableName
	for i, tableName := range tableNames {
		targetSchema, targetTable := r.Route(tableName.SchemaName, tableName.TableName)
		if targetSchema == tableName.SchemaName && targetTable == tableName.TableName {
			continue
		}
		if routedTableNames == nil {
			routedTableNames = append([]commonEvent.SchemaTableName(nil), tableNames...)
		}
		routedTableNames[i] = commonEvent.SchemaTableName{
			SchemaName: targetSchema,
			TableName:  targetTable,
		}
	}
	return routedTableNames
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
