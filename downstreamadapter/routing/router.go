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

// rule represents a single routing rule.
type rule struct {
	filter           tfilter.Filter
	targetSchemaExpr string
	targetTableExpr  string
}

// Router routes origin schema/table names to target schema/table names.
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

	originSchema := tableInfo.TableName.Schema
	originTable := tableInfo.TableName.Table
	targetSchema, targetTable, changed := r.route(originSchema, originTable)
	if !changed {
		return tableInfo
	}

	return tableInfo.CloneWithRouting(targetSchema, targetTable)
}

// ApplyToDDLEvent returns the original DDL event unless routing changes the query or related
// table metadata. When routing applies, it builds a routed DDL event once.
func (r Router) ApplyToDDLEvent(ddl *commonEvent.DDLEvent) (*commonEvent.DDLEvent, error) {
	if len(r.rules) == 0 || ddl == nil {
		return ddl, nil
	}

	originSchema := ddl.GetSchemaName()
	originTable := ddl.GetTableName()
	originExtraSchema := ddl.GetExtraSchemaName()
	originExtraTable := ddl.GetExtraTableName()

	targetSchemaName, targetTableName, nameChanged := r.route(originSchema, originTable)
	targetExtraSchemaName, targetExtraTableName, extraNameChanged := r.route(originExtraSchema, originExtraTable)

	tableInfo := r.ApplyToTableInfo(ddl.TableInfo)
	multipleTableInfos := r.applyToMultipleTableInfos(ddl.MultipleTableInfos)
	blockedTableNames := r.applyToBlockedTableNames(ddl.BlockedTableNames)

	structuredChanged := nameChanged ||
		extraNameChanged ||
		tableInfo != ddl.TableInfo ||
		multipleTableInfos != nil ||
		blockedTableNames != nil
	if isUnsupportedTableRoutingDDLAction(model.ActionType(ddl.Type)) {
		if structuredChanged {
			return nil, errors.ErrTableRoutingFailed.GenWithStack(
				"DDL type %d is not supported by table routing", ddl.Type)
		}
		return ddl, nil
	}

	query := ddl.Query
	shouldRewriteQuery := ddl.Query != "" &&
		(structuredChanged || ddlQueryMayContainAdditionalTableRefs(model.ActionType(ddl.Type)))
	if shouldRewriteQuery {
		var err error
		query, err = r.rewriteDDLQuery(ddl)
		if err != nil {
			return nil, err
		}
	}
	if query == ddl.Query && !structuredChanged {
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

// rewriteDDLQuery rewrites a DDL query by applying routing rules
// to transform source table names to target table names.
func (r Router) rewriteDDLQuery(ddl *commonEvent.DDLEvent) (string, error) {
	if len(r.rules) == 0 || ddl.Query == "" {
		return ddl.Query, nil
	}

	queries := []string{ddl.Query}
	if strings.Contains(ddl.Query, ";") {
		var err error
		queries, err = commonEvent.SplitQueries(ddl.Query)
		if err != nil {
			log.Error("failed to split ddl query",
				zap.String("keyspace", r.changefeedID.Keyspace()),
				zap.String("changefeed", r.changefeedID.Name()),
				zap.String("query", ddl.Query), zap.Error(err))
			return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
		}
		if len(queries) == 0 {
			return ddl.Query, nil
		}
	}

	defaultSchemas := ddlDefaultSchemas(ddl, len(queries))
	var (
		builder strings.Builder
		routed  bool
	)
	for i := range queries {
		query := queries[i]
		newQuery, changed, err := r.rewriteSingleDDLQuery(query, defaultSchemas[i])
		if err != nil {
			return "", err
		}
		if changed {
			routed = true
			query = newQuery
		}
		appendDDLQuery(&builder, query, len(queries) > 1)
	}
	if !routed {
		return ddl.Query, nil
	}

	newQuery := builder.String()
	log.Info("ddl query rewritten with routing",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("originalQuery", ddl.Query),
		zap.String("newQuery", newQuery))
	return newQuery, nil
}

func (r Router) rewriteSingleDDLQuery(query string, defaultSchema string) (string, bool, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		log.Error("failed to parse ddl query",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query), zap.Error(err))
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	sourceTables, err := fetchDDLTables(defaultSchema, stmt)
	if err != nil {
		log.Error("failed to fetch ddl tables",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query), zap.Error(err))
		return "", false, err
	}

	if len(sourceTables) == 0 {
		return query, false, nil
	}

	var (
		routed       bool
		targetTables = make([]*filter.Table, 0, len(sourceTables))
	)
	for _, srcTable := range sourceTables {
		targetSchema, targetTable, changed := r.route(srcTable.Schema, srcTable.Name)
		if changed {
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
		log.Error("failed to rewrite ddl query",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query), zap.Any("targetTables", targetTables), zap.Error(err))
		return "", false, err
	}

	return newQuery, true, nil
}

func ddlDefaultSchemas(ddl *commonEvent.DDLEvent, queryCount int) []string {
	defaultSchema := ddlDefaultSchema(ddl)
	defaultSchemas := make([]string, queryCount)
	for i := range defaultSchemas {
		defaultSchemas[i] = defaultSchema
	}
	if len(ddl.MultipleTableInfos) != queryCount {
		return defaultSchemas
	}
	for i, info := range ddl.MultipleTableInfos {
		if info != nil {
			defaultSchemas[i] = info.GetSchemaName()
		}
	}
	return defaultSchemas
}

func ddlDefaultSchema(ddl *commonEvent.DDLEvent) string {
	if ddl.TableInfo != nil {
		return ddl.TableInfo.GetSchemaName()
	}
	return ddl.GetSchemaName()
}

func appendDDLQuery(builder *strings.Builder, query string, ensureSemicolon bool) {
	builder.WriteString(query)
	if ensureSemicolon && !strings.HasSuffix(query, ";") {
		builder.WriteByte(';')
	}
}

func isUnsupportedTableRoutingDDLAction(actionType model.ActionType) bool {
	switch actionType {
	case cdcfilter.ActionAddFullTextIndex, cdcfilter.ActionCreateHybridIndex:
		return true
	default:
	}
	return false
}

func ddlQueryMayContainAdditionalTableRefs(actionType model.ActionType) bool {
	switch actionType {
	case model.ActionCreateTable,
		model.ActionCreateTables,
		model.ActionDropTable,
		model.ActionRenameTable,
		model.ActionRenameTables,
		model.ActionCreateView,
		model.ActionDropView,
		model.ActionAddForeignKey,
		model.ActionExchangeTablePartition:
		return true
	default:
		return false
	}
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
