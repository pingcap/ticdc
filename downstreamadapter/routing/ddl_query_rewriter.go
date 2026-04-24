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
	"bytes"
	"strings"

	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	cdcfilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/filter"
	"go.uber.org/zap"
)

// mustRewriteDDLQuery rewrites a DDL query by applying routing rules
// to transform source table names to target table names.
func (r Router) mustRewriteDDLQuery(ddl *commonEvent.DDLEvent) string {
	if len(r.rules) == 0 {
		return ddl.Query
	}

	switch model.ActionType(ddl.Type) {
	case cdcfilter.ActionAddFullTextIndex:
		targetSchema, targetTable, changed := r.route(ddl.GetSchemaName(), ddl.GetTableName())
		if !changed {
			return ddl.Query
		}
		newQuery := r.rewriteAddFullTextIndexQuery(ddl.Query, formatQualifiedTableName(targetSchema, targetTable))
		log.Info("ddl query rewritten with routing",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("originalQuery", ddl.Query),
			zap.String("newQuery", newQuery))
		return newQuery
	case cdcfilter.ActionCreateHybridIndex:
		targetSchema, targetTable, changed := r.route(ddl.GetSchemaName(), ddl.GetTableName())
		if !changed {
			return ddl.Query
		}
		newQuery := r.rewriteCreateHybridIndexQuery(ddl.Query, formatQualifiedTableName(targetSchema, targetTable))
		log.Info("ddl query rewritten with routing",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("originalQuery", ddl.Query),
			zap.String("newQuery", newQuery))
		return newQuery
	default:
	}

	queries := []string{ddl.Query}
	if strings.Contains(ddl.Query, ";") {
		var err error
		queries, err = commonEvent.SplitQueries(ddl.Query)
		if err != nil {
			log.Panic("split ddl query failed when rewriting",
				zap.String("keyspace", r.changefeedID.Keyspace()),
				zap.String("changefeed", r.changefeedID.Name()),
				zap.String("query", ddl.Query), zap.Error(err))
		}
	}

	defaultSchema := ddl.GetSchemaName()
	var (
		builder strings.Builder
		routed  bool
	)
	for i := range queries {
		query := queries[i]
		newQuery, changed := r.rewriteSingleDDLQuery(query, defaultSchema)
		if changed {
			routed = true
			query = newQuery
		}
		builder.WriteString(query)
		if len(queries) > 1 && !strings.HasSuffix(query, ";") {
			builder.WriteByte(';')
		}
	}
	if !routed {
		return ddl.Query
	}

	newQuery := builder.String()
	log.Info("ddl query rewritten with routing",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("originalQuery", ddl.Query),
		zap.String("newQuery", newQuery))
	return newQuery
}

func (r Router) rewriteSingleDDLQuery(query string, defaultSchema string) (string, bool) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		log.Panic("parse ddl failed when rewriting",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query), zap.Error(err))
	}

	sourceTables := fetchDDLTables(defaultSchema, stmt)
	if len(sourceTables) == 0 {
		return query, false
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
		return query, false
	}

	return mustRewriteDDLStmtTables(stmt, targetTables), true
}

func (r Router) rewriteAddFullTextIndexQuery(query string, targetTableName string) string {
	const (
		alterTable       = "ALTER TABLE"
		addFullTextIndex = "ADD FULLTEXT INDEX"
	)

	upperQuery := strings.ToUpper(query)
	if !strings.HasPrefix(upperQuery, alterTable) {
		log.Panic("rewrite parser unsupported ddl query failed",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query))
	}

	tableStart := len(alterTable)
	addIndexStart := strings.Index(upperQuery[tableStart:], addFullTextIndex)
	if addIndexStart < 0 || strings.TrimSpace(query[tableStart:tableStart+addIndexStart]) == "" {
		log.Panic("rewrite parser unsupported ddl query failed",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query))
	}
	addIndexStart += tableStart

	return query[:tableStart] + " " +
		targetTableName + " " +
		strings.TrimLeft(query[addIndexStart:], " \n\r\t\f")
}

func (r Router) rewriteCreateHybridIndexQuery(query string, targetTableName string) string {
	const createHybridIndex = "CREATE HYBRID INDEX"

	upperQuery := strings.ToUpper(query)
	columnListStart := strings.IndexByte(query, '(')
	onStart := -1
	if columnListStart >= 0 {
		onStart = strings.LastIndex(upperQuery[:columnListStart], " ON ")
	}
	tableStart := onStart + len(" ON ")
	if !strings.HasPrefix(upperQuery, createHybridIndex) ||
		columnListStart < 0 ||
		onStart < 0 ||
		strings.TrimSpace(query[tableStart:columnListStart]) == "" {
		log.Panic("rewrite parser unsupported ddl query failed",
			zap.String("keyspace", r.changefeedID.Keyspace()),
			zap.String("changefeed", r.changefeedID.Name()),
			zap.String("query", query))
	}
	return query[:tableStart] +
		targetTableName +
		strings.TrimLeft(query[columnListStart:], " \n\r\t\f")
}

func formatQualifiedTableName(schema, table string) string {
	if schema == "" {
		return quoteDDLIdentifier(table)
	}
	if table == "" {
		return quoteDDLIdentifier(schema)
	}
	return quoteDDLIdentifier(schema) + "." + quoteDDLIdentifier(table)
}

func quoteDDLIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func ddlQueryMayContainMoreTableNames(actionType model.ActionType) bool {
	switch actionType {
	case model.ActionCreateTable:
		// CREATE TABLE `target_db`.`new_t` LIKE `source_db`.`old_t`
		// CREATE TABLE `source_db`.`child` (... REFERENCES `source_db`.`parent`(`id`))
		return true
	case model.ActionCreateTables:
		// CREATE TABLE `source_db`.`t1`(...);CREATE TABLE `source_db`.`t2`(...)
		return true
	case model.ActionDropTable:
		// DROP TABLE `source_db`.`t1`, `source_db`.`t2`
		return true
	case model.ActionRenameTable:
		// RENAME TABLE `source_db`.`old_t` TO `source_db`.`new_t`
		return true
	case model.ActionRenameTables:
		// RENAME TABLE `source_db`.`a` TO `source_db`.`b`, `source_db`.`c` TO `source_db`.`d`
		return true
	case model.ActionCreateView:
		// CREATE VIEW `other_db`.`v` AS SELECT * FROM `source_db`.`orders`
		return true
	case model.ActionDropView:
		// DROP VIEW `source_db`.`v1`, `source_db`.`v2`
		return true
	case model.ActionAddForeignKey:
		// ALTER TABLE `source_db`.`child` ADD FOREIGN KEY (`pid`) REFERENCES `source_db`.`parent`(`id`)
		return true
	case model.ActionExchangeTablePartition:
		// ALTER TABLE `source_db`.`pt` EXCHANGE PARTITION `p0` WITH TABLE `source_db`.`normal_t`
		return true
	default:
	}
	return false
}

// tableNameExtractor extracts table names from DDL AST nodes.
// ref: https://github.com/pingcap/tidb/blob/09feccb529be2830944e11f5fed474020f50370f/server/sql_info_fetcher.go#L46
type tableNameExtractor struct {
	curDB string
	names []*filter.Table
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		tb := &filter.Table{Schema: t.Schema.O, Name: t.Name.O}
		if tb.Schema == "" {
			tb.Schema = tne.curDB
		}
		tne.names = append(tne.names, tb)
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// fetchDDLTables returns tables in DDL statement.
// Because we use visitor pattern, first tableName is always upper-most table in AST.
// Specifically:
//   - for `CREATE TABLE ... LIKE` DDL, result contains [sourceTable, sourceRefTable]
//   - for RENAME TABLE DDL, result contains [old1, new1, old2, new2, old3, new3, ...] because of TiDB parser
//   - for other DDL, order of tableName is the node visit order.
func fetchDDLTables(schema string, stmt ast.StmtNode) []*filter.Table {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		log.Panic("fetch ddl tables got non ddl statement", zap.Any("stmt", stmt))
	}

	// Special cases: schema related SQLs don't have tableName
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		dbName := v.Name.O
		if dbName == "" {
			dbName = schema
		}
		return []*filter.Table{{Schema: dbName, Name: ""}}
	case *ast.CreateDatabaseStmt:
		return []*filter.Table{{Schema: v.Name.O, Name: ""}}
	case *ast.DropDatabaseStmt:
		return []*filter.Table{{Schema: v.Name.O, Name: ""}}
	}

	e := &tableNameExtractor{
		curDB: schema,
		names: make([]*filter.Table, 0),
	}
	stmt.Accept(e)

	return e.names
}

// tableRenameVisitor renames tables in DDL AST nodes.
type tableRenameVisitor struct {
	targetNames []*filter.Table
	i           int
	hasErr      bool
}

func (v *tableRenameVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
		return in, true
	}
	if t, ok := in.(*ast.TableName); ok {
		if v.i >= len(v.targetNames) {
			v.hasErr = true
			return in, true
		}
		t.Schema = ast.NewCIStr(v.targetNames[v.i].Schema)
		t.Name = ast.NewCIStr(v.targetNames[v.i].Name)
		v.i++
		return in, true
	}
	return in, false
}

func (v *tableRenameVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
		return in, false
	}
	return in, true
}

// mustRewriteDDLStmtTables renames tables in DDL by given `targetTables`.
// Argument `targetTables` should have the same structure as the return value of fetchDDLTables.
// Returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func mustRewriteDDLStmtTables(stmt ast.StmtNode, targetTables []*filter.Table) string {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		log.Panic("rewrite ddl query got non ddl statement", zap.Any("stmt", stmt))
	}

	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		if len(targetTables) != 1 {
			log.Panic("rewrite ddl query got unexpected target table count",
				zap.Int("expected", 1),
				zap.Int("actual", len(targetTables)))
		}
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	case *ast.CreateDatabaseStmt:
		if len(targetTables) != 1 {
			log.Panic("rewrite ddl query got unexpected target table count",
				zap.Int("expected", 1),
				zap.Int("actual", len(targetTables)))
		}
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	case *ast.DropDatabaseStmt:
		if len(targetTables) != 1 {
			log.Panic("rewrite ddl query got unexpected target table count",
				zap.Int("expected", 1),
				zap.Int("actual", len(targetTables)))
		}
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	default:
		visitor := &tableRenameVisitor{
			targetNames: targetTables,
		}
		stmt.Accept(visitor)
		if visitor.hasErr {
			log.Panic("rewrite ddl query got too few target tables",
				zap.Int("targetTableCount", len(targetTables)))
		}
		// Check if all target tables were consumed - extra targets indicate a configuration mismatch
		if visitor.i < len(targetTables) {
			log.Panic("rewrite ddl query got too many target tables",
				zap.Int("targetTableCount", len(targetTables)),
				zap.Int("usedTableCount", visitor.i))
		}
	}

	bf := &bytes.Buffer{}
	err := stmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment | format.RestoreStringWithoutDefaultCharset,
		In:    bf,
	})
	if err != nil {
		log.Panic("restore ddl query failed", zap.Error(err))
	}

	return bf.String()
}
