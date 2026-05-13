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

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

// rewriteParserBackedDDLQuery rewrites a parser-supported DDL query by applying routing rules.
func (r Router) rewriteParserBackedDDLQuery(ddl *commonEvent.DDLEvent) (string, error) {
	if len(r.rules) == 0 {
		return ddl.Query, nil
	}

	queries, err := splitMultiStmtDDLQuery(ddl.Query)
	if err != nil {
		return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	var (
		builder strings.Builder
		routed  bool
	)
	for i := range queries {
		query := queries[i]
		newQuery, err := r.rewriteSingleDDLQuery(query, ddl.GetSchemaName())
		if err != nil {
			return "", err
		}
		if newQuery != query {
			routed = true
			query = newQuery
		}
		builder.WriteString(query)
		if len(queries) > 1 && !strings.HasSuffix(query, ";") {
			builder.WriteByte(';')
		}
	}
	if !routed {
		return ddl.Query, nil
	}

	return builder.String(), nil
}

func splitMultiStmtDDLQuery(query string) ([]string, error) {
	if !strings.Contains(query, ";") {
		return []string{query}, nil
	}

	// SplitQueries is parser-backed. Use its statement count to distinguish a
	// real multi-statement query from semicolons inside strings or comments.
	queries, err := commonEvent.SplitQueries(query)
	if err != nil {
		return nil, err
	}
	if len(queries) <= 1 {
		return []string{query}, nil
	}
	return queries, nil
}

// rewriteSingleDDLQuery routes a single DDL statement.
// If the schema is not qualified, fill it with the default schema.
// Cross schema scenario must be qualified before enter the router.
// Example:
//
//	defaultSchema = "source_db"
//	query         = "ALTER TABLE t ADD COLUMN c INT"
//	fillDefaultSchema → [{source_db, t}]
//	route({source_db, t}) with rule source_db.* → target_db.{table}_r
//	→ "ALTER TABLE `target_db`.`t_r` ADD COLUMN `c` INT"
func (r Router) rewriteSingleDDLQuery(query string, defaultSchema string) (string, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	sourceTables := extractTableNames(stmt)
	if len(sourceTables) == 0 {
		return query, nil
	}
	fillDefaultSchema(sourceTables, defaultSchema)

	var (
		routed       bool
		targetTables = make([]commonEvent.SchemaTableName, 0, len(sourceTables))
	)
	for _, srcTable := range sourceTables {
		targetSchema, targetTable, changed, err := r.route(srcTable.SchemaName, srcTable.TableName)
		if err != nil {
			return "", err
		}
		if changed {
			routed = true
		}
		targetTables = append(targetTables, commonEvent.SchemaTableName{
			SchemaName: targetSchema,
			TableName:  targetTable,
		})
	}

	if !routed {
		return query, nil
	}

	newQuery, err := rewriteDDLStmtTables(stmt, sourceTables, targetTables)
	if err != nil {
		return "", err
	}
	return newQuery, nil
}

func fillDefaultSchema(tables []commonEvent.SchemaTableName, defaultSchema string) {
	if defaultSchema == "" {
		return
	}

	for i := range tables {
		if tables[i].SchemaName == "" && tables[i].TableName != "" {
			tables[i].SchemaName = defaultSchema
		}
	}
}

// tableNameExtractor extracts table names from DDL AST nodes.
// ref: https://github.com/pingcap/tidb/blob/09feccb529be2830944e11f5fed474020f50370f/server/sql_info_fetcher.go#L46
type tableNameExtractor struct {
	names []commonEvent.SchemaTableName
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		tne.names = append(tne.names, commonEvent.SchemaTableName{SchemaName: t.Schema.O, TableName: t.Name.O})
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// extractTableNames returns the tables in a DDL statement in AST visit order.
// The first element is always the topmost table (the DDL target).
//
// Examples (sourceTables returned):
//
//	CREATE TABLE `db`.`t1` LIKE `db`.`t2`
//	    → [{db, t1}, {db, t2}]
//	RENAME TABLE `db`.`a` TO `db`.`b`, `db`.`c` TO `db`.`d`
//	    → [{db, a}, {db, b}, {db, c}, {db, d}]
//	ALTER TABLE `db`.`t` ADD COLUMN `c` INT
//	    → [{db, t}]
func extractTableNames(stmt ast.StmtNode) []commonEvent.SchemaTableName {
	// Special cases: schema related SQLs don't have tableName
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		return []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}
	case *ast.CreateDatabaseStmt:
		return []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}
	case *ast.DropDatabaseStmt:
		return []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}
	}

	e := &tableNameExtractor{
		names: make([]commonEvent.SchemaTableName, 0),
	}
	stmt.Accept(e)

	return e.names
}

// tableRenameVisitor rewrites table names and column qualifiers in a DDL AST.
//
// TableName nodes are rewritten positionally in the same traversal order as
// extractTableNames. ColumnName qualifiers like `t`.`col` in CREATE VIEW are
// rewritten via lookup maps (see newTableRenameVisitor).
//
// Example for a CREATE VIEW with routing rule source_db.* → target_db.{table}_r:
//
//	Source AST:
//	  CREATE VIEW `source_db`.`v` AS
//	    SELECT `source_db`.`t`.`id` FROM `source_db`.`t`
//
//	Positional: {source_db, v} → {target_db, v_r}
//	            {source_db, t} → {target_db, t_r}
//
//	Column qualifier: `source_db`.`t`.`id`
//	    qualified lookup: {source_db, t} → {target_db, t_r}
//	    → `target_db`.`t_r`.`id`
//
//	Rewritten AST:
//	  CREATE VIEW `target_db`.`v_r` AS
//	    SELECT `target_db`.`t_r`.`id` FROM `target_db`.`t_r`
type tableRenameVisitor struct {
	// targetNames contains routed names aligned with tableNameExtractor output.
	targetNames []commonEvent.SchemaTableName
	// targetByQualifiedSource maps qualified source table names to routed names.
	targetByQualifiedSource map[string]commonEvent.SchemaTableName
	// targetByTable maps unambiguous source table names to routed names.
	targetByTable map[string]commonEvent.SchemaTableName
	// ambiguousTables records source table names that appear under multiple schemas.
	ambiguousTables map[string]struct{}
	// i is the next targetNames index to consume.
	i int
	// hasErr records targetNames exhaustion because ast.Visitor cannot return an error.
	hasErr bool
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
		t.Schema = ast.NewCIStr(v.targetNames[v.i].SchemaName)
		t.Name = ast.NewCIStr(v.targetNames[v.i].TableName)
		v.i++
		return in, true
	}
	if c, ok := in.(*ast.ColumnName); ok {
		v.rewriteColumnName(c)
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

// rewriteColumnName rewrites column qualifiers (e.g. `db`.`t`.`col`) to match
// routed table names. Qualified references use the qualified lookup;
// unqualified references use the table-only lookup when unambiguous.
func (v *tableRenameVisitor) rewriteColumnName(c *ast.ColumnName) {
	if c == nil || c.Table.O == "" {
		return
	}

	if c.Schema.O != "" {
		target, ok := v.targetByQualifiedSource[qualifiedTableKey(c.Schema.O, c.Table.O)]
		if !ok {
			return
		}
		c.Schema = ast.NewCIStr(target.SchemaName)
		c.Table = ast.NewCIStr(target.TableName)
		return
	}

	tableKey := strings.ToLower(c.Table.O)
	if _, ambiguous := v.ambiguousTables[tableKey]; ambiguous {
		return
	}
	target, ok := v.targetByTable[tableKey]
	if !ok {
		return
	}
	c.Schema = ast.NewCIStr(target.SchemaName)
	c.Table = ast.NewCIStr(target.TableName)
}

// newTableRenameVisitor builds lookup maps for column qualifier rewriting.
// It pairs each source table with its routed target and populates:
//   - targetByQualifiedSource: <schema, table> → target (fully qualified column refs)
//   - targetByTable: table → target (unqualified column refs, only when unambiguous)
//
// A table name is ambiguous when it appears under multiple schemas and each
// schema routes to a different target. Ambiguous tables are skipped during
// column rewriting because the correct target cannot be determined.
func newTableRenameVisitor(
	sourceTables []commonEvent.SchemaTableName,
	targetTables []commonEvent.SchemaTableName,
) *tableRenameVisitor {
	visitor := &tableRenameVisitor{
		targetNames:             targetTables,
		targetByQualifiedSource: make(map[string]commonEvent.SchemaTableName, len(sourceTables)),
		targetByTable:           make(map[string]commonEvent.SchemaTableName, len(sourceTables)),
		ambiguousTables:         make(map[string]struct{}),
	}

	for i, source := range sourceTables {
		if i >= len(targetTables) || source.TableName == "" {
			continue
		}
		target := targetTables[i]
		if source.SchemaName != "" {
			visitor.targetByQualifiedSource[qualifiedTableKey(source.SchemaName, source.TableName)] = target
		}

		tableKey := strings.ToLower(source.TableName)
		if existing, ok := visitor.targetByTable[tableKey]; ok &&
			(!strings.EqualFold(existing.SchemaName, target.SchemaName) ||
				!strings.EqualFold(existing.TableName, target.TableName)) {
			visitor.ambiguousTables[tableKey] = struct{}{}
			delete(visitor.targetByTable, tableKey)
			continue
		}
		if _, ambiguous := visitor.ambiguousTables[tableKey]; !ambiguous {
			visitor.targetByTable[tableKey] = target
		}
	}
	return visitor
}

// qualifiedTableKey returns a canonical lookup key for a schema-qualified table.
// It joins with a null byte to prevent collisions (e.g. "a"+"bc" vs "ab"+"c").
func qualifiedTableKey(schema, table string) string {
	return strings.ToLower(schema) + "\x00" + strings.ToLower(table)
}

// rewriteDDLStmtTables rewrites table names and column qualifiers in a DDL AST.
// sourceTables and targetTables must have matching lengths and follow the
// traversal order produced by extractTableNames. TableName nodes are rewritten
// positionally; ColumnName qualifiers are rewritten via lookup maps built from
// the source/target table pairs (see newTableRenameVisitor).
//
// Returned DDL uses StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func rewriteDDLStmtTables(
	stmt ast.StmtNode,
	sourceTables []commonEvent.SchemaTableName,
	targetTables []commonEvent.SchemaTableName,
) (string, error) {
	if _, ok := stmt.(ast.DDLNode); !ok {
		return "", errors.ErrTableRoutingFailed.GenWithStack(
			"rewrite ddl query got non ddl statement: %T", stmt)
	}

	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		if len(targetTables) != 1 {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected 1, got %d", len(targetTables))
		}
		v.Name = ast.NewCIStr(targetTables[0].SchemaName)
	case *ast.CreateDatabaseStmt:
		if len(targetTables) != 1 {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected 1, got %d", len(targetTables))
		}
		v.Name = ast.NewCIStr(targetTables[0].SchemaName)
	case *ast.DropDatabaseStmt:
		if len(targetTables) != 1 {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected 1, got %d", len(targetTables))
		}
		v.Name = ast.NewCIStr(targetTables[0].SchemaName)
	default:
		visitor := newTableRenameVisitor(sourceTables, targetTables)
		stmt.Accept(visitor)
		if visitor.hasErr {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got too few target tables: count=%d", len(targetTables))
		}
		// Check if all target tables were consumed - extra targets indicate a configuration mismatch
		if visitor.i < len(targetTables) {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got too many target tables: count=%d, used=%d", len(targetTables), visitor.i)
		}
	}

	bf := &bytes.Buffer{}
	err := stmt.Restore(&format.RestoreCtx{
		// TiDB stores the original SQL in sessionctx.QueryString and copies it into
		// DDL job.Query:
		// https://github.com/pingcap/tidb/blob/8f2630e53d5d/pkg/session/session.go#L2905
		// https://github.com/pingcap/tidb/blob/8f2630e53d5d/pkg/ddl/executor.go#L6952-L6957
		// After routing mutates the AST, CDC must serialize it again. Keep the
		// parser's standard restore style, TiDB special comments, and default
		// charset handling consistent with CDC's DDL query normalization.
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment | format.RestoreStringWithoutDefaultCharset,
		In:    bf,
	})
	if err != nil {
		return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	return bf.String(), nil
}
