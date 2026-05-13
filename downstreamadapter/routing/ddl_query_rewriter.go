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
		newQuery, err := r.rewriteSingleDDLQuery(query, ddl.GetSchemaName(), ddl.BlockedTableNames)
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

func (r Router) rewriteSingleDDLQuery(
	query string,
	defaultSchema string,
	blockedTableNames []commonEvent.SchemaTableName,
) (string, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return "", errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	sourceTables := extractTableNames(stmt)
	if len(sourceTables) == 0 {
		return query, nil
	}
	if err := resolveUnqualifiedReferences(stmt, sourceTables, blockedTableNames); err != nil {
		return "", err
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

	newQuery, err := rewriteDDLStmtTables(stmt, targetTables)
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

// resolveUnqualifiedReferences ensures every table reference extracted from
// the DDL AST carries a non-empty schema name before the router applies
// routing rules. Without this, fillDefaultSchema would later assign the DDL
// event's own schema to every unqualified table, which is wrong when a table
// belongs to a different schema than the DDL target.
//
// It handles two DDL patterns where the parser sees multiple tables and at
// least one reference may lack a schema qualifier:
//
// CREATE TABLE LIKE (ast.CreateTableStmt with non-nil ReferTable):
//
//	The ReferTable is the LIKE source. If it has no schema, the router needs
//	to know which schema the source table lives in. The blockedTableNames
//	parameter carries upstream metadata (e.g. from job.InvolvingSchemaInfo)
//	that maps the unqualified table name to its real schema.
//
//	Example:
//	  -- Session is in source_extra_db, DDL creates a table in source_extra_db
//	  -- but the LIKE source "users" belongs to source_db.
//	  CREATE TABLE `source_extra_db`.`external_users` LIKE `users`
//	  -> extractTableNames -> [{source_extra_db, external_users}, {"", users}]
//	  -> fillDefaultSchema would wrongly set users' schema to source_extra_db
//	  -> blockedTableNames carries {source_db, users} from upstream metadata
//	  -> this function patches sourceTables[1].SchemaName = "source_db"
//
// CREATE TABLE AS SELECT / CREATE VIEW (ast.CreateTableStmt with non-nil
//
//	Select, or ast.CreateViewStmt):
//	The SELECT body may reference tables from other schemas without
//	qualifiers. Without upstream schema metadata for every referenced table,
//	the router cannot safely determine the correct schema and must reject
//	the rewrite.
//
//	Example:
//	  CREATE VIEW `target_db`.`v` AS SELECT `id` FROM `users`
//	  -> extractTableNames -> [{target_db, v}, {"", users}]
//	  -> fillDefaultSchema would set users' schema to "target_db" (wrong)
//	  -> no metadata available to resolve, return ErrTableRoutingFailed
//
// For DDLs where the target table itself has no schema qualifier (e.g. USE
// db + CREATE TABLE t LIKE u), fillDefaultSchema handles both the target
// and source correctly because they all belong to the same session schema.
func resolveUnqualifiedReferences(
	stmt ast.StmtNode,
	sourceTables []commonEvent.SchemaTableName,
	blockedTableNames []commonEvent.SchemaTableName,
) error {
	switch s := stmt.(type) {
	case *ast.CreateTableStmt:
		if s.Table == nil || s.Table.Schema.O == "" {
			return nil
		}

		if s.ReferTable != nil && s.ReferTable.Schema.O == "" && s.ReferTable.Name.O != "" {
			for _, blockedTableName := range blockedTableNames {
				if blockedTableName.SchemaName != "" &&
					strings.EqualFold(blockedTableName.TableName, s.ReferTable.Name.O) &&
					len(sourceTables) > 1 {
					sourceTables[1].SchemaName = blockedTableName.SchemaName
					break
				}
			}
			if len(sourceTables) <= 1 || sourceTables[1].SchemaName == "" {
				return errors.ErrTableRoutingFailed.GenWithStack(
					"table routing cannot rewrite ddl with unqualified referenced table because upstream default schema is unavailable: %T",
					stmt)
			}
		}
		if s.Select != nil && hasUnqualifiedTableName(sourceTables[1:]) {
			return errors.ErrTableRoutingFailed.GenWithStack(
				"table routing cannot rewrite ddl with unqualified referenced table because upstream default schema is unavailable: %T",
				stmt)
		}
	case *ast.CreateViewStmt:
		if s.ViewName == nil || s.ViewName.Schema.O == "" {
			return nil
		}
		if hasUnqualifiedTableName(sourceTables[1:]) {
			return errors.ErrTableRoutingFailed.GenWithStack(
				"table routing cannot rewrite ddl with unqualified referenced table because upstream default schema is unavailable: %T",
				stmt)
		}
	}
	return nil
}

func hasUnqualifiedTableName(tables []commonEvent.SchemaTableName) bool {
	for _, table := range tables {
		if table.SchemaName == "" && table.TableName != "" {
			return true
		}
	}
	return false
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

// extractTableNames returns tables in DDL statement.
// Because we use visitor pattern, first tableName is always upper-most table in AST.
// Specifically:
//   - for `CREATE TABLE ... LIKE` DDL, result contains [sourceTable, sourceRefTable]
//   - for RENAME TABLE DDL, result contains [old1, new1, old2, new2, old3, new3, ...] because of TiDB parser
//   - for other DDL, order of tableName is the node visit order.
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

// tableRenameVisitor rewrites *ast.TableName nodes in the same traversal order
// used by tableNameExtractor. Each visited table consumes one entry from
// targetNames, so the caller can detect too few or too many target names after
// traversal.
type tableRenameVisitor struct {
	// targetNames contains routed names aligned with tableNameExtractor output.
	targetNames []commonEvent.SchemaTableName
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
	return in, false
}

func (v *tableRenameVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
		return in, false
	}
	return in, true
}

// rewriteDDLStmtTables renames tables in DDL by given `targetTables`.
// Argument `targetTables` should have the same structure as the return value of extractTableNames.
// Returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func rewriteDDLStmtTables(stmt ast.StmtNode, targetTables []commonEvent.SchemaTableName) (string, error) {
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
		visitor := &tableRenameVisitor{
			targetNames: targetTables,
		}
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
