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
	"github.com/pingcap/ticdc/pkg/sqlname"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
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
		newQuery, changed, err := r.rewriteSingleDDLQuery(query, ddl.GetSchemaName())
		if err != nil {
			return "", err
		}
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

// rewriteSingleDDLQuery routes one DDL statement and reports whether the
// statement text changed. Unqualified table names are resolved with the
// statement's default schema.
//
// Example:
//
//	defaultSchema = "source_db"
//	query         = "ALTER TABLE t ADD COLUMN c INT"
//	route {source_db, t} with rule source_db.* → target_db.{table}_r
//	→ "ALTER TABLE `target_db`.`t_r` ADD COLUMN `c` INT"
func (r Router) rewriteSingleDDLQuery(query string, defaultSchema string) (string, bool, error) {
	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		return "", false, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}
	if _, ok := stmt.(ast.DDLNode); !ok {
		// Non-DDL statements carry no routed names.
		return query, false, nil
	}

	routed, err := rewriteDDLStmt(stmt, r, defaultSchema)
	if err != nil {
		return "", false, err
	}
	if !routed {
		return query, false, nil
	}

	newQuery, err := restoreDDLStmt(stmt)
	if err != nil {
		return "", false, err
	}
	return newQuery, true, nil
}

// rewriteDDLStmt routes the names of one DDL AST in place and reports whether
// anything was routed.
func rewriteDDLStmt(stmt ast.StmtNode, router Router, defaultSchema string) (bool, error) {
	switch stmt := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		return routeDatabaseName(&stmt.Name, router)
	case *ast.CreateDatabaseStmt:
		return routeDatabaseName(&stmt.Name, router)
	case *ast.DropDatabaseStmt:
		return routeDatabaseName(&stmt.Name, router)
	}

	bindings := sqlname.Bind(stmt, defaultSchema)
	routed := false
	_, err := bindings.Apply(func(source sqlname.Name) (sqlname.Name, error) {
		binding, err := router.Route(source.Schema, source.Table)
		if err != nil {
			return sqlname.Name{}, err
		}
<<<<<<< HEAD
		t.Schema = parser_model.NewCIStr(v.targetNames[v.i].SchemaName)
		t.Name = parser_model.NewCIStr(v.targetNames[v.i].TableName)
		v.i++
		return in, true
	}
	if c, ok := in.(*ast.ColumnName); ok {
		v.rewriteColumnName(c)
		return in, true
	}
	return in, false
=======
		routed = routed || binding.routed()
		return sqlname.Name{Schema: binding.Target.Schema, Table: binding.Target.Table}, nil
	})
	return routed, err
>>>>>>> 07c236849 (routing: fix rule validation, MQ dispatch and view rewriting (#6259))
}

// routeDatabaseName routes the database name of a database-level DDL.
func routeDatabaseName(name *ast.CIStr, router Router) (bool, error) {
	binding, err := router.Route(name.O, "")
	if err != nil {
		return false, err
	}
	if !binding.routed() {
		return false, nil
	}
	*name = ast.NewCIStr(binding.Target.Schema)
	return true, nil
}

<<<<<<< HEAD
// rewriteColumnName rewrites only schema-qualified column references
// (e.g. `db`.`t`.`col`) to match routed table names.
func (v *tableRenameVisitor) rewriteColumnName(c *ast.ColumnName) {
	if c == nil || c.Schema.O == "" || c.Table.O == "" {
		return
	}

	target, ok := v.targetByQualifiedSource[normalizedSchemaTableName(c.Schema.O, c.Table.O)]
	if !ok {
		return
	}
	c.Schema = parser_model.NewCIStr(target.SchemaName)
	c.Table = parser_model.NewCIStr(target.TableName)
}

// newTableRenameVisitor builds the lookup map used for schema-qualified column
// references. It pairs each source table with its routed target.
func newTableRenameVisitor(
	sourceTables []commonEvent.SchemaTableName,
	targetTables []commonEvent.SchemaTableName,
) *tableRenameVisitor {
	visitor := &tableRenameVisitor{
		targetNames:             targetTables,
		targetByQualifiedSource: make(map[commonEvent.SchemaTableName]commonEvent.SchemaTableName, len(sourceTables)),
	}

	for i, source := range sourceTables {
		if i >= len(targetTables) || source.TableName == "" {
			continue
		}
		target := targetTables[i]
		if source.SchemaName != "" {
			visitor.targetByQualifiedSource[normalizedSchemaTableName(source.SchemaName, source.TableName)] = target
		}
	}
	return visitor
}

func normalizedSchemaTableName(schema, table string) commonEvent.SchemaTableName {
	return commonEvent.SchemaTableName{
		SchemaName: strings.ToLower(schema),
		TableName:  strings.ToLower(table),
	}
}

// rewriteDDLStmtTables rewrites table names in a DDL AST.
// sourceTables and targetTables must have matching lengths and follow the
// traversal order produced by extractTableNames. TableName nodes are rewritten
// positionally. For CREATE VIEW, schema-qualified column references are also
// updated so `db`.`table`.`column` keeps pointing at the routed table.
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
		v.Name = parser_model.NewCIStr(targetTables[0].SchemaName)
	case *ast.CreateDatabaseStmt:
		if len(targetTables) != 1 {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected 1, got %d", len(targetTables))
		}
		v.Name = parser_model.NewCIStr(targetTables[0].SchemaName)
	case *ast.DropDatabaseStmt:
		if len(targetTables) != 1 {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected 1, got %d", len(targetTables))
		}
		v.Name = parser_model.NewCIStr(targetTables[0].SchemaName)
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

=======
// restoreDDLStmt serializes a routed DDL AST.
//
// Returned DDL uses StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func restoreDDLStmt(stmt ast.StmtNode) (string, error) {
>>>>>>> 07c236849 (routing: fix rule validation, MQ dispatch and view rewriting (#6259))
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
