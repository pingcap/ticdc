// Copyright 2025 PingCAP, Inc.
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

package util

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/util/filter"
)

func genTableName(schema string, table string) *filter.Table {
	return &filter.Table{Schema: schema, Name: table}
}

// tableNameExtractor extracts table names from DDL AST nodes.
// ref: https://github.com/pingcap/tidb/blob/09feccb529be2830944e11f5fed474020f50370f/server/sql_info_fetcher.go#L46
type tableNameExtractor struct {
	curDB string
	names []*filter.Table
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.ReferenceDef); ok {
		return in, true
	}
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

// FetchDDLTables returns tables in DDL statement.
// Because we use visitor pattern, first tableName is always upper-most table in AST.
// Specifically:
//   - for `CREATE TABLE ... LIKE` DDL, result contains [sourceTable, sourceRefTable]
//   - for RENAME TABLE DDL, result contains [old1, new1, old2, new2, old3, new3, ...] because of TiDB parser
//   - for other DDL, order of tableName is the node visit order.
func FetchDDLTables(schema string, stmt ast.StmtNode) ([]*filter.Table, error) {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		return nil, fmt.Errorf("unknown DDL type: %T", stmt)
	}

	// Special cases: schema related SQLs don't have tableName
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		dbName := v.Name.O
		if dbName == "" {
			dbName = schema
		}
		return []*filter.Table{{Schema: dbName, Name: ""}}, nil
	case *ast.CreateDatabaseStmt:
		return []*filter.Table{{Schema: v.Name.O, Name: ""}}, nil
	case *ast.DropDatabaseStmt:
		return []*filter.Table{{Schema: v.Name.O, Name: ""}}, nil
	}

	e := &tableNameExtractor{
		curDB: schema,
		names: make([]*filter.Table, 0),
	}
	stmt.Accept(e)

	return e.names, nil
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
	if _, ok := in.(*ast.ReferenceDef); ok {
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

// RenameDDLTable renames tables in DDL by given `targetTables`.
// Argument `targetTables` should have the same structure as the return value of FetchDDLTables.
// Returned DDL is formatted like StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func RenameDDLTable(stmt ast.StmtNode, targetTables []*filter.Table) (string, error) {
	switch stmt.(type) {
	case ast.DDLNode:
	default:
		return "", fmt.Errorf("unknown DDL type: %T", stmt)
	}

	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	case *ast.CreateDatabaseStmt:
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	case *ast.DropDatabaseStmt:
		v.Name = ast.NewCIStr(targetTables[0].Schema)
	default:
		visitor := &tableRenameVisitor{
			targetNames: targetTables,
		}
		stmt.Accept(visitor)
		if visitor.hasErr {
			return "", fmt.Errorf("failed to rewrite DDL: not enough target tables for statement, got %d tables", len(targetTables))
		}
		// Check if all target tables were consumed - extra targets indicate a configuration mismatch
		if visitor.i < len(targetTables) {
			return "", fmt.Errorf("failed to rewrite DDL: %d target tables provided but only %d were used in statement", len(targetTables), visitor.i)
		}
	}

	var b []byte
	bf := bytes.NewBuffer(b)
	err := stmt.Restore(&format.RestoreCtx{
		Flags: format.DefaultRestoreFlags | format.RestoreTiDBSpecialComment | format.RestoreStringWithoutDefaultCharset,
		In:    bf,
	})
	if err != nil {
		return "", fmt.Errorf("failed to restore DDL AST: %w", err)
	}

	return bf.String(), nil
}
