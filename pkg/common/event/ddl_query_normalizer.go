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

package event

import (
	"strings"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// NormalizeCreateViewQueryWithStoredSelect replaces the SELECT body in a
// CREATE VIEW query with TiDB's stored View.SelectStmt when the stored SELECT
// carries information that the original query text does not carry.
//
// TiDB persists the normalized SELECT body of a view in TableInfo.View.SelectStmt
// when executing CREATE VIEW, so this field can carry resolved source-table
// references even if job.Query keeps the original session-level text.
func NormalizeCreateViewQueryWithStoredSelect(query string, storedSelectStmt string, currentSchema string) (string, error) {
	if query == "" || storedSelectStmt == "" {
		return query, nil
	}

	stmt, err := parser.New().ParseOneStmt(query, "", "")
	if err != nil {
		return query, errors.WrapError(errors.ErrDDLEventError, err)
	}
	createViewStmt, ok := stmt.(*ast.CreateViewStmt)
	if !ok {
		return query, nil
	}

	selectStmt, err := parser.New().ParseOneStmt(storedSelectStmt, "", "")
	if err != nil {
		return query, errors.WrapError(errors.ErrDDLEventError, err)
	}
	if !normalizeCreateViewSelect(selectStmt, currentSchema) {
		return query, nil
	}

	createViewStmt.Select = selectStmt
	normalizedQuery, err := Restore(createViewStmt)
	if err != nil {
		return query, errors.WrapError(errors.ErrDDLEventError, err)
	}
	return normalizedQuery, nil
}

type createViewSelectNormalizer struct {
	changed bool
	scopes  []createViewSelectScope
}

type createViewSelectScope struct {
	aliases         map[string]struct{}
	tableByName     map[string]string
	ambiguousTables map[string]struct{}
}

// normalizeCreateViewSelect returns true when CREATE VIEW should use the stored
// SELECT body. It also turns unaliased table-qualified column references into
// schema-qualified references: `orders`.`id` with FROM `source_db`.`orders`
// becomes `source_db`.`orders`.`id`. Explicit alias references are preserved.
func normalizeCreateViewSelect(selectStmt ast.StmtNode, currentSchema string) bool {
	currentSchemaOnly := createViewSelectUsesCurrentSchemaOnly(selectStmt, currentSchema)
	normalizer := &createViewSelectNormalizer{
		scopes: make([]createViewSelectScope, 0),
	}
	selectStmt.Accept(normalizer)
	return !currentSchemaOnly || normalizer.changed
}

func createViewSelectUsesCurrentSchemaOnly(selectStmt ast.StmtNode, currentSchema string) bool {
	for _, schema := range extractTableSchemas(selectStmt) {
		if schema != "" && !strings.EqualFold(schema, currentSchema) {
			return false
		}
	}
	return true
}

func (n *createViewSelectNormalizer) Enter(in ast.Node) (ast.Node, bool) {
	switch v := in.(type) {
	case *ast.SelectStmt:
		n.scopes = append(n.scopes, buildCreateViewSelectScope(v))
	case *ast.ColumnName:
		n.qualifyColumnName(v)
	}
	return in, false
}

func (n *createViewSelectNormalizer) Leave(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.SelectStmt); ok {
		n.scopes = n.scopes[:len(n.scopes)-1]
	}
	return in, true
}

func (n *createViewSelectNormalizer) qualifyColumnName(c *ast.ColumnName) {
	if len(n.scopes) == 0 || c == nil || c.Schema.O != "" || c.Table.O == "" {
		return
	}

	scope := n.scopes[len(n.scopes)-1]
	tableKey := strings.ToLower(c.Table.O)
	if _, ok := scope.aliases[tableKey]; ok {
		return
	}
	if _, ok := scope.ambiguousTables[tableKey]; ok {
		return
	}
	schema, ok := scope.tableByName[tableKey]
	if !ok {
		return
	}
	c.Schema = ast.NewCIStr(schema)
	n.changed = true
}

func buildCreateViewSelectScope(selectStmt *ast.SelectStmt) createViewSelectScope {
	scope := createViewSelectScope{
		aliases:         make(map[string]struct{}),
		tableByName:     make(map[string]string),
		ambiguousTables: make(map[string]struct{}),
	}
	if selectStmt == nil || selectStmt.From == nil || selectStmt.From.TableRefs == nil {
		return scope
	}
	collectCreateViewSelectTables(selectStmt.From.TableRefs, &scope)
	return scope
}

func collectCreateViewSelectTables(node ast.ResultSetNode, scope *createViewSelectScope) {
	switch v := node.(type) {
	case *ast.Join:
		if v.Left != nil {
			collectCreateViewSelectTables(v.Left, scope)
		}
		if v.Right != nil {
			collectCreateViewSelectTables(v.Right, scope)
		}
	case *ast.TableSource:
		if v.AsName.O != "" {
			scope.aliases[strings.ToLower(v.AsName.O)] = struct{}{}
			return
		}
		tableName, ok := v.Source.(*ast.TableName)
		if !ok || tableName.Schema.O == "" || tableName.Name.O == "" {
			return
		}
		tableKey := strings.ToLower(tableName.Name.O)
		if _, ambiguous := scope.ambiguousTables[tableKey]; ambiguous {
			return
		}
		if _, exists := scope.tableByName[tableKey]; exists {
			delete(scope.tableByName, tableKey)
			scope.ambiguousTables[tableKey] = struct{}{}
			return
		}
		scope.tableByName[tableKey] = tableName.Schema.O
	}
}

type tableSchemaExtractor struct {
	schemas []string
}

func (e *tableSchemaExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		e.schemas = append(e.schemas, t.Schema.O)
		return in, true
	}
	return in, false
}

func (e *tableSchemaExtractor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// extractTableSchemas returns schema qualifiers from all *ast.TableName nodes in
// AST visit order. Unqualified tables contribute an empty schema name.
func extractTableSchemas(node ast.Node) []string {
	if node == nil {
		return nil
	}

	extractor := &tableSchemaExtractor{
		schemas: make([]string, 0),
	}
	node.Accept(extractor)
	return extractor.schemas
}
