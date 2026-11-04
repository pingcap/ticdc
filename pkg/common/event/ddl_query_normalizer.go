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
	"github.com/pingcap/ticdc/pkg/sqlname"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// NormalizeCreateViewQueryWithStoredSelect replaces the SELECT body in a
// CREATE VIEW query with TiDB's stored View.SelectStmt when the stored SELECT
// carries information that the original query text does not carry.
// When resolve is provided, it must return canonical source names from the
// catalog at the DDL timestamp. Declarations and their references are normalized
// together, including the view name. This function never applies routing rules.
//
// TiDB persists the normalized SELECT body of a view in TableInfo.View.SelectStmt
// when executing CREATE VIEW, so this field can carry resolved source-table
// references even if job.Query keeps the original session-level text.
//
// Example:
//
//	query            = "CREATE VIEW `target_db`.`v` AS SELECT `id` FROM `users`"
//	storedSelectStmt = "SELECT `id` FROM `source_db`.`users`"
//	currentSchema    = "target_db"
//
//					 → "CREATE VIEW `target_db`.`v` AS SELECT `id` FROM `source_db`.`users`"
//
// Example:
//
//	query            = "CREATE VIEW `other_db`.`v` AS SELECT `orders`.`id` FROM `orders`"
//	storedSelectStmt = "SELECT `orders`.`id` AS `id` FROM `source_db`.`orders`"
//	currentSchema    = "other_db"
//
//	                 → "CREATE VIEW `other_db`.`v` AS SELECT `source_db`.`orders`.`id` AS `id` FROM `source_db`.`orders`"
func NormalizeCreateViewQueryWithStoredSelect(query string, storedSelectStmt string, currentSchema string, resolve sqlname.Resolver) (string, error) {
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
	if resolve != nil {
		// Bind the stored SELECT and view declaration to the same historical catalog.
		// Always retain these canonical names, even when no reference needs qualification.
		createViewStmt.Select = selectStmt
		if _, err := sqlname.Bind(createViewStmt, currentSchema).Apply(resolve); err != nil {
			return query, err
		}
	} else {
		if !normalizeCreateViewSelect(selectStmt, currentSchema) {
			return query, nil
		}
		createViewStmt.Select = selectStmt
	}

	normalizedQuery, err := Restore(createViewStmt)
	if err != nil {
		return query, errors.WrapError(errors.ErrDDLEventError, err)
	}
	return normalizedQuery, nil
}

// normalizeCreateViewSelect qualifies bound physical table references.
// With no catalog, original table spellings remain.
func normalizeCreateViewSelect(selectStmt ast.StmtNode, currentSchema string) bool {
	currentSchemaOnly := createViewSelectUsesCurrentSchemaOnly(selectStmt, currentSchema)
	changed, _ := sqlname.Bind(selectStmt, "").Apply(nil)
	return !currentSchemaOnly || changed
}

func createViewSelectUsesCurrentSchemaOnly(selectStmt ast.StmtNode, currentSchema string) bool {
	for _, schema := range extractTableSchemas(selectStmt) {
		if schema != "" && !strings.EqualFold(schema, currentSchema) {
			return false
		}
	}
	return true
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
