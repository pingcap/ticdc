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
// carries cross-schema table references.
//
// Example — cross-schema reference:
//
//	query            = "CREATE VIEW `target_db`.`v` AS SELECT `id` FROM `users`"
//	storedSelectStmt = "SELECT `id` FROM `source_db`.`users`"
//	currentSchema    = "target_db"
//
//	The original query omits the schema for `users`. In MySQL/TiDB, `users`
//	resolves to the session database, which is `source_db`. TiDB records this
//	resolution in View.SelectStmt with the fully qualified table name.
//	Because `source_db` != `target_db` (the view's own schema), the function
//	replaces the SELECT body:
//
//	→ "CREATE VIEW `target_db`.`v` AS SELECT `id` FROM `source_db`.`users`"
//
// Example — same-schema reference (no change):
//
//	query            = "CREATE VIEW `db`.`v` AS SELECT `id` FROM `t`"
//	storedSelectStmt = "SELECT `id` FROM `db`.`t`"
//	currentSchema    = "db"
//
//	All referenced tables are in the view's own schema, so the original query
//	is kept as-is.
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
	if createViewSelectUsesCurrentSchemaOnly(selectStmt, currentSchema) {
		return query, nil
	}

	createViewStmt.Select = selectStmt
	query, err = Restore(createViewStmt)
	if err != nil {
		return query, errors.WrapError(errors.ErrDDLEventError, err)
	}
	return query, nil
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
