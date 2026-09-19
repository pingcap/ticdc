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
	"slices"
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

	visitor := newTableRenameVisitor(router, defaultSchema)
	stmt.Accept(visitor)
	if visitor.err != nil {
		return false, visitor.err
	}
	visitor.resolveBindings()
	return visitor.routed, nil
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

// restoreDDLStmt serializes a routed DDL AST.
//
// Returned DDL uses StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func restoreDDLStmt(stmt ast.StmtNode) (string, error) {
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

// cteScopes tracks CTE visibility in AST visit order. Non-recursive CTEs
// become visible after their definition; recursive CTEs can reference themselves.
// CTE references are not physical tables and must not be renamed.
type cteScopes struct {
	scopes []map[string]struct{}
}

func (c *cteScopes) enter(in ast.Node) {
	switch n := in.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		c.scopes = append(c.scopes, nil)
	case *ast.CommonTableExpression:
		if n.IsRecursive {
			c.add(n.Name.L)
		}
	}
}

func (c *cteScopes) leave(in ast.Node) {
	switch n := in.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		c.scopes = c.scopes[:len(c.scopes)-1]
	case *ast.CommonTableExpression:
		c.add(n.Name.L)
	}
}

func (c *cteScopes) add(name string) {
	i := len(c.scopes) - 1
	if c.scopes[i] == nil {
		c.scopes[i] = make(map[string]struct{})
	}
	c.scopes[i][name] = struct{}{}
}

func (c *cteScopes) contains(table *ast.TableName) bool {
	if table.Schema.O != "" {
		return false
	}
	for _, scope := range slices.Backward(c.scopes) {
		if _, ok := scope[table.Name.L]; ok {
			return true
		}
	}
	return false
}

// tableRenameVisitor rewrites table names and table references of a DDL AST.
//
// Every physical table name is routed and rewritten where it is visited, so no
// positional bookkeeping is needed. References are rewritten through the SELECT
// scope chain:
//
//   - `db`.`table`.`col` and `db`.`table`.* name a physical table directly and
//     are routed directly.
//   - `table`.`col` and `table`.* resolve to a range variable. The innermost
//     SELECT wins; when it does not declare the name, the search continues in
//     the enclosing SELECT. An alias, a CTE name, or an ambiguous declaration
//     stops the search, because the reference does not denote a physical table.
//
// The resolution rules are shared with the CREATE VIEW normalization in
// pkg/common/event (createViewSelectNormalizer.qualifyColumnName), which resolves
// the same references to their source schema instead of the routed table. Keep
// both in sync; TestRewriteParserBackedDDLQueryRangeVariableResolution pins the
// same case list.
//
// Example for a CREATE VIEW with routing rule source_db.* → target_db.{table}_r:
//
//	Source AST:
//	  CREATE VIEW `source_db`.`v` AS
//	    SELECT `source_db`.`t`.`id` FROM `source_db`.`t`
//
//	Rewritten AST:
//	  CREATE VIEW `target_db`.`v_r` AS
//	    SELECT `target_db`.`t_r`.`id` FROM `target_db`.`t_r`
type tableRenameVisitor struct {
	ctes          cteScopes
	router        Router
	defaultSchema string
	// scope is the innermost SELECT scope being visited.
	scope *selectScope
	// bindings are table-qualified references, resolved after the whole walk.
	bindings []pendingBinding
	// routed records whether any name changed.
	routed bool
	// err holds the first routing error; ast.Visitor cannot return errors.
	err error
}

// selectScope records the range variables of one SELECT (or set operation) node.
// Scopes form a chain through parent: a table-qualified reference is resolved
// from its own SELECT outward, so a correlated reference finds the enclosing
// table. The maps are filled during the AST walk but only read afterwards, so
// resolution does not depend on the order in which nodes are visited.
type selectScope struct {
	parent *selectScope
	// aliases holds range variable names that do not denote a physical table:
	// table aliases, derived tables, and CTE references. A name found here hides
	// physical tables declared in outer scopes.
	aliases map[string]struct{}
	// tables maps a normalized unaliased physical table name to its routed name.
	tables map[string]commonEvent.SchemaTableName
	// ambiguousTables holds physical table names declared more than once in this
	// scope; their qualifiers cannot be bound to a single table.
	ambiguousTables map[string]struct{}
}

// pendingBinding is one table-qualified reference: a column such as `t`.`c`, or
// a wildcard such as `t`.*. Schema-qualified references are rewritten
// immediately and never become pending.
type pendingBinding struct {
	scope *selectScope
	// aliasKey is lower-cased, like every alias and CTE key.
	aliasKey string
	// tableKey follows the router's case sensitivity for physical table names.
	tableKey string
	column   *ast.ColumnName
	wildcard *ast.WildCardField
}

func newTableRenameVisitor(router Router, defaultSchema string) *tableRenameVisitor {
	return &tableRenameVisitor{
		router:        router,
		defaultSchema: defaultSchema,
	}
}

func (v *tableRenameVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return in, true
	}
	v.ctes.enter(in)
	switch n := in.(type) {
	case *ast.SelectStmt:
		v.enterScope()
		v.collectWildCards(n.Fields)
	case *ast.SetOprStmt, *ast.SetOprSelectList:
		v.enterScope()
	case *ast.TableSource:
		v.collectTable(n)
	}
	if t, ok := in.(*ast.TableName); ok {
		if !v.ctes.contains(t) {
			v.renameTable(t)
		}
		return in, true
	}
	if c, ok := in.(*ast.ColumnName); ok {
		if c.Schema.O != "" {
			v.rewriteColumnName(c)
		} else if c.Table.O != "" {
			v.bindings = append(v.bindings, pendingBinding{
				scope:    v.scope,
				aliasKey: c.Table.L,
				tableKey: v.tableKey(c.Table.O),
				column:   c,
			})
		}
		return in, true
	}
	return in, false
}

func (v *tableRenameVisitor) Leave(in ast.Node) (ast.Node, bool) {
	if v.err != nil {
		return in, false
	}
	v.ctes.leave(in)
	switch in.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		v.leaveScope()
	}
	return in, true
}

// route resolves one name, filling an empty schema with the statement default.
func (v *tableRenameVisitor) route(schema, table string) (RouteBinding, error) {
	if schema == "" && table != "" {
		schema = v.defaultSchema
	}
	return v.router.Route(schema, table)
}

// renameTable routes one physical table name and rewrites it in place.
func (v *tableRenameVisitor) renameTable(t *ast.TableName) {
	binding, err := v.route(t.Schema.O, t.Name.O)
	if err != nil {
		v.err = err
		return
	}
	t.Schema = ast.NewCIStr(binding.Target.Schema)
	t.Name = ast.NewCIStr(binding.Target.Table)
	v.routed = v.routed || binding.routed()
}

// qualifiedTarget routes a schema-qualified table reference
// (`db`.`table`.`col` or `db`.`table`.*) and reports whether it moved.
func (v *tableRenameVisitor) qualifiedTarget(schema, table string) (commonEvent.SchemaTableName, bool) {
	binding, err := v.router.Route(schema, table)
	if err != nil {
		v.err = err
		return commonEvent.SchemaTableName{}, false
	}
	if !binding.routed() {
		return commonEvent.SchemaTableName{}, false
	}
	v.routed = true
	return schemaTableName(binding.Target), true
}

func schemaTableName(key TableKey) commonEvent.SchemaTableName {
	return commonEvent.SchemaTableName{SchemaName: key.Schema, TableName: key.Table}
}

func (v *tableRenameVisitor) enterScope() {
	v.scope = &selectScope{
		parent:          v.scope,
		aliases:         make(map[string]struct{}),
		tables:          make(map[string]commonEvent.SchemaTableName),
		ambiguousTables: make(map[string]struct{}),
	}
}

func (v *tableRenameVisitor) leaveScope() {
	v.scope = v.scope.parent
}

// addRangeVariable records a range variable that is not a physical table, so it
// hides physical tables with the same name declared in outer scopes.
func (v *tableRenameVisitor) addRangeVariable(name string) {
	if v.scope == nil {
		return
	}
	v.scope.aliases[name] = struct{}{}
}

// collectWildCards rewrites the schema-qualified wildcards of one SELECT field
// list and records the rest. WildCardField nodes are not visited by ast.Visitor,
// so they are read from the field list.
func (v *tableRenameVisitor) collectWildCards(fields *ast.FieldList) {
	if fields == nil || v.scope == nil {
		return
	}
	for _, field := range fields.Fields {
		wildcard := field.WildCard
		if wildcard == nil || wildcard.Table.O == "" {
			continue
		}
		if wildcard.Schema.O != "" {
			if target, ok := v.qualifiedTarget(wildcard.Schema.O, wildcard.Table.O); ok {
				wildcard.Schema = ast.NewCIStr(target.SchemaName)
				wildcard.Table = ast.NewCIStr(target.TableName)
			}
			continue
		}
		v.bindings = append(v.bindings, pendingBinding{
			scope:    v.scope,
			aliasKey: wildcard.Table.L,
			tableKey: v.tableKey(wildcard.Table.O),
			wildcard: wildcard,
		})
	}
}

func (v *tableRenameVisitor) collectTable(table *ast.TableSource) {
	if v.scope == nil {
		return
	}
	if table.AsName.O != "" {
		v.addRangeVariable(table.AsName.L)
		return
	}
	sourceTable, ok := table.Source.(*ast.TableName)
	if !ok || sourceTable.Name.O == "" {
		return
	}
	if v.ctes.contains(sourceTable) {
		// A CTE reference is a range variable, not a physical table.
		v.addRangeVariable(sourceTable.Name.L)
		return
	}
	binding, err := v.route(sourceTable.Schema.O, sourceTable.Name.O)
	if err != nil {
		v.err = err
		return
	}
	key := v.tableKey(sourceTable.Name.O)
	if _, exists := v.scope.tables[key]; exists {
		delete(v.scope.tables, key)
		v.scope.ambiguousTables[key] = struct{}{}
		return
	}
	if _, ambiguous := v.scope.ambiguousTables[key]; ambiguous {
		return
	}
	v.scope.tables[key] = schemaTableName(binding.Target)
}

// resolveBindings rewrites the collected table-qualified references. It runs
// after the whole statement is visited, so every scope is complete.
func (v *tableRenameVisitor) resolveBindings() {
	for _, binding := range v.bindings {
		target, ok := resolveRangeVariable(binding.scope, binding.aliasKey, binding.tableKey)
		if !ok {
			continue
		}
		if binding.column != nil {
			binding.column.Schema = ast.NewCIStr(target.SchemaName)
			binding.column.Table = ast.NewCIStr(target.TableName)
			continue
		}
		binding.wildcard.Schema = ast.NewCIStr(target.SchemaName)
		binding.wildcard.Table = ast.NewCIStr(target.TableName)
	}
}

// resolveRangeVariable returns the routed name of the table a table-qualified
// reference denotes. The search starts at the reference's own SELECT and walks
// outward, so a correlated reference resolves to the enclosing table. An alias,
// a CTE name, or an ambiguous declaration stops the search. aliasKey is matched
// case-insensitively; tableKey follows the router's case sensitivity.
func resolveRangeVariable(scope *selectScope, aliasKey, tableKey string) (commonEvent.SchemaTableName, bool) {
	for s := scope; s != nil; s = s.parent {
		if _, ok := s.aliases[aliasKey]; ok {
			return commonEvent.SchemaTableName{}, false
		}
		if _, ok := s.ambiguousTables[tableKey]; ok {
			return commonEvent.SchemaTableName{}, false
		}
		if target, ok := s.tables[tableKey]; ok {
			return target, true
		}
	}
	return commonEvent.SchemaTableName{}, false
}

// rewriteColumnName rewrites a schema-qualified column reference
// (e.g. `db`.`t`.`col`) so it keeps pointing at the routed table.
func (v *tableRenameVisitor) rewriteColumnName(c *ast.ColumnName) {
	if c == nil || c.Schema.O == "" || c.Table.O == "" {
		return
	}
	target, ok := v.qualifiedTarget(c.Schema.O, c.Table.O)
	if !ok {
		return
	}
	c.Schema = ast.NewCIStr(target.SchemaName)
	c.Table = ast.NewCIStr(target.TableName)
}

// tableKey normalizes an unqualified physical table name. Table names follow the
// router's case sensitivity, so a case-sensitive router keeps `T` and `t`
// distinct. Aliases are not normalized here: they are matched through their
// lower-cased form, like SQL identifiers.
func (v *tableRenameVisitor) tableKey(name string) string {
	return normalizeIdentifier(name, v.router.caseSensitive)
}
