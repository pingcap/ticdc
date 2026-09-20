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
//   - Both schema-qualified and table-qualified columns and wildcards reuse
//     the route of their FROM table. SQL name binding is case-insensitive,
//     independently of the route matcher configuration.
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
	// withScopes saves the consuming queries while visiting their CTE definitions.
	withScopes []*selectScope
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
	tables map[TableKey]commonEvent.SchemaTableName
	// ambiguousTables holds physical table names declared more than once in this
	// scope; their qualifiers cannot be bound to a single table.
	ambiguousTables map[TableKey]struct{}
}

// pendingBinding is a column or wildcard qualifier, resolved after FROM tables
// have been collected. source is lower-cased according to TiDB SQL name binding;
// an empty schema denotes an unqualified range variable.
type pendingBinding struct {
	scope    *selectScope
	source   TableKey
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
	case *ast.WithClause:
		// CTE definitions see the query's outer scopes, but not its FROM.
		// Keep CTE name visibility in v.ctes independent of this table scope.
		v.withScopes = append(v.withScopes, v.scope)
		v.scope = v.scope.parent
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
		if c.Table.O != "" {
			v.bindings = append(v.bindings, pendingBinding{
				scope:  v.scope,
				source: TableKey{Schema: c.Schema.L, Table: c.Table.L},
				column: c,
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
	case *ast.WithClause:
		v.scope = v.withScopes[len(v.withScopes)-1]
		v.withScopes = v.withScopes[:len(v.withScopes)-1]
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

func schemaTableName(key TableKey) commonEvent.SchemaTableName {
	return commonEvent.SchemaTableName{SchemaName: key.Schema, TableName: key.Table}
}

func (v *tableRenameVisitor) enterScope() {
	v.scope = &selectScope{
		parent:          v.scope,
		aliases:         make(map[string]struct{}),
		tables:          make(map[TableKey]commonEvent.SchemaTableName),
		ambiguousTables: make(map[TableKey]struct{}),
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

// collectWildCards records qualified wildcards of one SELECT field list.
// WildCardField nodes are not visited by ast.Visitor, so they are read here.
func (v *tableRenameVisitor) collectWildCards(fields *ast.FieldList) {
	if fields == nil || v.scope == nil {
		return
	}
	for _, field := range fields.Fields {
		wildcard := field.WildCard
		if wildcard == nil || wildcard.Table.O == "" {
			continue
		}

		v.bindings = append(v.bindings, pendingBinding{
			scope:    v.scope,
			source:   TableKey{Schema: wildcard.Schema.L, Table: wildcard.Table.L},
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
	// Index both qualified and unqualified names. Two same-named tables from
	// different schemas only make the unqualified name ambiguous.
	source := binding.Source.normalized(false)
	keys := []TableKey{{Table: source.Table}}
	if source.Schema != "" {
		keys = append(keys, source)
	}
	for _, key := range keys {
		if _, exists := v.scope.tables[key]; exists {
			delete(v.scope.tables, key)
			v.scope.ambiguousTables[key] = struct{}{}
			continue
		}
		if _, ambiguous := v.scope.ambiguousTables[key]; !ambiguous {
			v.scope.tables[key] = schemaTableName(binding.Target)
		}
	}
}

// resolveBindings rewrites the collected table-qualified references. It runs
// after the whole statement is visited, so every scope is complete.
func (v *tableRenameVisitor) resolveBindings() {
	for _, binding := range v.bindings {
		target, ok := resolveRangeVariable(binding.scope, binding.source)
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

// resolveRangeVariable returns the routed name of the FROM table denoted by a
// qualifier. SQL name binding is always case-insensitive. Unqualified aliases,
// CTE names and ambiguous declarations stop the search through outer scopes.
func resolveRangeVariable(scope *selectScope, source TableKey) (commonEvent.SchemaTableName, bool) {
	for s := scope; s != nil; s = s.parent {
		if source.Schema == "" {
			if _, ok := s.aliases[source.Table]; ok {
				return commonEvent.SchemaTableName{}, false
			}
		}
		if _, ok := s.ambiguousTables[source]; ok {
			return commonEvent.SchemaTableName{}, false
		}
		if target, ok := s.tables[source]; ok {
			return target, true
		}
	}
	return commonEvent.SchemaTableName{}, false
}
