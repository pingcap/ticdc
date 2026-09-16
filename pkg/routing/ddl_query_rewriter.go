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
		binding, err := r.Route(srcTable.SchemaName, srcTable.TableName)
		if err != nil {
			return "", err
		}
		if binding.routed() {
			routed = true
		}
		targetTables = append(targetTables, commonEvent.SchemaTableName{
			SchemaName: binding.Target.Schema,
			TableName:  binding.Target.Table,
		})
	}

	if !routed {
		return query, nil
	}

	newQuery, err := rewriteDDLStmtTables(stmt, sourceTables, targetTables, r.caseSensitive)
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

// cteScopes tracks CTE visibility in AST visit order. Non-recursive CTEs
// become visible after their definition; recursive CTEs can reference themselves.
// Both extraction and rewriting must skip the same CTE references.
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

// tableNameExtractor extracts table names from DDL AST nodes.
// ref: https://github.com/pingcap/tidb/blob/09feccb529be2830944e11f5fed474020f50370f/server/sql_info_fetcher.go#L46
type tableNameExtractor struct {
	ctes  cteScopes
	names []commonEvent.SchemaTableName
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	tne.ctes.enter(in)
	if t, ok := in.(*ast.TableName); ok {
		if tne.ctes.contains(t) {
			return in, true
		}
		tne.names = append(tne.names, commonEvent.SchemaTableName{SchemaName: t.Schema.O, TableName: t.Name.O})
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	tne.ctes.leave(in)
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

// tableRenameVisitor rewrites table names in a DDL AST.
//
// TableName nodes are rewritten positionally in the same traversal order as
// extractTableNames. References to those tables are rewritten through the SELECT
// scope chain:
//
//   - `db`.`table`.`col` and `db`.`table`.* name a physical table directly, so
//     they use the qualified lookup.
//   - `table`.`col` and `table`.* resolve to a range variable. The innermost
//     SELECT wins; when it does not declare the name, the search continues in
//     the enclosing SELECT. An alias, a CTE name, or an ambiguous declaration
//     stops the search, because the reference does not denote a physical table.
//
// The resolution rules below are shared with the CREATE VIEW normalization in
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
//	Positional: {source_db, v} → {target_db, v_r}
//	            {source_db, t} → {target_db, t_r}
//
//	Schema-qualified column reference: `source_db`.`t`.`id`
//	    qualified lookup: {source_db, t} → {target_db, t_r}
//	    → `target_db`.`t_r`.`id`
//
//	Rewritten AST:
//	  CREATE VIEW `target_db`.`v_r` AS
//	    SELECT `target_db`.`t_r`.`id` FROM `target_db`.`t_r`
type tableRenameVisitor struct {
	ctes        cteScopes
	sourceNames []commonEvent.SchemaTableName
	// targetNames contains routed names aligned with tableNameExtractor output.
	targetNames []commonEvent.SchemaTableName
	// targetByQualifiedSource maps qualified source table names to routed names.
	targetByQualifiedSource map[commonEvent.SchemaTableName]commonEvent.SchemaTableName
	// caseSensitive keeps physical table names case-sensitive, like the router's
	// rule matching. Range variable aliases stay case-insensitive.
	caseSensitive bool
	// scope is the innermost SELECT scope being visited.
	scope *selectScope
	// bindings are table-qualified references, resolved after the whole walk.
	bindings []pendingBinding
	// i is the next targetNames index to consume.
	i int
	// hasErr records targetNames exhaustion because ast.Visitor cannot return an error.
	hasErr bool
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
	// tables maps a lower-cased unaliased physical table name to its routed name.
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

func (v *tableRenameVisitor) Enter(in ast.Node) (ast.Node, bool) {
	if v.hasErr {
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
		if v.ctes.contains(t) {
			return in, true
		}
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
	if v.hasErr {
		return in, false
	}
	v.ctes.leave(in)
	switch in.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		v.leaveScope()
	}
	return in, true
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

// collectWildCards records the table-qualified wildcards of one SELECT field
// list. WildCardField nodes are not visited by ast.Visitor, so they are read from
// the field list. A schema-qualified wildcard names a physical table and is
// rewritten immediately, like a schema-qualified column.
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
			if target, ok := v.targetByQualifiedSource[qualifiedSourceKey(wildcard.Schema.O, wildcard.Table.O, v.caseSensitive)]; ok {
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
	if !ok {
		return
	}
	if v.ctes.contains(sourceTable) {
		// A CTE reference is a range variable, not a physical table.
		v.addRangeVariable(sourceTable.Name.L)
		return
	}
	if v.i >= len(v.sourceNames) || v.i >= len(v.targetNames) {
		return
	}
	// The TableName immediately following this TableSource uses index i.
	source, target := v.sourceNames[v.i], v.targetNames[v.i]
	if source.TableName == "" {
		return
	}
	key := v.tableKey(source.TableName)
	if _, exists := v.scope.tables[key]; exists {
		delete(v.scope.tables, key)
		v.scope.ambiguousTables[key] = struct{}{}
		return
	}
	if _, ambiguous := v.scope.ambiguousTables[key]; ambiguous {
		return
	}
	v.scope.tables[key] = target
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

// rewriteColumnName rewrites only schema-qualified column references
// (e.g. `db`.`t`.`col`) to match routed table names.
func (v *tableRenameVisitor) rewriteColumnName(c *ast.ColumnName) {
	if c == nil || c.Schema.O == "" || c.Table.O == "" {
		return
	}

	target, ok := v.targetByQualifiedSource[qualifiedSourceKey(c.Schema.O, c.Table.O, v.caseSensitive)]
	if !ok {
		return
	}
	c.Schema = ast.NewCIStr(target.SchemaName)
	c.Table = ast.NewCIStr(target.TableName)
}

// newTableRenameVisitor builds the lookup map used for schema-qualified column
// references. It pairs each source table with its routed target.
func newTableRenameVisitor(
	sourceTables []commonEvent.SchemaTableName,
	targetTables []commonEvent.SchemaTableName,
	caseSensitive bool,
) *tableRenameVisitor {
	visitor := &tableRenameVisitor{
		sourceNames:             sourceTables,
		targetNames:             targetTables,
		caseSensitive:           caseSensitive,
		targetByQualifiedSource: make(map[commonEvent.SchemaTableName]commonEvent.SchemaTableName, len(sourceTables)),
	}

	for i, source := range sourceTables {
		if i >= len(targetTables) || source.TableName == "" {
			continue
		}
		target := targetTables[i]
		if source.SchemaName != "" {
			visitor.targetByQualifiedSource[qualifiedSourceKey(source.SchemaName, source.TableName, caseSensitive)] = target
		}
	}
	return visitor
}

// tableKey normalizes an unqualified physical table name. Table names follow the
// router's case sensitivity, so a case-sensitive router keeps `T` and `t`
// distinct. Aliases are not normalized here: they are matched through their
// lower-cased form, like SQL identifiers.
func (v *tableRenameVisitor) tableKey(name string) string {
	return normalizeIdentifier(name, v.caseSensitive)
}

// qualifiedSourceKey normalizes a schema-qualified table name for lookup.
func qualifiedSourceKey(schema, table string, caseSensitive bool) commonEvent.SchemaTableName {
	return commonEvent.SchemaTableName{
		SchemaName: normalizeIdentifier(schema, caseSensitive),
		TableName:  normalizeIdentifier(table, caseSensitive),
	}
}

// rewriteDDLStmtTables rewrites table names in a DDL AST.
// sourceTables and targetTables must have matching lengths and follow the
// traversal order produced by extractTableNames. TableName nodes are rewritten
// positionally. Column and wildcard references that name a table are rewritten
// through the SELECT scope chain, so schema-qualified, table-qualified, and
// correlated references all keep pointing at the routed table.
//
// Returned DDL uses StringSingleQuotes, KeyWordUppercase and NameBackQuotes.
func rewriteDDLStmtTables(
	stmt ast.StmtNode,
	sourceTables []commonEvent.SchemaTableName,
	targetTables []commonEvent.SchemaTableName,
	caseSensitive bool,
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
		visitor := newTableRenameVisitor(sourceTables, targetTables, caseSensitive)
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
		visitor.resolveBindings()
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
