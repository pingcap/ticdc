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

type schemaNameChange struct {
	from string
	to   string
}

// ddlRoutePlan contains the rewritten query and routing semantics that cannot
// be inferred from whether the query text changed.
type ddlRoutePlan struct {
	// query is the final DDL query after applying routing rules.
	query string
	// hasOutOfBandTableMetadata tells ApplyToDDLEvent to inspect table metadata
	// such as MultipleTableInfos even when query itself is unchanged.
	hasOutOfBandTableMetadata bool
	// tableNamePreservingSchemaChanges records schema name changes whose routed
	// tables must keep the same names before and after the schema change.
	tableNamePreservingSchemaChanges []schemaNameChange
}

// ddlRouteSpec describes the routing-relevant semantics extracted from one
// DDL AST before routing rules are applied.
type ddlRouteSpec struct {
	// sourceTables lists schema and table names extracted directly from the DDL
	// query. A schema-level entry has an empty table name.
	sourceTables []commonEvent.SchemaTableName
	// hasOutOfBandTableMetadata marks a DDL whose event can carry routable table
	// metadata, such as MultipleTableInfos, that is not named in the query.
	hasOutOfBandTableMetadata bool
	// tableNamePreservingSchemaChanges records schema name changes whose tables
	// must resolve to the same routed table names on both sides.
	tableNamePreservingSchemaChanges []schemaNameChange
}

// rewriteParserBackedDDLQuery rewrites a parser-supported DDL query by applying routing rules.
func (r Router) rewriteParserBackedDDLQuery(ddl *commonEvent.DDLEvent) (string, error) {
	plan, err := r.buildParserBackedDDLRoutePlan(ddl)
	if err != nil {
		return "", err
	}
	return plan.query, nil
}

func (r Router) buildParserBackedDDLRoutePlan(ddl *commonEvent.DDLEvent) (ddlRoutePlan, error) {
	if len(r.rules) == 0 {
		return ddlRoutePlan{query: ddl.Query}, nil
	}

	queries, err := splitMultiStmtDDLQuery(ddl.Query)
	if err != nil {
		return ddlRoutePlan{}, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	var (
		builder strings.Builder
		plan    ddlRoutePlan
		routed  bool
	)
	for i := range queries {
		query := queries[i]
		singlePlan, err := r.buildSingleDDLRoutePlan(query, ddl.GetSchemaName())
		if err != nil {
			return ddlRoutePlan{}, err
		}
		plan.hasOutOfBandTableMetadata = plan.hasOutOfBandTableMetadata ||
			singlePlan.hasOutOfBandTableMetadata
		plan.tableNamePreservingSchemaChanges = append(
			plan.tableNamePreservingSchemaChanges,
			singlePlan.tableNamePreservingSchemaChanges...)
		if singlePlan.query != query {
			routed = true
			query = singlePlan.query
		}
		builder.WriteString(query)
		if len(queries) > 1 && !strings.HasSuffix(query, ";") {
			builder.WriteByte(';')
		}
	}
	if !routed {
		plan.query = ddl.Query
		return plan, nil
	}

	plan.query = builder.String()
	return plan, nil
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
	plan, err := r.buildSingleDDLRoutePlan(query, defaultSchema)
	if err != nil {
		return "", err
	}
	return plan.query, nil
}

func (r Router) buildSingleDDLRoutePlan(query string, defaultSchema string) (ddlRoutePlan, error) {
	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return ddlRoutePlan{}, errors.WrapError(errors.ErrTableRoutingFailed, err)
	}

	spec := extractDDLRouteSpec(stmt)
	plan := ddlRoutePlan{
		query:                            query,
		hasOutOfBandTableMetadata:        spec.hasOutOfBandTableMetadata,
		tableNamePreservingSchemaChanges: spec.tableNamePreservingSchemaChanges,
	}
	sourceTables := spec.sourceTables
	if len(sourceTables) == 0 {
		return plan, nil
	}
	fillDefaultSchema(sourceTables, defaultSchema)

	var (
		routed       bool
		targetTables = make([]commonEvent.SchemaTableName, 0, len(sourceTables))
	)
	for _, srcTable := range sourceTables {
		binding, err := r.Route(srcTable.SchemaName, srcTable.TableName)
		if err != nil {
			return ddlRoutePlan{}, err
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
		return plan, nil
	}

	newQuery, err := rewriteDDLStmtTables(stmt, sourceTables, targetTables)
	if err != nil {
		return ddlRoutePlan{}, err
	}
	plan.query = newQuery
	return plan, nil
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
	return extractDDLRouteSpec(stmt).sourceTables
}

func extractDDLRouteSpec(stmt ast.StmtNode) ddlRouteSpec {
	// Special cases: schema related SQLs don't have tableName
	switch v := stmt.(type) {
	case *ast.AlterDatabaseStmt:
		return ddlRouteSpec{sourceTables: []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}}
	case *ast.CreateDatabaseStmt:
		return ddlRouteSpec{sourceTables: []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}}
	case *ast.DropDatabaseStmt:
		return ddlRouteSpec{sourceTables: []commonEvent.SchemaTableName{{SchemaName: v.Name.O, TableName: ""}}}
	case *ast.FlashBackDatabaseStmt:
		spec := ddlRouteSpec{
			sourceTables: []commonEvent.SchemaTableName{{SchemaName: v.DBName.O, TableName: ""}},
			// FLASHBACK DATABASE restores tables that are carried by the DDL
			// event metadata but are not named in the query.
			hasOutOfBandTableMetadata: true,
		}
		if v.NewName != "" {
			spec.sourceTables = append(
				spec.sourceTables,
				commonEvent.SchemaTableName{SchemaName: v.NewName, TableName: ""})
			spec.tableNamePreservingSchemaChanges = []schemaNameChange{{
				from: v.DBName.O,
				to:   v.NewName,
			}}
		}
		return spec
	}

	e := &tableNameExtractor{
		names: make([]commonEvent.SchemaTableName, 0),
	}
	stmt.Accept(e)

	return ddlRouteSpec{sourceTables: e.names}
}

// tableRenameVisitor rewrites table names in a DDL AST.
//
// TableName nodes are rewritten positionally in the same traversal order as
// extractTableNames. For CREATE VIEW, TiDB represents `db`.`table`.`column` as
// a ColumnName node, so the visitor also rewrites the schema/table qualifier
// when it is explicitly schema-qualified. Unqualified qualifiers such as
// `table`.`column` may be aliases and are left unchanged.
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
	// targetNames contains routed names aligned with tableNameExtractor output.
	targetNames []commonEvent.SchemaTableName
	// targetByQualifiedSource maps qualified source table names to routed names.
	targetByQualifiedSource map[commonEvent.SchemaTableName]commonEvent.SchemaTableName
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
	c.Schema = ast.NewCIStr(target.SchemaName)
	c.Table = ast.NewCIStr(target.TableName)
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
	case *ast.FlashBackDatabaseStmt:
		expectedCount := 1
		if v.NewName != "" {
			expectedCount = 2
		}
		if len(targetTables) != expectedCount {
			return "", errors.ErrTableRoutingFailed.GenWithStack(
				"rewrite ddl query got unexpected target table count: expected %d, got %d",
				expectedCount, len(targetTables))
		}
		for i, targetTable := range targetTables {
			if i == 0 {
				v.DBName = ast.NewCIStr(targetTable.SchemaName)
			} else {
				v.NewName = targetTable.SchemaName
			}
		}
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
