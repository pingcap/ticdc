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

// Package sqlname binds table qualifiers in parser ASTs. It covers physical
// table declarations, aliases, CTEs, SELECT/set-operation scopes and correlated
// table references, including derived and lateral tables. It does not resolve
// columns, infer types or plan queries.
package sqlname

import (
	"maps"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// Name identifies a source or target table. Source spellings are resolved by
// the caller against the catalog before routing; SQL binding always folds case.
type Name struct{ Schema, Table string }

func (n Name) folded() Name {
	return Name{Schema: strings.ToLower(n.Schema), Table: strings.ToLower(n.Table)}
}

// Resolver resolves one physical table to its canonical source or routed name.
// Aliases and CTE references are never passed to it. Errors abort rewriting
// before any AST mutation.
type Resolver func(Name) (Name, error)

// Bindings retains the source declarations and the references bound to them.
// Bind first, then Apply: AST mutation cannot change subsequent resolution.
type Bindings struct {
	tables []*tableBinding
	refs   []reference
}

type tableBinding struct {
	source Name
	node   *ast.TableName
}

type reference struct {
	table    *tableBinding
	column   *ast.ColumnName
	wildcard *ast.WildCardField
}

// Bind finds physical tables and binds qualified columns and wildcards to their
// FROM declarations without modifying node. The default schema resolves only
// unqualified physical tables, never CTE names or aliases.
func Bind(node ast.Node, defaultSchema string) *Bindings {
	v := &binder{defaultSchema: defaultSchema, declarations: make(map[*ast.TableName]*tableBinding)}
	node.Accept(v)
	for _, pending := range v.pending {
		if table := resolve(pending.scope, pending.name); table != nil {
			v.bindings.refs = append(v.bindings.refs, reference{table: table, column: pending.column, wildcard: pending.wildcard})
		}
	}
	return &v.bindings
}

// Apply computes all target names before changing declarations or references.
// A nil resolver keeps source names and qualifies bound references. The result
// reports any AST name change, including qualification alone.
func (b *Bindings) Apply(resolve Resolver) (bool, error) {
	targets := make(map[*tableBinding]Name, len(b.tables))
	for _, table := range b.tables {
		target := table.source
		if resolve != nil {
			var err error
			target, err = resolve(table.source)
			if err != nil {
				return false, err
			}
		}
		targets[table] = target
	}
	changed := false
	set := func(schema, table *ast.CIStr, target Name) {
		if schema.O != target.Schema || table.O != target.Table {
			*schema, *table = ast.NewCIStr(target.Schema), ast.NewCIStr(target.Table)
			changed = true
		}
	}
	for _, table := range b.tables {
		set(&table.node.Schema, &table.node.Name, targets[table])
	}
	for _, ref := range b.refs {
		target := targets[ref.table]
		if ref.column != nil {
			set(&ref.column.Schema, &ref.column.Table, target)
		} else {
			set(&ref.wildcard.Schema, &ref.wildcard.Table, target)
		}
	}
	return changed, nil
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

// scope holds SQL visibility, independently of the AST traversal stack.
type scope struct {
	parent    *scope
	aliases   map[string]struct{}
	tables    map[Name]*tableBinding
	ambiguous map[Name]struct{}
}

type pendingReference struct {
	scope    *scope
	name     Name
	column   *ast.ColumnName
	wildcard *ast.WildCardField
}

type binder struct {
	defaultSchema string
	ctes          cteScopes
	scope         *scope
	scopeStack    []*scope
	declarations  map[*ast.TableName]*tableBinding
	bindings      Bindings
	pending       []pendingReference
}

func (v *binder) enterScope() {
	v.scope = &scope{parent: v.scope, aliases: make(map[string]struct{}), tables: make(map[Name]*tableBinding), ambiguous: make(map[Name]struct{})}
}

func (v *binder) Enter(in ast.Node) (ast.Node, bool) {
	v.ctes.enter(in)
	switch n := in.(type) {
	case *ast.SelectStmt:
		v.enterScope()
		if n.Fields != nil {
			for _, field := range n.Fields.Fields {
				if w := field.WildCard; w != nil && w.Table.O != "" {
					v.pending = append(v.pending, pendingReference{scope: v.scope, name: Name{w.Schema.L, w.Table.L}, wildcard: w})
				}
			}
		}
	case *ast.SetOprStmt, *ast.SetOprSelectList:
		v.enterScope()
	case *ast.WithClause:
		// Definitions see true outer queries, but not their consumer's FROM.
		// CTE name visibility is tracked separately and remains intact.
		v.scopeStack = append(v.scopeStack, v.scope)
		v.scope = v.scope.parent
	case *ast.TableSource:
		consumer := v.scope
		v.scopeStack = append(v.scopeStack, consumer)
		visible := consumer
		switch n.Source.(type) {
		case *ast.SelectStmt, *ast.SetOprStmt:
			if consumer != nil {
				visible = consumer.parent
				if n.Lateral {
					// Capture only preceding FROM items. Later declarations and
					// this derived table's own alias are not visible inside it.
					visible = &scope{
						parent: consumer.parent, aliases: maps.Clone(consumer.aliases),
						tables: maps.Clone(consumer.tables), ambiguous: maps.Clone(consumer.ambiguous),
					}
				}
			}
		}
		v.collectTable(n)
		v.scope = visible
	case *ast.TableName:
		if !v.ctes.contains(n) {
			v.declare(n)
		}
		return in, true
	case *ast.ColumnName:
		if n.Table.O != "" {
			v.pending = append(v.pending, pendingReference{scope: v.scope, name: Name{n.Schema.L, n.Table.L}, column: n})
		}
		return in, true
	}
	return in, false
}

func (v *binder) Leave(in ast.Node) (ast.Node, bool) {
	v.ctes.leave(in)
	switch in.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt, *ast.SetOprSelectList:
		v.scope = v.scope.parent
	case *ast.WithClause, *ast.TableSource:
		v.scope = v.scopeStack[len(v.scopeStack)-1]
		v.scopeStack = v.scopeStack[:len(v.scopeStack)-1]
	}
	return in, true
}

func (v *binder) declare(node *ast.TableName) *tableBinding {
	if table, ok := v.declarations[node]; ok {
		return table
	}
	source := Name{node.Schema.O, node.Name.O}
	if source.Schema == "" {
		source.Schema = v.defaultSchema
	}
	table := &tableBinding{source: source, node: node}
	v.declarations[node] = table
	v.bindings.tables = append(v.bindings.tables, table)
	return table
}

func (v *binder) collectTable(node *ast.TableSource) {
	if v.scope == nil {
		return
	}
	if node.AsName.O != "" {
		v.scope.aliases[node.AsName.L] = struct{}{}
		return
	}
	source, ok := node.Source.(*ast.TableName)
	if !ok || source.Name.O == "" {
		return
	}
	if v.ctes.contains(source) {
		v.scope.aliases[source.Name.L] = struct{}{}
		return
	}
	table := v.declare(source)
	name := table.source.folded()
	keys := []Name{{Table: name.Table}}
	if name.Schema != "" {
		keys = append(keys, name)
	}
	for _, key := range keys {
		if _, exists := v.scope.tables[key]; exists {
			delete(v.scope.tables, key)
			v.scope.ambiguous[key] = struct{}{}
		} else if _, ambiguous := v.scope.ambiguous[key]; !ambiguous {
			v.scope.tables[key] = table
		}
	}
}

func resolve(s *scope, name Name) *tableBinding {
	for ; s != nil; s = s.parent {
		if name.Schema == "" {
			if _, alias := s.aliases[name.Table]; alias {
				return nil
			}
		}
		if _, ambiguous := s.ambiguous[name]; ambiguous {
			return nil
		}
		if table, ok := s.tables[name]; ok {
			return table
		}
	}
	return nil
}
