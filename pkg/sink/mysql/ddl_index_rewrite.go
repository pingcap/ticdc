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

package mysql

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
)

type indexKeyPart struct {
	nameL  string
	length int
}

func restoreAnonymousIndexToNamedIndex(query string, tableInfo *common.TableInfo) (string, bool, error) {
	if query == "" || tableInfo == nil {
		return query, false, nil
	}

	p := parser.New()
	stmt, err := p.ParseOneStmt(query, "", "")
	if err != nil {
		return query, false, errors.Trace(err)
	}

	alterStmt, ok := stmt.(*ast.AlterTableStmt)
	if !ok {
		return query, false, nil
	}

	changed := false
	for _, spec := range alterStmt.Specs {
		if spec == nil || spec.Tp != ast.AlterTableAddConstraint || spec.Constraint == nil {
			continue
		}
		constraint := spec.Constraint
		if constraint.Name != "" {
			continue
		}
		if !isIndexConstraint(constraint) {
			continue
		}

		indexName, ok := findIndexNameForConstraint(tableInfo, constraint)
		if !ok {
			continue
		}
		constraint.Name = indexName
		changed = true
	}

	if !changed {
		return query, false, nil
	}

	restoredQuery, err := restoreDDLStmt(stmt)
	if err != nil {
		return query, false, err
	}
	return restoredQuery, true, nil
}

func isIndexConstraint(constraint *ast.Constraint) bool {
	if constraint == nil {
		return false
	}
	switch constraint.Tp {
	case ast.ConstraintKey,
		ast.ConstraintIndex,
		ast.ConstraintUniq,
		ast.ConstraintUniqKey,
		ast.ConstraintUniqIndex,
		ast.ConstraintFulltext,
		ast.ConstraintVector,
		ast.ConstraintColumnar:
		return true
	default:
		return false
	}
}

func isUniqueIndexConstraint(constraint *ast.Constraint) bool {
	if constraint == nil {
		return false
	}
	switch constraint.Tp {
	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		return true
	default:
		return false
	}
}

func findIndexNameForConstraint(tableInfo *common.TableInfo, constraint *ast.Constraint) (string, bool) {
	keyParts, ok := getConstraintKeyParts(constraint)
	if !ok {
		return "", false
	}

	wantUnique := isUniqueIndexConstraint(constraint)
	indices := tableInfo.GetIndices()
	if len(indices) == 0 {
		return "", false
	}

	matches := make([]*timodel.IndexInfo, 0, 1)
	for _, index := range indices {
		if index == nil || index.Primary {
			continue
		}
		if index.Unique != wantUnique {
			continue
		}
		// For `ADD INDEX` jobs, TableInfo may contain indices in non-public states.
		// Only use public indices to avoid selecting transient metadata.
		if index.State != timodel.StatePublic {
			continue
		}
		if !indexColumnsMatchKeyParts(index.Columns, keyParts) {
			continue
		}
		matches = append(matches, index)
	}
	if len(matches) != 1 {
		return "", false
	}
	return matches[0].Name.O, true
}

func getConstraintKeyParts(constraint *ast.Constraint) ([]indexKeyPart, bool) {
	if constraint == nil || len(constraint.Keys) == 0 {
		return nil, false
	}

	parts := make([]indexKeyPart, 0, len(constraint.Keys))
	for _, key := range constraint.Keys {
		if key == nil || key.Expr != nil || key.Column == nil {
			return nil, false
		}
		parts = append(parts, indexKeyPart{
			nameL:  key.Column.Name.L,
			length: key.Length,
		})
	}
	return parts, true
}

func indexColumnsMatchKeyParts(indexColumns []*timodel.IndexColumn, keyParts []indexKeyPart) bool {
	if len(indexColumns) != len(keyParts) {
		return false
	}
	for i, part := range keyParts {
		col := indexColumns[i]
		if col == nil {
			return false
		}
		if col.Name.L != part.nameL {
			return false
		}
		if part.length > 0 {
			if col.Length != part.length {
				return false
			}
		}
	}
	return true
}

func restoreDDLStmt(stmt ast.StmtNode) (string, error) {
	var sb strings.Builder
	restoreFlags := format.RestoreTiDBSpecialComment |
		format.RestoreNameBackQuotes |
		format.RestoreKeyWordUppercase |
		format.RestoreStringSingleQuotes |
		format.SkipPlacementRuleForRestore |
		format.RestoreWithTTLEnableOff
	if err := stmt.Restore(format.NewRestoreCtx(restoreFlags, &sb)); err != nil {
		return "", errors.Trace(err)
	}
	return sb.String(), nil
}
