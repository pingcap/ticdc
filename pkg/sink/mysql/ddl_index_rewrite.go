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
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

func restoreAnonymousIndexToNamedIndex(query string, tableInfo *common.TableInfo, indexIDs []int64) (string, bool, error) {
	if query == "" || tableInfo == nil || len(indexIDs) == 0 {
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

	indexNameByID := getIndexNameByIDMap(tableInfo)
	if len(indexNameByID) == 0 {
		return query, false, nil
	}

	indexConstraints := make([]*ast.Constraint, 0)
	for _, spec := range alterStmt.Specs {
		if spec == nil || spec.Tp != ast.AlterTableAddConstraint || spec.Constraint == nil {
			continue
		}
		constraint := spec.Constraint
		if !isIndexConstraint(constraint) {
			continue
		}
		indexConstraints = append(indexConstraints, constraint)
	}
	if len(indexConstraints) == 0 {
		return query, false, nil
	}

	changed := false
	for i, constraint := range indexConstraints {
		if i >= len(indexIDs) {
			break
		}
		if constraint.Name != "" {
			continue
		}
		indexName, ok := indexNameByID[indexIDs[i]]
		if !ok {
			continue
		}
		constraint.Name = indexName
		changed = true
	}

	if !changed {
		return query, false, nil
	}

	restoredQuery, err := commonEvent.Restore(stmt)
	if err != nil {
		return query, false, err
	}
	return restoredQuery, true, nil
}

func getIndexNameByIDMap(tableInfo *common.TableInfo) map[int64]string {
	indices := tableInfo.GetIndices()
	if len(indices) == 0 {
		return nil
	}
	indexNameByID := make(map[int64]string, len(indices))
	for _, index := range indices {
		if index == nil {
			continue
		}
		indexNameByID[index.ID] = index.Name.O
	}
	return indexNameByID
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
