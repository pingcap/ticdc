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

package sqlmodel

import (
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// V1 multi-row DML generators expand WHERE predicates as `(cond1) OR (cond2) ...`.
//
// Compared with the v2 IN-based form, this form produces longer SQL, but it can
// correctly match rows when any key column is NULL because RowChange.genWhere can
// emit `IS NULL` predicates.
func genDeleteSQLV1(changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.Panic("row changes is empty")
		return "", nil
	}

	first := changes[0]

	var buf strings.Builder
	buf.Grow(1024)
	buf.WriteString("DELETE FROM ")
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" WHERE (")

	allArgs := make([]interface{}, 0, len(changes)*CommonIndexColumnsCount)

	for i, c := range changes {
		if i > 0 {
			buf.WriteString(") OR (")
		}
		// Each RowChange generates its own WHERE predicate. Append args in the same order
		// as the predicates are concatenated.
		args := c.genWhere(&buf)
		allArgs = append(allArgs, args...)
	}
	buf.WriteString(")")
	return buf.String(), allArgs
}

func genUpdateSQLV1(changes ...*RowChange) (string, []any) {
	if len(changes) == 0 {
		log.Panic("row changes is empty")
		return "", nil
	}
	var buf strings.Builder
	buf.Grow(1024)

	// Generate UPDATE `db`.`table` SET
	first := changes[0]
	buf.WriteString("UPDATE ")
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" SET ")

	// Pre-generate essential sub statements used after WHEN and in the final WHERE.
	// Each entry in whenCaseStmts is a full predicate like:
	//   `pk1` = ? AND `pk2` IS ?
	var (
		whenCaseStmts = make([]string, len(changes))
		whenCaseArgs  = make([][]interface{}, len(changes))
	)
	whereColumns, _ := first.whereColumnsAndValues()

	var whereBuf strings.Builder
	for i, c := range changes {
		whereBuf.Reset()
		whereBuf.Grow(128)
		whenCaseArgs[i] = c.genWhere(&whereBuf)
		whenCaseStmts[i] = whereBuf.String()
	}

	// Build generated columns lower name set to accelerate the following check
	targetGeneratedColSet := generatedColumnsNameSet(first.targetTableInfo.GetColumns())

	// Generate `ColumnName`=CASE WHEN .. THEN .. END
	// Use this value in order to identify which is the first CaseWhenThen line,
	// because generated column can happen any where and it will be skipped.
	isFirstCaseWhenThenLine := true
	for _, column := range first.targetTableInfo.GetColumns() {
		// skip generated columns
		if _, ok := targetGeneratedColSet[column.Name.L]; ok {
			continue
		}
		if !isFirstCaseWhenThenLine {
			// insert ", " after END of each lines except for the first line.
			buf.WriteString(", ")
		}

		buf.WriteString(common.QuoteName(column.Name.String()) + "=CASE")
		for i := range changes {
			buf.WriteString(" WHEN ")
			buf.WriteString(whenCaseStmts[i])
			buf.WriteString(" THEN ?")
		}
		buf.WriteString(" END")
		isFirstCaseWhenThenLine = false
	}

	// Generate WHERE (...) OR (...)
	buf.WriteString(" WHERE (")
	for i, s := range whenCaseStmts {
		if i > 0 {
			buf.WriteString(") OR (")
		}
		buf.WriteString(s)
	}
	buf.WriteString(")")

	// Build args of the UPDATE SQL.
	//
	// The generated SQL is roughly:
	//   UPDATE t SET c1 = CASE WHEN <where1> THEN ? WHEN <where2> THEN ? END,
	//                c2 = CASE WHEN <where1> THEN ? WHEN <where2> THEN ? END
	//   WHERE (<where1>) OR (<where2>)
	//
	// Since each `<whereX>` contains placeholders, args are grouped by column:
	// for each assignable column and each row, append `[where values..., post value]`.
	// At the end, append all WHERE values again for the trailing WHERE clause.
	var assignValueColumnCount int
	var skipColIdx []int
	for i, col := range first.sourceTableInfo.GetColumns() {
		if _, ok := targetGeneratedColSet[col.Name.L]; ok {
			skipColIdx = append(skipColIdx, i)
			continue
		}
		assignValueColumnCount++
	}
	whereValuesAtTheEnd := make([]any, 0, len(changes)*len(whereColumns))
	args := make([]any, 0,
		assignValueColumnCount*len(changes)*(len(whereColumns)+1)+len(whereValuesAtTheEnd))
	argsPerCol := make([][]any, assignValueColumnCount)
	for i := 0; i < assignValueColumnCount; i++ {
		argsPerCol[i] = make([]any, 0, len(changes)*(len(whereColumns)+1))
	}
	for i, change := range changes {
		whereValues := whenCaseArgs[i]
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.Panic("len(whereValues) != len(whereColumns)",
				zap.Int("len(whereValues)", len(whereValues)),
				zap.Int("len(whereColumns)", len(whereColumns)),
				zap.String("whereValues", util.RedactArgs(whereValues)),
				zap.Stringer("sourceTable", change.sourceTable))
		}

		whereValuesAtTheEnd = append(whereValuesAtTheEnd, whereValues...)

		i := 0 // used as index of skipColIdx
		writeableCol := 0
		for j, val := range change.postValues {
			if i < len(skipColIdx) && skipColIdx[i] == j {
				i++
				continue
			}
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], whereValues...)
			argsPerCol[writeableCol] = append(argsPerCol[writeableCol], val)
			writeableCol++
		}
	}
	for _, a := range argsPerCol {
		args = append(args, a...)
	}
	args = append(args, whereValuesAtTheEnd...)

	return buf.String(), args
}
