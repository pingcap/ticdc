// Copyright 2022 PingCAP, Inc.
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

const (
	// CommonIndexColumnsCount means common columns count of an index, index contains 1, 2,
	// , 3 or 4 columns are common, but index contains 5 columns or more are not that common,
	// so we use 4 as the common index column count. It will be used to pre-allocate slice space.
	CommonIndexColumnsCount = 4
	// DefaultWhereClause is the default strategy for generating WHERE predicates in multi-row DML.
	//
	// "v2" uses the compact `(col1,col2) IN ((?,?),(?,?))` form for better performance. It is
	// not able to match rows when any key column is NULL (see TODOs in v2 generators).
	// "v1" uses the `(... ) OR (... )` form, which can handle NULL keys at the cost of longer SQL.
	DefaultWhereClause = "v2"
)

// GenDeleteSQL generates the DELETE SQL and its arguments.
// Input `changes` should have same target table and same columns for WHERE
// (typically same PK/NOT NULL UK), otherwise the behaviour is undefined.
// whereClause selects the WHERE predicate strategy, see DefaultWhereClause.
func GenDeleteSQL(whereClause string, changes ...*RowChange) (string, []interface{}) {
	// Keep empty as default to make the strategy stable even if callers don't
	// explicitly set whereClause (for example, older configs/tests).
	if whereClause == "" || whereClause == DefaultWhereClause {
		return genDeleteSQLV2(changes...)
	}
	return genDeleteSQLV1(changes...)
}

// GenUpdateSQL generates the UPDATE SQL and its arguments.
// Input `changes` should have same target table and same columns for WHERE
// (typically same PK/NOT NULL UK), otherwise the behaviour is undefined.
// whereClause selects the WHERE predicate strategy, see DefaultWhereClause.
func GenUpdateSQL(whereClause string, changes ...*RowChange) (string, []any) {
	// Keep empty as default to make the strategy stable even if callers don't
	// explicitly set whereClause (for example, older configs/tests).
	if whereClause == "" || whereClause == DefaultWhereClause {
		return genUpdateSQLV2(changes...)
	}
	return genUpdateSQLV1(changes...)
}

// GenInsertSQL generates the INSERT SQL and its arguments.
// Input `changes` should have same target table and same modifiable columns,
// otherwise the behaviour is undefined.
func GenInsertSQL(tp DMLType, changes ...*RowChange) (string, []interface{}) {
	if len(changes) == 0 {
		log.Panic("row changes is empty")
		return "", nil
	}

	first := changes[0]

	var buf strings.Builder
	buf.Grow(1024)
	if tp == DMLReplace {
		buf.WriteString("REPLACE INTO ")
	} else {
		buf.WriteString("INSERT INTO ")
	}
	buf.WriteString(first.targetTable.QuoteString())
	buf.WriteString(" (")
	columnNum := 0
	var skipColIdx []int

	// build generated columns lower name set to accelerate the following check
	generatedColumns := generatedColumnsNameSet(first.targetTableInfo.GetColumns())
	for i, col := range first.sourceTableInfo.GetColumns() {
		if _, ok := generatedColumns[col.Name.L]; ok {
			skipColIdx = append(skipColIdx, i)
			continue
		}

		if columnNum != 0 {
			buf.WriteByte(',')
		}
		columnNum++
		buf.WriteString(common.QuoteName(col.Name.O))
	}
	buf.WriteString(") VALUES ")
	holder := valuesHolder(columnNum)
	for i := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}
	if tp == DMLInsertOnDuplicateUpdate {
		buf.WriteString(" ON DUPLICATE KEY UPDATE ")
		i := 0 // used as index of skipColIdx
		writtenFirstCol := false

		for j, col := range first.sourceTableInfo.GetColumns() {
			if i < len(skipColIdx) && skipColIdx[i] == j {
				i++
				continue
			}

			if writtenFirstCol {
				buf.WriteByte(',')
			}
			writtenFirstCol = true

			colName := common.QuoteName(col.Name.O)
			buf.WriteString(colName + "=VALUES(" + colName + ")")
		}
	}

	args := make([]interface{}, 0, len(changes)*(len(first.sourceTableInfo.GetColumns())-len(skipColIdx)))
	for _, change := range changes {
		i := 0 // used as index of skipColIdx
		for j, val := range change.postValues {
			if i >= len(skipColIdx) {
				args = append(args, change.postValues[j:]...)
				break
			}
			if skipColIdx[i] == j {
				i++
				continue
			}
			args = append(args, val)
		}
	}
	return buf.String(), args
}

func genDeleteSQLV2(changes ...*RowChange) (string, []interface{}) {
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

	whereColumns, _ := first.whereColumnsAndValues()
	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString(common.QuoteName(column) + ",")
		} else {
			buf.WriteString(common.QuoteName(column) + ")")
		}
	}
	buf.WriteString(" IN (")
	args := make([]interface{}, 0, len(changes)*len(whereColumns))
	holder := valuesHolder(len(whereColumns))
	for i, change := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
		_, whereValues := change.whereColumnsAndValues()
		// a simple check about different number of WHERE values, not trying to
		// cover all cases
		if len(whereValues) != len(whereColumns) {
			log.Panic("len(whereValues) != len(whereColumns)",
				zap.Int("len(whereValues)", len(whereValues)),
				zap.Int("len(whereColumns)", len(whereColumns)),
				zap.String("whereValues", util.RedactArgs(whereValues)),
				zap.Stringer("sourceTable", change.sourceTable))
			return "", nil
		}
		args = append(args, whereValues...)
	}
	buf.WriteString(")")
	return buf.String(), args
}

func genUpdateSQLV2(changes ...*RowChange) (string, []any) {
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

	// Pre-generate essential sub statements used after WHEN, WHERE.
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

	// Generate WHERE (...) IN (...)
	buf.WriteString(" WHERE (")

	for i, column := range whereColumns {
		if i != len(whereColumns)-1 {
			buf.WriteString(common.QuoteName(column) + ",")
		} else {
			buf.WriteString(common.QuoteName(column) + ")")
		}
	}
	buf.WriteString(" IN (")
	holder := valuesHolder(len(whereColumns))
	for i := range changes {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(holder)
	}
	buf.WriteString(")")

	// Build args of the UPDATE SQL.
	//
	// The generated SQL is roughly:
	//   UPDATE t SET c1 = CASE WHEN <where1> THEN ? WHEN <where2> THEN ? END,
	//                c2 = CASE WHEN <where1> THEN ? WHEN <where2> THEN ? END
	//   WHERE (<pk cols>) IN ((...),(...)...)
	//
	// Since each `<whereX>` contains placeholders, args are grouped by column:
	// for each assignable column and each row, append `[where values..., post value]`.
	// At the end, append all WHERE values again for the trailing IN (...) predicate.
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
