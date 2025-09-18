// Copyright 2024 PingCAP, Inc.
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
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap/zapcore"
)

type tsPair struct {
	startTs  uint64
	commitTs uint64
}

type preparedDMLs struct {
	sqls            []string
	values          [][]interface{}
	rowCount        int
	approximateSize int64
	tsPairs         []tsPair
}

func (d *preparedDMLs) LogDebug(events []*commonEvent.DMLEvent) {
	if log.GetLevel() != zapcore.DebugLevel {
		return
	}

	// Calculate total count
	totalCount := len(d.sqls)

	if len(d.sqls) == 0 {
		log.Debug("No SQL statements to log")
		return
	}

	// Build complete log content in a single string
	var logBuilder strings.Builder
	logBuilder.WriteString(fmt.Sprintf("Total SQL Count: %d, Row Count: %d :", totalCount, d.rowCount))

	// Build SQL statements and arguments section
	for i, sql := range d.sqls {
		var args []interface{}
		if i < len(d.values) {
			args = d.values[i]
		}

		// Format the arguments as a string
		argsStr := "("
		for j, arg := range args {
			if j > 0 {
				argsStr += ", "
			}
			if arg == nil {
				argsStr += "NULL"
			} else if str, ok := arg.(string); ok {
				argsStr += fmt.Sprintf(`"%s"`, str)
			} else {
				argsStr += fmt.Sprintf("%v", arg)
			}
		}
		argsStr += ")"

		// Add formatted SQL and args to log content
		logBuilder.WriteString(fmt.Sprintf("[%03d] Query: %s,", i+1, sql))
		logBuilder.WriteString(fmt.Sprintf("      Args: %s,", argsStr))
	}

	// Build timestamp information
	commitTsList := make([]uint64, len(events))
	startTsList := make([]uint64, len(events))
	for i, event := range events {
		commitTsList[i] = event.GetCommitTs()
		startTsList[i] = event.GetStartTs()
	}

	logBuilder.WriteString(fmt.Sprintf("CommitTs: %v,", commitTsList))
	logBuilder.WriteString(fmt.Sprintf("StartTs:  %v,", startTsList))
	logBuilder.WriteString("End")

	// Output the complete log content in a single call
	log.Debug(logBuilder.String())
}

func (d *preparedDMLs) String() string {
	return fmt.Sprintf("sqls: %v, values: %v, rowCount: %d, approximateSize: %d, startTs: %v", d.fmtSqls(), d.values, d.rowCount, d.approximateSize, d.tsPairs)
}

func (d *preparedDMLs) fmtSqls() string {
	builder := strings.Builder{}
	for _, sql := range d.sqls {
		builder.WriteString(sql)
		builder.WriteString(";")
	}
	return builder.String()
}

var dmlsPool = sync.Pool{
	New: func() interface{} {
		return &preparedDMLs{
			sqls:    make([]string, 0, 128),
			values:  make([][]interface{}, 0, 128),
			tsPairs: make([]tsPair, 0, 128),
		}
	},
}

func (d *preparedDMLs) reset() {
	d.sqls = d.sqls[:0]
	d.values = d.values[:0]
	d.tsPairs = d.tsPairs[:0]
	d.rowCount = 0
	d.approximateSize = 0
}

// prepareReplace builds a parametric REPLACE statement as following
// sql: `REPLACE INTO `test`.`t` VALUES (?,?,?)`
func buildInsert(
	tableInfo *common.TableInfo,
	row commonEvent.RowChange,
	inSafeMode bool,
) (string, []interface{}) {
	args := getArgs(&row.Row, tableInfo)
	if len(args) == 0 {
		return "", nil
	}

	var sql string
	if inSafeMode {
		sql = tableInfo.GetPreReplaceSQL()
	} else {
		sql = tableInfo.GetPreInsertSQL()
	}

	if sql == "" {
		log.Panic("PreInsertSQL should not be empty")
	}

	return sql, args
}

// prepareDelete builds a parametric DELETE statement as following
// sql: `DELETE FROM `test`.`t` WHERE x = ? AND y >= ? LIMIT 1`
func buildDelete(tableInfo *common.TableInfo, row commonEvent.RowChange, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	quoteTable := tableInfo.TableName.QuoteString()
	builder.WriteString("DELETE FROM ")
	builder.WriteString(quoteTable)
	builder.WriteString(" WHERE ")

	colNames, whereArgs := whereSlice(&row.PreRow, tableInfo, forceReplicate)
	if len(whereArgs) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(whereArgs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if whereArgs[i] == nil {
			builder.WriteString(common.QuoteName(colNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(common.QuoteName(colNames[i]))
			builder.WriteString(" = ?")
			args = append(args, whereArgs[i])
		}
	}
	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func buildUpdate(tableInfo *common.TableInfo, row commonEvent.RowChange, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	if tableInfo.GetPreUpdateSQL() == "" {
		log.Panic("PreUpdateSQL should not be empty")
	}
	builder.WriteString(tableInfo.GetPreUpdateSQL())

	args := getArgs(&row.Row, tableInfo)
	if len(args) == 0 {
		return "", nil
	}

	whereColNames, whereArgs := whereSlice(&row.PreRow, tableInfo, forceReplicate)
	if len(whereArgs) == 0 {
		return "", nil
	}

	builder.WriteString(" WHERE ")
	for i := 0; i < len(whereColNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if whereArgs[i] == nil {
			builder.WriteString(common.QuoteName(whereColNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(common.QuoteName(whereColNames[i]))
			builder.WriteString(" = ?")
			args = append(args, whereArgs[i])
		}
	}

	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func getArgs(row *chunk.Row, tableInfo *common.TableInfo) []interface{} {
	args := make([]interface{}, 0, len(tableInfo.GetColumns()))
	for i, col := range tableInfo.GetColumns() {
		if col == nil || col.IsGenerated() {
			continue
		}
		v := common.ExtractColVal(row, col, i)
		args = append(args, v)
	}
	return args
}

func getArgsWithGeneratedColumn(row *chunk.Row, tableInfo *common.TableInfo) []interface{} {
	args := make([]interface{}, 0, len(tableInfo.GetColumns()))
	for i, col := range tableInfo.GetColumns() {
		if col == nil {
			continue
		}
		v := common.ExtractColVal(row, col, i)
		args = append(args, v)
	}
	return args
}

// whereSlice returns the column names and values for the WHERE clause
func whereSlice(row *chunk.Row, tableInfo *common.TableInfo, forceReplicate bool) ([]string, []interface{}) {
	args := make([]interface{}, 0, len(tableInfo.GetColumns()))
	colNames := make([]string, 0, len(tableInfo.GetColumns()))
	// Try to use unique key values when available
	for i, col := range tableInfo.GetColumns() {
		if col == nil || !tableInfo.IsHandleKey(col.ID) {
			continue
		}
		colNames = append(colNames, col.Name.O)
		v := common.ExtractColVal(row, col, i)
		args = append(args, v)
	}

	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 && forceReplicate {
		for i, col := range tableInfo.GetColumns() {
			colNames = append(colNames, col.Name.O)
			v := common.ExtractColVal(row, col, i)
			args = append(args, v)
		}
	}
	return colNames, args
}
