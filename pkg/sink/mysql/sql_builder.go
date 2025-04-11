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
	"github.com/pingcap/tiflow/pkg/quotes"
)

type preparedDMLs struct {
	sqls            []string
	values          [][]interface{}
	rowCount        int
	approximateSize int64
	startTs         []uint64
}

func (d *preparedDMLs) String() string {
	return fmt.Sprintf("sqls: %v, values: %v, rowCount: %d, approximateSize: %d, startTs: %v", d.fmtSqls(), d.values, d.rowCount, d.approximateSize, d.startTs)
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
			startTs: make([]uint64, 0, 128),
		}
	},
}

func (d *preparedDMLs) reset() {
	d.sqls = d.sqls[:0]
	d.values = d.values[:0]
	d.startTs = d.startTs[:0]
	d.rowCount = 0
	d.approximateSize = 0
}

// prepareReplace builds a parametric REPLACE statement as following
// sql: `REPLACE INTO `test`.`t` VALUES (?,?,?)`
func buildInsert(
	tableInfo *common.TableInfo,
	row commonEvent.RowChange,
	translateToInsert bool,
) (string, []interface{}) {
	args := getArgs(&row.Row, tableInfo, false)
	if len(args) == 0 {
		return "", nil
	}

	var sql string
	if translateToInsert {
		sql = tableInfo.GetPreInsertSQL()
	} else {
		sql = tableInfo.GetPreReplaceSQL()
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
			builder.WriteString(quotes.QuoteName(colNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]))
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

	args := getArgs(&row.Row, tableInfo, false)
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
			builder.WriteString(quotes.QuoteName(whereColNames[i]))
			builder.WriteString(" IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(whereColNames[i]))
			builder.WriteString(" = ?")
			args = append(args, whereArgs[i])
		}
	}

	builder.WriteString(" LIMIT 1")
	sql := builder.String()
	return sql, args
}

func getArgs(row *chunk.Row, tableInfo *common.TableInfo, enableGeneratedColumn bool) []interface{} {
	args := make([]interface{}, 0, len(tableInfo.GetColumns()))
	for i, col := range tableInfo.GetColumns() {
		if col == nil || (col.IsGenerated() && !enableGeneratedColumn) {
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
