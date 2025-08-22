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

package txnsink

import (
	"strings"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/sqlmodel"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// SQLGenerator handles SQL generation for transaction groups
type SQLGenerator struct{}

// NewSQLGenerator creates a new SQL generator
func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{}
}

// ConvertTxnGroupToSQL converts a transaction group to SQL statements
func (g *SQLGenerator) ConvertTxnGroupToSQL(txnGroup *TxnGroup) (*TxnSQL, error) {
	// Group events by table for batch processing
	tableEvents := make(map[int64][]*commonEvent.DMLEvent)
	for _, event := range txnGroup.Events {
		tableID := event.GetTableID()
		tableEvents[tableID] = append(tableEvents[tableID], event)
	}

	var allSQLs []string
	var allArgs [][]interface{}

	// Process each table's events
	for _, events := range tableEvents {
		if len(events) == 0 {
			continue
		}

		// Generate SQL for this table's events
		sqls, args, err := g.generateTableSQL(events)
		if err != nil {
			return nil, err
		}

		allSQLs = append(allSQLs, sqls...)
		allArgs = append(allArgs, args...)
	}

	// Wrap in transaction
	if len(allSQLs) > 0 {
		transactionSQL := "BEGIN;" + strings.Join(allSQLs, ";") + ";COMMIT;"
		transactionArgs := make([]interface{}, 0)
		for _, arg := range allArgs {
			transactionArgs = append(transactionArgs, arg...)
		}

		return &TxnSQL{
			TxnGroup: txnGroup,
			SQLs:     []string{transactionSQL},
			Keys:     txnGroup.ExtractKeys(),
		}, nil
	}

	return &TxnSQL{
		TxnGroup: txnGroup,
		SQLs:     []string{},
		Keys:     txnGroup.ExtractKeys(),
	}, nil
}

// generateTableSQL generates SQL statements for events of the same table
func (g *SQLGenerator) generateTableSQL(events []*commonEvent.DMLEvent) ([]string, [][]interface{}, error) {
	if len(events) == 0 {
		return []string{}, [][]interface{}{}, nil
	}

	tableInfo := events[0].TableInfo

	// Group rows by type (insert, update, delete)
	insertRows, updateRows, deleteRows := g.groupRowsByType(events, tableInfo)

	var sqls []string
	var args [][]interface{}

	// Handle delete operations
	if len(deleteRows) > 0 {
		for _, rows := range deleteRows {
			sql, value := g.genDeleteSQL(rows...)
			sqls = append(sqls, sql)
			args = append(args, value)
		}
	}

	// Handle update operations - use INSERT ON DUPLICATE KEY UPDATE
	if len(updateRows) > 0 {
		for _, rows := range updateRows {
			sql, value := g.genInsertOnDuplicateUpdateSQL(rows...)
			sqls = append(sqls, sql)
			args = append(args, value)
		}
	}

	// Handle insert operations - use INSERT ON DUPLICATE KEY UPDATE
	if len(insertRows) > 0 {
		for _, rows := range insertRows {
			sql, value := g.genInsertOnDuplicateUpdateSQL(rows...)
			sqls = append(sqls, sql)
			args = append(args, value)
		}
	}

	return sqls, args, nil
}

// groupRowsByType groups rows by their type (insert, update, delete)
func (g *SQLGenerator) groupRowsByType(events []*commonEvent.DMLEvent, tableInfo *common.TableInfo) (
	insertRows, updateRows, deleteRows [][]*sqlmodel.RowChange,
) {
	insertRow := make([]*sqlmodel.RowChange, 0)
	updateRow := make([]*sqlmodel.RowChange, 0)
	deleteRow := make([]*sqlmodel.RowChange, 0)

	for _, event := range events {
		event.Rewind()
		for {
			row, ok := event.GetNextRow()
			if !ok {
				break
			}

			switch row.RowType {
			case commonEvent.RowTypeInsert:
				args := g.getArgsWithGeneratedColumn(&row.Row, tableInfo)
				newInsertRow := sqlmodel.NewRowChange(
					&tableInfo.TableName,
					nil,
					nil,
					args,
					tableInfo,
					nil, nil)
				insertRow = append(insertRow, newInsertRow)

			case commonEvent.RowTypeUpdate:
				args := g.getArgsWithGeneratedColumn(&row.Row, tableInfo)
				preArgs := g.getArgsWithGeneratedColumn(&row.PreRow, tableInfo)
				newUpdateRow := sqlmodel.NewRowChange(
					&tableInfo.TableName,
					nil,
					preArgs,
					args,
					tableInfo,
					nil, nil)
				updateRow = append(updateRow, newUpdateRow)

			case commonEvent.RowTypeDelete:
				preArgs := g.getArgsWithGeneratedColumn(&row.PreRow, tableInfo)
				newDeleteRow := sqlmodel.NewRowChange(
					&tableInfo.TableName,
					nil,
					preArgs,
					nil,
					tableInfo,
					nil, nil)
				deleteRow = append(deleteRow, newDeleteRow)
			}
		}
	}

	// Group rows into batches
	if len(insertRow) > 0 {
		insertRows = append(insertRows, insertRow)
	}
	if len(updateRow) > 0 {
		updateRows = append(updateRows, updateRow)
	}
	if len(deleteRow) > 0 {
		deleteRows = append(deleteRows, deleteRow)
	}

	return
}

// genDeleteSQL generates DELETE SQL for multiple rows
func (g *SQLGenerator) genDeleteSQL(rows ...*sqlmodel.RowChange) (string, []interface{}) {
	return sqlmodel.GenDeleteSQL(rows...)
}

// genInsertOnDuplicateUpdateSQL generates INSERT ON DUPLICATE KEY UPDATE SQL
func (g *SQLGenerator) genInsertOnDuplicateUpdateSQL(rows ...*sqlmodel.RowChange) (string, []interface{}) {
	return sqlmodel.GenInsertSQL(sqlmodel.DMLInsertOnDuplicateUpdate, rows...)
}

// getArgsWithGeneratedColumn extracts column values including generated columns
func (g *SQLGenerator) getArgsWithGeneratedColumn(row *chunk.Row, tableInfo *common.TableInfo) []interface{} {
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
