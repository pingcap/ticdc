// Copyright 2023 PingCAP, Inc.
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

package common

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	pMySQL "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"go.uber.org/zap"
)

// GetMySQLType get the mysql type from column info
func GetMySQLType(columnInfo *model.ColumnInfo, fullType bool) string {
	if !fullType {
		result := types.TypeToStr(columnInfo.GetType(), columnInfo.GetCharset())
		result = withUnsigned4MySQLType(result, pMySQL.HasUnsignedFlag(columnInfo.GetFlag()))
		result = withZerofill4MySQLType(result, pMySQL.HasZerofillFlag(columnInfo.GetFlag()))
		return result
	}
	return columnInfo.GetTypeDesc()
}

// when encoding the canal format, for unsigned mysql type, add `unsigned` keyword.
// it should have the form `t unsigned`, such as `int unsigned`
func withUnsigned4MySQLType(mysqlType string, unsigned bool) string {
	if unsigned && mysqlType != "bit" && mysqlType != "year" {
		return mysqlType + " unsigned"
	}
	return mysqlType
}

func withZerofill4MySQLType(mysqlType string, zerofill bool) string {
	if zerofill && !strings.HasPrefix(mysqlType, "year") {
		return mysqlType + " zerofill"
	}
	return mysqlType
}

// ColumnsHolder read columns from sql.Rows
type ColumnsHolder struct {
	Values        []interface{}
	ValuePointers []interface{}
	Types         []*sql.ColumnType
}

func newColumnHolder(rows *sql.Rows) (*ColumnsHolder, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, errors.Trace(err)
	}

	values := make([]interface{}, len(columnTypes))
	valuePointers := make([]interface{}, len(columnTypes))
	for i := range values {
		valuePointers[i] = &values[i]
	}

	return &ColumnsHolder{
		Values:        values,
		ValuePointers: valuePointers,
		Types:         columnTypes,
	}, nil
}

// Length return the column count
func (h *ColumnsHolder) Length() int {
	return len(h.Values)
}

// MustQueryTimezone query the timezone from the upstream database
func MustQueryTimezone(ctx context.Context, db *sql.DB) string {
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Panic("establish connection to the upstream tidb failed", zap.Error(err))
	}
	defer conn.Close()

	var timezone string
	query := "SELECT @@global.time_zone"
	err = conn.QueryRowContext(ctx, query).Scan(&timezone)
	if err != nil {
		log.Panic("query timezone failed", zap.Error(err))
	}

	log.Info("query global timezone from the upstream tidb",
		zap.Any("timezone", timezone))
	return timezone
}

func queryRowChecksum(
	ctx context.Context, db *sql.DB, event *commonEvent.RowChangedEvent,
) error {
	var (
		schema   = event.TableInfo.GetSchemaName()
		table    = event.TableInfo.GetTableName()
		commitTs = event.CommitTs
	)

	pkNames := event.TableInfo.GetPrimaryKeyColumnNames()
	if len(pkNames) == 0 {
		log.Warn("cannot query row checksum without primary key",
			zap.String("schema", schema), zap.String("table", table))
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Panic("establish connection to the upstream tidb failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	defer conn.Close()

	if event.Checksum.Current != 0 {
		conditions := make(map[string]interface{})
		for _, name := range pkNames {
			for _, col := range event.Columns {
				colID := event.TableInfo.ForceGetColumnIDByName(col.Name)
				if event.TableInfo.ForceGetColumnName(colID) == name {
					conditions[name] = col.Value
				}
			}
		}
		result := queryRowChecksumAux(ctx, conn, commitTs, schema, table, conditions)
		if result != 0 && result != event.Checksum.Current {
			log.Error("verify upstream TiDB columns-level checksum, current checksum mismatch",
				zap.Uint32("expected", event.Checksum.Current),
				zap.Uint32("actual", result))
			return errors.New("checksum mismatch")
		}
	}

	if event.Checksum.Previous != 0 {
		conditions := make(map[string]interface{})
		for _, name := range pkNames {
			for _, col := range event.PreColumns {
				colID := event.TableInfo.ForceGetColumnIDByName(col.Name)
				if event.TableInfo.ForceGetColumnName(colID) == name {
					conditions[name] = col.Value
				}
			}
		}
		result := queryRowChecksumAux(ctx, conn, commitTs-1, schema, table, conditions)
		if result != 0 && result != event.Checksum.Previous {
			log.Error("verify upstream TiDB columns-level checksum, previous checksum mismatch",
				zap.Uint32("expected", event.Checksum.Previous),
				zap.Uint32("actual", result))
			return errors.New("checksum mismatch")
		}
	}

	return nil
}

func queryRowChecksumAux(
	ctx context.Context, conn *sql.Conn, commitTs uint64, schema string, table string, conditions map[string]interface{},
) uint32 {
	var result uint32
	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
		if ok {
			// Error 8055 (HY000): snapshot is older than GC safe point
			if mysqlErr.Number == 8055 {
				log.Error("set snapshot read failed, since snapshot is older than GC safe point")
			}
		}

		log.Error("set snapshot read failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
		return result
	}

	query = fmt.Sprintf("select tidb_row_checksum() from %s.%s where ", schema, table)
	var whereClause string
	for name, value := range conditions {
		if whereClause != "" {
			whereClause += " and "
		}
		switch value.(type) {
		case []byte, string:
			whereClause += fmt.Sprintf("%s = '%v'", name, value)
		default:
			whereClause += fmt.Sprintf("%s = %v", name, value)
		}
	}
	query += whereClause

	err = conn.QueryRowContext(ctx, query).Scan(&result)
	if err != nil {
		log.Panic("scan row failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	return result
}

// MustSnapshotQuery query the db by the snapshot read with the given commitTs
func MustSnapshotQuery(
	ctx context.Context, db *sql.DB, commitTs uint64, schema, table string, conditions map[string]interface{},
) *ColumnsHolder {
	conn, err := db.Conn(ctx)
	if err != nil {
		log.Panic("establish connection to the upstream tidb failed",
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	defer conn.Close()

	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	_, err = conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
		if ok {
			// Error 8055 (HY000): snapshot is older than GC safe point
			if mysqlErr.Number == 8055 {
				log.Error("set snapshot read failed, since snapshot is older than GC safe point")
			}
		}

		log.Panic("set snapshot read failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}

	// 2. query the whole row
	query = fmt.Sprintf("select * from %s.%s where ", schema, table)
	var whereClause string
	for name, value := range conditions {
		if whereClause != "" {
			whereClause += " and "
		}
		whereClause += fmt.Sprintf("%s = %v", name, value)
	}
	query += whereClause

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		log.Panic("query row failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	defer rows.Close()

	holder, err := newColumnHolder(rows)
	if err != nil {
		log.Panic("obtain the columns holder failed",
			zap.String("query", query),
			zap.String("schema", schema), zap.String("table", table),
			zap.Uint64("commitTs", commitTs), zap.Error(err))
	}
	for rows.Next() {
		err = rows.Scan(holder.ValuePointers...)
		if err != nil {
			log.Panic("scan row failed",
				zap.String("query", query),
				zap.String("schema", schema), zap.String("table", table),
				zap.Uint64("commitTs", commitTs), zap.Error(err))
		}
	}
	return holder
}

// MustBinaryLiteralToInt convert bytes into uint64,
// by follow https://github.com/pingcap/tidb/blob/e3417913f58cdd5a136259b902bf177eaf3aa637/types/binary_literal.go#L105
func MustBinaryLiteralToInt(bytes []byte) uint64 {
	bytes = trimLeadingZeroBytes(bytes)
	length := len(bytes)

	if length > 8 {
		log.Panic("invalid bit value found", zap.ByteString("value", bytes))
		return math.MaxUint64
	}

	if length == 0 {
		return 0
	}

	// Note: the byte-order is BigEndian.
	val := uint64(bytes[0])
	for i := 1; i < length; i++ {
		val = (val << 8) | uint64(bytes[i])
	}
	return val
}

func trimLeadingZeroBytes(bytes []byte) []byte {
	if len(bytes) == 0 {
		return bytes
	}
	pos, posMax := 0, len(bytes)-1
	for ; pos < posMax; pos++ {
		if bytes[pos] != 0 {
			break
		}
	}
	return bytes[pos:]
}

// FakeTableIDAllocator is a fake table id allocator
type FakeTableIDAllocator struct {
	tableIDs       map[string]int64
	currentTableID int64
}

// NewFakeTableIDAllocator creates a new FakeTableIDAllocator
func NewFakeTableIDAllocator() *FakeTableIDAllocator {
	return &FakeTableIDAllocator{
		tableIDs: make(map[string]int64),
	}
}

func (g *FakeTableIDAllocator) allocateByKey(key string) int64 {
	if tableID, ok := g.tableIDs[key]; ok {
		return tableID
	}
	g.currentTableID++
	g.tableIDs[key] = g.currentTableID
	return g.currentTableID
}

// AllocateTableID allocates a table id
func (g *FakeTableIDAllocator) AllocateTableID(schema, table string) int64 {
	key := fmt.Sprintf("`%s`.`%s`", commonType.EscapeName(schema), commonType.EscapeName(table))
	return g.allocateByKey(key)
}

// AllocatePartitionID allocates a partition id
func (g *FakeTableIDAllocator) AllocatePartitionID(schema, table, name string) int64 {
	key := fmt.Sprintf("`%s`.`%s`.`%s`", commonType.EscapeName(schema), commonType.EscapeName(table), commonType.EscapeName(name))
	return g.allocateByKey(key)
}
