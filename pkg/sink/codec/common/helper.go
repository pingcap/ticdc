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
	"strconv"
	"strings"
	"unsafe"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	ptypes "github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
)

// GetMySQLType get the mysql type from column info
func GetMySQLType(columnInfo *model.ColumnInfo, fullType bool) string {
	if !fullType {
		result := types.TypeToStr(columnInfo.GetType(), columnInfo.GetCharset())
		result = withUnsigned4MySQLType(result, mysql.HasUnsignedFlag(columnInfo.GetFlag()))
		result = withZerofill4MySQLType(result, mysql.HasZerofillFlag(columnInfo.GetFlag()))
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

// IsBinaryMySQLType return true if the given mysqlType string is a binary type
func IsBinaryMySQLType(mysqlType string) bool {
	return strings.Contains(mysqlType, "blob") || strings.Contains(mysqlType, "binary")
}

// ExtractBasicMySQLType return the mysql type
func ExtractBasicMySQLType(mysqlType string) byte {
	mysqlType = strings.TrimPrefix(mysqlType, "unsigned ")
	for i := 0; i < len(mysqlType); i++ {
		if mysqlType[i] == '(' || mysqlType[i] == ' ' {
			return ptypes.StrToType(mysqlType[:i])
		}
	}
	return ptypes.StrToType(mysqlType)
}

func IsUnsignedMySQLType(mysqlType string) bool {
	return strings.Contains(mysqlType, "unsigned")
}

func ExtractFlenDecimal(mysqlType string, tp byte) (int, int) {
	if strings.HasPrefix(mysqlType, "enum") || strings.HasPrefix(mysqlType, "set") {
		return 0, 0
	}
	start := strings.Index(mysqlType, "(")
	end := strings.Index(mysqlType, ")")
	if start == -1 || end == -1 {
		if strings.HasPrefix("mysqlType", "bit") {
			return 8, 0
		}
		return mysql.GetDefaultFieldLengthAndDecimal(tp)
	}

	data := strings.Split(mysqlType[start+1:end], ",")
	flen, err := strconv.ParseInt(data[0], 10, 64)
	if err != nil {
		log.Panic("parse flen failed", zap.String("flen", data[0]), zap.Error(err))
	}

	if len(data) != 2 {
		return int(flen), types.UnspecifiedLength
	}

	decimal, err := strconv.ParseInt(data[1], 10, 64)
	if err != nil {
		log.Panic("parse decimal failed", zap.String("decimal", data[1]), zap.Error(err))
	}
	return int(flen), int(decimal)
}

// ExtractElements for the Enum and Set Type
func ExtractElements(mysqlType string) []string {
	start := strings.Index(mysqlType, "(")
	end := strings.LastIndex(mysqlType, ")")
	if start == -1 || end == -1 {
		return nil
	}
	parts := strings.Split(mysqlType[start+1:end], ",")
	elements := make([]string, 0, len(parts))
	for _, part := range parts {
		elements = append(elements, strings.Trim(part, "'"))
	}
	return elements
}

func ExtractDecimal(mysqlType string) int {
	start := strings.Index(mysqlType, "(")
	end := strings.Index(mysqlType, ")")
	if start == -1 || end == -1 {
		return 0
	}
	decimal := mysqlType[start+1 : end]
	result, err := strconv.ParseInt(decimal, 10, 64)
	if err != nil {
		log.Panic("parse decimal failed", zap.String("decimal", decimal), zap.Error(err))
	}
	return int(result)
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
	ctx context.Context, db *sql.DB, event *commonEvent.DMLEvent,
) error {
	var (
		schema   = event.TableInfo.GetSchemaName()
		table    = event.TableInfo.GetTableName()
		commitTs = event.GetCommitTs()
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

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		columns := event.TableInfo.GetColumns()
		if row.Checksum.Current != 0 {
			conditions := make(map[string]any)
			for _, name := range pkNames {
				for idx, col := range columns {
					if col.Name.O == name {
						d := row.Row.GetDatum(idx, &col.FieldType)
						conditions[name] = d.GetValue()
					}
				}
			}
			result := queryRowChecksumAux(ctx, conn, commitTs, schema, table, conditions)
			if result != 0 && result != row.Checksum.Current {
				log.Error("verify upstream TiDB columns-level checksum, current checksum mismatch",
					zap.Uint32("expected", row.Checksum.Current),
					zap.Uint32("actual", result))
				return errors.New("checksum mismatch")
			}
		}

		if row.Checksum.Previous != 0 {
			conditions := make(map[string]any)
			for _, name := range pkNames {
				for idx, col := range columns {
					if col.Name.O == name {
						d := row.PreRow.GetDatum(idx, &col.FieldType)
						conditions[name] = d.GetValue()
					}
				}
			}
			result := queryRowChecksumAux(ctx, conn, commitTs-1, schema, table, conditions)
			if result != 0 && result != row.Checksum.Previous {
				log.Error("verify upstream TiDB columns-level checksum, previous checksum mismatch",
					zap.Uint32("expected", row.Checksum.Previous),
					zap.Uint32("actual", result))
				return errors.New("checksum mismatch")
			}
		}
	}
	return nil
}

func queryRowChecksumAux(
	ctx context.Context, conn *sql.Conn, commitTs uint64, schema string, table string, conditions map[string]any,
) uint32 {
	var result uint32
	// 1. set snapshot read
	query := fmt.Sprintf("set @@tidb_snapshot=%d", commitTs)
	_, err := conn.ExecContext(ctx, query)
	if err != nil {
		mysqlErr, ok := errors.Cause(err).(*mysqlDriver.MySQLError)
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
		mysqlErr, ok := errors.Cause(err).(*mysqlDriver.MySQLError)
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
func MustBinaryLiteralToInt(bytes []byte) uint64 {
	val, err := types.BinaryLiteral(bytes).ToInt(types.DefaultStmtNoWarningContext)
	if err != nil {
		log.Panic("invalid bit value found", zap.ByteString("value", bytes))
		return math.MaxUint64
	}
	return val
}

const (
	replacementChar = "_"
	numberPrefix    = 'x'
)

// EscapeEnumAndSetOptions escapes ",", "\" and "”"
// https://github.com/debezium/debezium/blob/9f7ede0e0695f012c6c4e715e96aed85eecf6b5f/debezium-connector-mysql/src/main/java/io/debezium/connector/mysql/antlr/MySqlAntlrDdlParser.java#L374
func EscapeEnumAndSetOptions(option string) string {
	option = strings.ReplaceAll(option, ",", "\\,")
	option = strings.ReplaceAll(option, "\\'", "'")
	option = strings.ReplaceAll(option, "''", "'")
	return option
}

func isValidFirstCharacter(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func isValidNonFirstCharacter(c rune) bool {
	return isValidFirstCharacter(c) || (c >= '0' && c <= '9')
}

func isValidNonFirstCharacterForTopicName(c rune) bool {
	return isValidNonFirstCharacter(c) || c == '.'
}

// SanitizeName escapes not permitted chars
// https://avro.apache.org/docs/1.12.0/specification/#names
// see https://github.com/debezium/debezium/blob/main/debezium-core/src/main/java/io/debezium/schema/SchemaNameAdjuster.java
func SanitizeName(name string) string {
	changed := false
	var sb strings.Builder
	for i, c := range name {
		if i == 0 && !isValidFirstCharacter(c) {
			sb.WriteString(replacementChar)
			if c >= '0' && c <= '9' {
				sb.WriteRune(c)
			}
			changed = true
		} else if !isValidNonFirstCharacter(c) {
			sb.WriteString(replacementChar)
			changed = true
		} else {
			sb.WriteRune(c)
		}
	}

	sanitizedName := sb.String()
	if changed {
		log.Warn(
			"Name is potentially not safe for serialization, replace it",
			zap.String("name", name),
			zap.String("replacedName", sanitizedName),
		)
	}
	return sanitizedName
}

// SanitizeTopicName escapes not permitted chars for topic name
// https://github.com/debezium/debezium/blob/main/debezium-api/src/main/java/io/debezium/spi/topic/TopicNamingStrategy.java
func SanitizeTopicName(name string) string {
	changed := false
	var sb strings.Builder
	for _, c := range name {
		if !isValidNonFirstCharacterForTopicName(c) {
			sb.WriteString(replacementChar)
			changed = true
		} else {
			sb.WriteRune(c)
		}
	}

	sanitizedName := sb.String()
	if changed {
		log.Warn(
			"Table name sanitize",
			zap.String("name", name),
			zap.String("replacedName", sanitizedName),
		)
	}
	return sanitizedName
}

// UnsafeBytesToString create string from byte slice without copying
func UnsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// UnsafeStringToBytes create byte slice from string without copying
func UnsafeStringToBytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}
