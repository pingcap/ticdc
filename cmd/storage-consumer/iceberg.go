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

package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cmd/util"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	tidbTypes "github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

func buildIcebergDDLEvents(
	prev *sinkiceberg.TableVersion,
	curr *sinkiceberg.TableVersion,
) ([]*commonEvent.DDLEvent, error) {
	if curr == nil {
		return nil, fmt.Errorf("iceberg table version is nil")
	}

	events := make([]*commonEvent.DDLEvent, 0, 4)
	if prev == nil {
		tableInfo, err := buildIcebergTableInfo(curr)
		if err != nil {
			return nil, err
		}
		events = append(events,
			newIcebergDDLEvent(
				curr.SchemaName,
				"",
				fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", commonType.QuoteName(curr.SchemaName)),
				nil,
				model.ActionCreateSchema,
				uint64(curr.MetadataVersion),
			),
			newIcebergDDLEvent(
				curr.SchemaName,
				curr.TableName,
				fmt.Sprintf(
					"CREATE TABLE IF NOT EXISTS %s (%s)",
					commonType.QuoteSchema(curr.SchemaName, curr.TableName),
					strings.Join(icebergCreateTableSQLDefs(curr.Columns), ","),
				),
				tableInfo,
				model.ActionCreateTable,
				uint64(curr.MetadataVersion),
			),
		)
		return events, nil
	}

	prevByName := make(map[string]sinkiceberg.Column, len(prev.Columns))
	for _, col := range prev.Columns {
		prevByName[col.Name] = col
	}
	currByName := make(map[string]sinkiceberg.Column, len(curr.Columns))
	for _, col := range curr.Columns {
		currByName[col.Name] = col
	}

	for _, col := range prev.Columns {
		if _, ok := currByName[col.Name]; ok {
			continue
		}
		events = append(events, newIcebergDDLEvent(
			curr.SchemaName,
			curr.TableName,
			fmt.Sprintf(
				"ALTER TABLE %s DROP COLUMN %s",
				commonType.QuoteSchema(curr.SchemaName, curr.TableName),
				commonType.QuoteName(col.Name),
			),
			nil,
			model.ActionDropColumn,
			uint64(curr.MetadataVersion),
		))
	}
	for _, col := range curr.Columns {
		if _, ok := prevByName[col.Name]; ok {
			continue
		}
		events = append(events, newIcebergDDLEvent(
			curr.SchemaName,
			curr.TableName,
			fmt.Sprintf(
				"ALTER TABLE %s ADD COLUMN %s",
				commonType.QuoteSchema(curr.SchemaName, curr.TableName),
				icebergColumnSQLDef(col),
			),
			nil,
			model.ActionAddColumn,
			uint64(curr.MetadataVersion),
		))
	}
	for _, currCol := range curr.Columns {
		prevCol, ok := prevByName[currCol.Name]
		if !ok {
			continue
		}
		if prevCol.Type == currCol.Type && prevCol.Required == currCol.Required {
			continue
		}
		events = append(events, newIcebergDDLEvent(
			curr.SchemaName,
			curr.TableName,
			fmt.Sprintf(
				"ALTER TABLE %s MODIFY COLUMN %s",
				commonType.QuoteSchema(curr.SchemaName, curr.TableName),
				icebergColumnSQLDef(currCol),
			),
			nil,
			model.ActionModifyColumn,
			uint64(curr.MetadataVersion),
		))
	}
	return events, nil
}

func buildIcebergDMLEvent(
	version *sinkiceberg.TableVersion,
	tableID int64,
	row sinkiceberg.ChangeRow,
) (*commonEvent.DMLEvent, error) {
	if version == nil {
		return nil, fmt.Errorf("iceberg table version is nil")
	}

	tableInfo, err := buildIcebergTableInfo(version)
	if err != nil {
		return nil, err
	}

	commitTs, err := strconv.ParseUint(strings.TrimSpace(row.CommitTs), 10, 64)
	if err != nil {
		return nil, err
	}
	startTs := commitTs
	if startTs > 0 {
		startTs--
	}

	dml := commonEvent.NewDMLEvent(commonType.NewDispatcherID(), tableID, startTs, commitTs, tableInfo)
	dml.TableInfoVersion = uint64(version.MetadataVersion)
	if strings.EqualFold(strings.TrimSpace(row.Op), "U") {
		dml.ReplicatingTs = commitTs + 1
	} else {
		dml.ReplicatingTs = commitTs
	}

	rows := chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1)
	dml.SetRows(rows)
	if err := appendIcebergChunkRow(rows, tableInfo, row.Columns); err != nil {
		return nil, err
	}

	switch strings.ToUpper(strings.TrimSpace(row.Op)) {
	case "I", "U":
		dml.RowTypes = append(dml.RowTypes, commonType.RowTypeInsert)
	case "D":
		dml.RowTypes = append(dml.RowTypes, commonType.RowTypeDelete)
	default:
		return nil, fmt.Errorf("unsupported iceberg row op: %s", row.Op)
	}
	dml.Length = 1
	return dml, nil
}

func buildIcebergTableInfo(version *sinkiceberg.TableVersion) (*commonType.TableInfo, error) {
	definition := cloudstorage.TableDefinition{
		Schema:       version.SchemaName,
		Table:        version.TableName,
		Columns:      make([]cloudstorage.TableCol, 0, len(version.Columns)),
		TotalColumns: len(version.Columns),
	}
	for _, col := range version.Columns {
		defCol, err := icebergColumnToTableCol(col)
		if err != nil {
			return nil, err
		}
		definition.Columns = append(definition.Columns, defCol)
	}

	tableInfo, err := definition.ToTableInfo()
	if err != nil {
		return nil, err
	}
	tableInfo.UpdateTS = uint64(version.MetadataVersion)
	return tableInfo, nil
}

func newIcebergDDLEvent(
	schemaName string,
	tableName string,
	query string,
	tableInfo *commonType.TableInfo,
	actionType model.ActionType,
	finishedTs uint64,
) *commonEvent.DDLEvent {
	return &commonEvent.DDLEvent{
		Version:    commonEvent.DDLEventVersion1,
		Type:       byte(actionType),
		SchemaName: schemaName,
		TableName:  tableName,
		Query:      query,
		TableInfo:  tableInfo,
		FinishedTs: finishedTs,
		BlockedTables: &commonEvent.InfluencedTables{
			InfluenceType: commonEvent.InfluenceTypeAll,
		},
	}
}

func icebergColumnSQLDefs(columns []sinkiceberg.Column) []string {
	defs := make([]string, 0, len(columns))
	for _, col := range columns {
		defs = append(defs, icebergColumnSQLDef(col))
	}
	return defs
}

func icebergCreateTableSQLDefs(columns []sinkiceberg.Column) []string {
	defs := icebergColumnSQLDefs(columns)
	if primaryKey := icebergPrimaryKeySQLDef(columns); primaryKey != "" {
		defs = append(defs, primaryKey)
	}
	return defs
}

func icebergPrimaryKeySQLDef(columns []sinkiceberg.Column) string {
	primaryKeyColumns := make([]string, 0, len(columns))
	for _, col := range columns {
		if !icebergColumnIsPrimaryKey(col) {
			continue
		}
		primaryKeyColumns = append(primaryKeyColumns, commonType.QuoteName(col.Name))
	}
	if len(primaryKeyColumns) == 0 {
		return ""
	}
	return fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeyColumns, ","))
}

func icebergColumnIsPrimaryKey(col sinkiceberg.Column) bool {
	return col.OriginalTableCol != nil && strings.EqualFold(strings.TrimSpace(col.OriginalTableCol.IsPK), "true")
}

func icebergColumnSQLDef(col sinkiceberg.Column) string {
	return fmt.Sprintf("%s %s %s",
		commonType.QuoteName(col.Name),
		icebergColumnSQLType(col),
		icebergColumnNullability(col.Required),
	)
}

func icebergColumnNullability(required bool) string {
	if required {
		return "NOT NULL"
	}
	return "NULL"
}

func icebergColumnSQLType(col sinkiceberg.Column) string {
	if sqlType, ok := originalIcebergColumnSQLType(col); ok {
		return sqlType
	}

	switch strings.TrimSpace(col.Type) {
	case "int":
		return "INT"
	case "long":
		return "BIGINT"
	case "float":
		return "FLOAT"
	case "double":
		return "DOUBLE"
	case "date":
		return "DATE"
	case "timestamp":
		return "DATETIME(6)"
	case "binary":
		return "LONGBLOB"
	default:
		if precision, scale, ok := parseIcebergDecimalType(col.Type); ok {
			return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale)
		}
		return "TEXT"
	}
}

func icebergColumnToTableCol(col sinkiceberg.Column) (cloudstorage.TableCol, error) {
	if original := cloneOriginalIcebergTableCol(col); original != nil {
		return *original, nil
	}

	result := cloudstorage.TableCol{
		Name: col.Name,
	}
	if col.Required {
		result.Nullable = "false"
	}

	switch strings.TrimSpace(col.Type) {
	case "int":
		result.Tp = "INT"
	case "long":
		result.Tp = "BIGINT"
	case "float":
		result.Tp = "FLOAT"
	case "double":
		result.Tp = "DOUBLE"
	case "date":
		result.Tp = "DATE"
	case "timestamp":
		result.Tp = "DATETIME"
		result.Scale = "6"
	case "binary":
		result.Tp = "BLOB"
	default:
		if precision, scale, ok := parseIcebergDecimalType(col.Type); ok {
			result.Tp = "DECIMAL"
			result.Precision = strconv.Itoa(precision)
			result.Scale = strconv.Itoa(scale)
			return result, nil
		}
		result.Tp = "VARCHAR"
	}
	return result, nil
}

func originalIcebergColumnSQLType(col sinkiceberg.Column) (string, bool) {
	if col.OriginalTableCol == nil {
		return "", false
	}

	switch strings.ToUpper(strings.TrimSpace(col.OriginalTableCol.Tp)) {
	case "CHAR", "VARCHAR", "VAR_STRING",
		"BINARY", "VARBINARY",
		"TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT",
		"TINYBLOB", "BLOB", "MEDIUMBLOB", "LONGBLOB",
		"ENUM", "SET":
		return formatTableColSQLType(*cloneOriginalIcebergTableCol(col)), true
	default:
		return "", false
	}
}

func cloneOriginalIcebergTableCol(col sinkiceberg.Column) *cloudstorage.TableCol {
	if col.OriginalTableCol == nil {
		return nil
	}

	cloned := *col.OriginalTableCol
	cloned.Name = col.Name
	if col.Required {
		cloned.Nullable = "false"
	} else {
		cloned.Nullable = ""
	}
	return &cloned
}

func formatTableColSQLType(col cloudstorage.TableCol) string {
	tp := strings.ToUpper(strings.TrimSpace(col.Tp))
	baseType := strings.TrimSuffix(tp, " UNSIGNED")

	switch baseType {
	case "CHAR", "VARCHAR", "VAR_STRING", "BINARY", "VARBINARY", "BIT":
		if col.Precision != "" {
			return fmt.Sprintf("%s(%s)", tp, col.Precision)
		}
	case "ENUM", "SET":
		if len(col.Elems) > 0 {
			return fmt.Sprintf("%s(%s)", tp, quoteSQLStringList(col.Elems))
		}
	}
	return tp
}

func quoteSQLStringList(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		escaped := strings.ReplaceAll(value, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, "'", "''")
		quoted = append(quoted, fmt.Sprintf("'%s'", escaped))
	}
	return strings.Join(quoted, ",")
}

func parseIcebergDecimalType(raw string) (int, int, bool) {
	s := strings.TrimSpace(raw)
	if !strings.HasPrefix(s, "decimal(") || !strings.HasSuffix(s, ")") {
		return 0, 0, false
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(s, "decimal("), ")")
	parts := strings.Split(inner, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}

	precision, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
	scale, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err1 != nil || err2 != nil || precision <= 0 || scale < 0 || scale > precision {
		return 0, 0, false
	}
	return precision, scale, true
}

func appendIcebergChunkRow(
	chk *chunk.Chunk,
	tableInfo *commonType.TableInfo,
	values map[string]*string,
) error {
	for idx, colInfo := range tableInfo.GetColumns() {
		if err := appendIcebergChunkValue(chk, idx, colInfo, values[colInfo.Name.O]); err != nil {
			return err
		}
	}
	return nil
}

func appendIcebergChunkValue(
	chk *chunk.Chunk,
	idx int,
	colInfo *model.ColumnInfo,
	value *string,
) error {
	if value == nil {
		chk.AppendNull(idx)
		return nil
	}

	switch colInfo.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong:
		n, err := strconv.ParseInt(*value, 10, 64)
		if err != nil {
			return err
		}
		chk.AppendInt64(idx, n)
	case mysql.TypeFloat:
		n, err := strconv.ParseFloat(*value, 32)
		if err != nil {
			return err
		}
		chk.AppendFloat32(idx, float32(n))
	case mysql.TypeDouble:
		n, err := strconv.ParseFloat(*value, 64)
		if err != nil {
			return err
		}
		chk.AppendFloat64(idx, n)
	case mysql.TypeNewDecimal:
		chk.AppendMyDecimal(idx, tidbTypes.NewDecFromStringForTest(*value))
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t, err := tidbTypes.ParseTime(tidbTypes.DefaultStmtNoWarningContext, *value, colInfo.GetType(), tidbTypes.MaxFsp)
		if err != nil {
			return err
		}
		chk.AppendTime(idx, t)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if strings.EqualFold(colInfo.GetCharset(), charset.CharsetBin) {
			raw, err := base64.StdEncoding.DecodeString(*value)
			if err != nil {
				return err
			}
			chk.AppendBytes(idx, raw)
			return nil
		}
		chk.AppendString(idx, *value)
	case mysql.TypeString, mysql.TypeVarString, mysql.TypeVarchar:
		if strings.EqualFold(colInfo.GetCharset(), charset.CharsetBin) {
			raw, err := base64.StdEncoding.DecodeString(*value)
			if err != nil {
				return err
			}
			chk.AppendBytes(idx, raw)
			return nil
		}
		chk.AppendString(idx, *value)
	default:
		chk.AppendString(idx, *value)
	}
	return nil
}

func (c *consumer) handleIceberg(ctx context.Context, round uint64) error {
	if c.icebergCfg == nil {
		return fmt.Errorf("iceberg config is not initialized")
	}

	tables, err := sinkiceberg.ListHadoopTables(ctx, c.icebergCfg, c.externalStorage)
	if err != nil {
		return err
	}

	filteredTables := make([]sinkiceberg.TableIdentifier, 0, len(tables))
	for _, table := range tables {
		if isInternalIcebergTable(table.SchemaName, table.TableName) {
			continue
		}
		filteredTables = append(filteredTables, table)
	}
	sort.Slice(filteredTables, func(i, j int) bool {
		if filteredTables[i].SchemaName != filteredTables[j].SchemaName {
			return filteredTables[i].SchemaName < filteredTables[j].SchemaName
		}
		return filteredTables[i].TableName < filteredTables[j].TableName
	})

	log.Info("storage consumer iceberg scan done",
		zap.Uint64("round", round),
		zap.Int("tableCount", len(filteredTables)))

	for order, table := range filteredTables {
		stateKey := icebergStateKey(table.SchemaName, table.TableName)
		state := c.icebergStates[stateKey]
		if state == nil {
			state = &icebergTableState{}
			c.icebergStates[stateKey] = state
		}
		if table.LatestMetadataVersion <= state.lastMetadataVersion {
			continue
		}

		tableID := c.tableIDGenerator.generateFakeTableID(table.SchemaName, table.TableName, 0)
		log.Info("storage consumer handle iceberg table",
			zap.Uint64("round", round),
			zap.Int("order", order),
			zap.String("schema", table.SchemaName),
			zap.String("table", table.TableName),
			zap.Int("fromVersion", state.lastMetadataVersion+1),
			zap.Int("toVersion", table.LatestMetadataVersion))

		prevVersion := state.lastTableVersion
		for metadataVersion := state.lastMetadataVersion + 1; metadataVersion <= table.LatestMetadataVersion; metadataVersion++ {
			currVersion, err := sinkiceberg.LoadTableVersion(
				ctx, c.icebergCfg, c.externalStorage, table.SchemaName, table.TableName, metadataVersion,
			)
			if err != nil {
				return err
			}

			ddlEvents, err := buildIcebergDDLEvents(prevVersion, currVersion)
			if err != nil {
				return err
			}
			for _, ddlEvent := range ddlEvents {
				seq := c.readSeq.Inc()
				log.Info("storage consumer read iceberg ddl event",
					zap.Uint64("seq", seq),
					zap.Uint64("round", round),
					zap.Int("order", order),
					zap.String("schema", table.SchemaName),
					zap.String("table", table.TableName),
					zap.Int("metadataVersion", metadataVersion),
					zap.String("query", ddlEvent.Query))
				if err := c.sink.WriteBlockEvent(ddlEvent); err != nil {
					return err
				}
			}

			for _, dataFile := range currVersion.DataFiles {
				seq := c.readSeq.Inc()
				log.Info("storage consumer read iceberg dml file",
					zap.Uint64("seq", seq),
					zap.Uint64("round", round),
					zap.Int("order", order),
					zap.String("schema", table.SchemaName),
					zap.String("table", table.TableName),
					zap.Int("metadataVersion", metadataVersion),
					zap.String("path", dataFile))

				rows, err := sinkiceberg.DecodeParquetFile(ctx, c.externalStorage, dataFile)
				if err != nil {
					return err
				}
				for _, row := range rows {
					dmlEvent, err := buildIcebergDMLEvent(currVersion, tableID, row)
					if err != nil {
						return err
					}
					c.appendIcebergEvent(dmlEvent)
				}
			}

			if err := c.flushIcebergDMLEvents(ctx, tableID); err != nil {
				return err
			}
			state.lastMetadataVersion = metadataVersion
			state.lastTableVersion = currVersion
			prevVersion = currVersion
		}
	}

	return nil
}

func (c *consumer) appendIcebergEvent(dml *commonEvent.DMLEvent) {
	tableID := dml.GetTableID()
	group := c.eventsGroup[tableID]
	if group == nil {
		group = util.NewEventsGroup(0, tableID)
		c.eventsGroup[tableID] = group
	}
	group.Append(dml, true)
}

func icebergStateKey(schemaName, tableName string) string {
	return commonType.QuoteSchema(schemaName, tableName)
}

func isInternalIcebergTable(schemaName, _ string) bool {
	return schemaName == "__ticdc"
}
