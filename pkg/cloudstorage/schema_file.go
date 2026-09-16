// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package cloudstorage

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/hash"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"go.uber.org/zap"
)

const (
	defaultSchemaFileVersion = 1
	marshalPrefix            = ""
	marshalIndent            = "    "
)

// TableCol is one column entry persisted in a schema file.
type TableCol struct {
	ID        string   `json:"ColumnId,omitempty"`
	Name      string   `json:"ColumnName" `
	Tp        string   `json:"ColumnType"`
	Default   any      `json:"ColumnDefault,omitempty"`
	Precision string   `json:"ColumnPrecision,omitempty"`
	Scale     string   `json:"ColumnScale,omitempty"`
	Nullable  string   `json:"ColumnNullable,omitempty"`
	IsPK      string   `json:"ColumnIsPk,omitempty"`
	Elems     []string `json:"ColumnElems,omitempty"`
}

// fromTiColumnInfo fills TableCol from a TiDB column. outputColumnID controls
// whether ColumnId is written into the schema file payload.
func (t *TableCol) fromTiColumnInfo(col *model.ColumnInfo, outputColumnID bool) {
	defaultFlen, defaultDecimal := mysql.GetDefaultFieldLengthAndDecimal(col.GetType())
	isDecimalNotDefault := col.GetDecimal() != defaultDecimal &&
		col.GetDecimal() != 0 &&
		col.GetDecimal() != types.UnspecifiedLength

	displayFlen, displayDecimal := col.GetFlen(), col.GetDecimal()
	if displayFlen == types.UnspecifiedLength {
		displayFlen = defaultFlen
	}
	if displayDecimal == types.UnspecifiedLength {
		displayDecimal = defaultDecimal
	}

	if outputColumnID {
		t.ID = strconv.FormatInt(col.ID, 10)
	}
	t.Name = col.Name.O
	t.Tp = strings.ToUpper(types.TypeToStr(col.GetType(), col.GetCharset()))
	if mysql.HasUnsignedFlag(col.GetFlag()) {
		t.Tp += " UNSIGNED"
	}
	if mysql.HasPriKeyFlag(col.GetFlag()) {
		t.IsPK = "true"
	}
	if mysql.HasNotNullFlag(col.GetFlag()) {
		t.Nullable = "false"
	}
	t.Default = col.GetDefaultValue()

	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		if isDecimalNotDefault {
			t.Scale = strconv.Itoa(displayDecimal)
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		t.Precision = strconv.Itoa(displayFlen)
		if isDecimalNotDefault {
			t.Scale = strconv.Itoa(displayDecimal)
		}
	case mysql.TypeNewDecimal:
		t.Precision = strconv.Itoa(displayFlen)
		t.Scale = strconv.Itoa(displayDecimal)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		t.Precision = strconv.Itoa(displayFlen)
	case mysql.TypeYear:
		t.Precision = strconv.Itoa(displayFlen)
	case mysql.TypeEnum, mysql.TypeSet:
		t.Elems = col.GetElems()
	}
}

// toTiColumnInfo returns a TiDB column reconstructed from TableCol. colID is
// used as the returned column ID.
func (t *TableCol) toTiColumnInfo(colID int64) *model.ColumnInfo {
	col := new(model.ColumnInfo)
	col.ID = colID
	col.Name = ast.NewCIStr(t.Name)
	tp := types.StrToType(strings.ToLower(strings.TrimSuffix(t.Tp, " UNSIGNED")))
	col.FieldType = *types.NewFieldType(tp)
	if strings.Contains(t.Tp, "UNSIGNED") {
		col.AddFlag(mysql.UnsignedFlag)
	}
	if t.IsPK == "true" {
		col.AddFlag(mysql.PriKeyFlag)
	}
	if t.Nullable == "false" {
		col.AddFlag(mysql.NotNullFlag)
	}
	col.DefaultValue = t.Default
	col.SetCharset(charset.CharsetUTF8MB4)
	if strings.Contains(t.Tp, "BLOB") || strings.Contains(t.Tp, "BINARY") {
		col.SetCharset(charset.CharsetBin)
	}
	setFlen := func(precision string) {
		if len(precision) > 0 {
			flen, err := strconv.Atoi(precision)
			if err != nil {
				log.Panic("parse precision failed, this should not happen",
					zap.String("precision", precision), zap.Error(err))
			}
			col.SetFlen(flen)
		}
	}
	setDecimal := func(scale string) {
		if len(scale) > 0 {
			decimal, err := strconv.Atoi(scale)
			if err != nil {
				log.Panic("parse scale failed, this should not happen",
					zap.String("scale", scale), zap.Error(err))
			}
			col.SetDecimal(decimal)
		}
	}
	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		setDecimal(t.Scale)
	case mysql.TypeDouble, mysql.TypeFloat, mysql.TypeNewDecimal:
		setFlen(t.Precision)
		setDecimal(t.Scale)
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeYear:
		setFlen(t.Precision)
	case mysql.TypeEnum, mysql.TypeSet:
		col.SetElems(t.Elems)
	}

	return col
}

// SchemaFile is the payload persisted as schema_*.json.
// It carries table structure in Columns/TotalColumns and DDL replay metadata in
// Query/Type/TableVersion.
type SchemaFile struct {
	Table   string `json:"Table"`
	Schema  string `json:"Schema"`
	Version uint64 `json:"Version"`
	// TableVersion is the schema version encoded into schema file name:
	// schema_{TableVersion}_{checksum}.json.
	// It is passed from tableInfoVersion in path generation.
	TableVersion uint64     `json:"TableVersion"`
	Query        string     `json:"Query"`
	Type         byte       `json:"Type"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

// checksumPayload ignores DDL replay fields and path metadata.
type checksumPayload struct {
	Table        string     `json:"Table"`
	Schema       string     `json:"Schema"`
	Version      uint64     `json:"Version"`
	Columns      []TableCol `json:"TableColumns"`
	TotalColumns int        `json:"TableColumnsTotal"`
}

// DDLEvent returns the DDL event represented by SchemaFile.
// The returned event includes a TableInfo rebuilt from Columns plus the Query,
// Type, schema/table name, and FinishedTs from the schema file.
func (t *SchemaFile) DDLEvent() *commonEvent.DDLEvent {
	return &commonEvent.DDLEvent{
		TableInfo:     t.TableInfo(),
		FinishedTs:    t.TableVersion,
		Type:          t.Type,
		Query:         t.Query,
		SchemaName:    t.Schema,
		TableName:     t.Table,
		BlockedTables: &commonEvent.InfluencedTables{InfluenceType: commonEvent.InfluenceTypeAll},
	}
}

// Build fills SchemaFile from a DDL event for persistence.
// outputColumnID controls whether column IDs are written. If event.TableInfo is
// nil, only schema/table name and DDL metadata are filled.
func (t *SchemaFile) Build(event *commonEvent.DDLEvent, outputColumnID bool) {
	t.Version = defaultSchemaFileVersion
	t.TableVersion = event.FinishedTs
	t.Query = event.Query
	t.Type = event.Type

	info := event.TableInfo
	if info == nil {
		t.Schema = event.GetTargetSchemaName()
		t.Table = event.GetTargetTableName()
		return
	}
	t.Schema = info.GetTargetSchemaName()
	t.Table = info.GetTargetTableName()
	t.TotalColumns = len(info.GetColumns())
	for _, col := range info.GetColumns() {
		var tableCol TableCol
		tableCol.fromTiColumnInfo(col, outputColumnID)
		t.Columns = append(t.Columns, tableCol)
	}
}

// TableInfo returns decoder TableInfo rebuilt from SchemaFile columns.
// It uses deterministic mock column IDs starting from 100.
func (t *SchemaFile) TableInfo() *common.TableInfo {
	tidbTableInfo := &model.TableInfo{
		Name: ast.NewCIStr(t.Table),
	}
	nextMockID := int64(100) // 100 is an arbitrary number
	for _, col := range t.Columns {
		tiCol := col.toTiColumnInfo(nextMockID)
		if mysql.HasPriKeyFlag(tiCol.GetFlag()) {
			// use PKIsHandle to make sure that the primary keys can be detected
			tidbTableInfo.PKIsHandle = true
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
		nextMockID++
	}
	return common.NewTableInfo4Decoder(t.Schema, tidbTableInfo)
}

// Marshal returns the indented JSON payload written to schema files.
func (t *SchemaFile) Marshal() []byte {
	data, err := json.MarshalIndent(t, marshalPrefix, marshalIndent)
	if err != nil {
		log.Panic("marshal the schema file failed, this should not happen",
			zap.Any("schemaFile", t), zap.Error(err))
	}
	return data
}

// marshalForChecksum marshals fields covered by the path checksum.
func (t *SchemaFile) marshalForChecksum() []byte {
	// sort columns by name
	sortedColumns := make([]TableCol, len(t.Columns))
	copy(sortedColumns, t.Columns)
	sort.Slice(sortedColumns, func(i, j int) bool {
		return sortedColumns[i].Name < sortedColumns[j].Name
	})

	payload := checksumPayload{
		Table:        t.Table,
		Schema:       t.Schema,
		Columns:      sortedColumns,
		TotalColumns: t.TotalColumns,
	}

	data, err := json.MarshalIndent(payload, marshalPrefix, marshalIndent)
	if err != nil {
		log.Panic("marshal for the schema file checksum failed, this should not happen",
			zap.Any("payload", payload), zap.Error(err))
	}
	return data
}

// Checksum returns the checksum used in schema file names.
func (t *SchemaFile) Checksum() uint32 {
	hasher := hash.NewPositionInertia()
	hasher.Reset()
	data := t.marshalForChecksum()

	hasher.Write(data)
	return hasher.Sum32()
}

// Path returns the schema file path for this payload.
// Database-level schema files use <schema>/meta/...
// Table-level schema files use <schema>/<table>/meta/... or <tableID>/meta/...
// when useTableIDAsPath is true. The file name includes the generated checksum.
func (t *SchemaFile) Path(useTableIDAsPath bool, tableID int64) string {
	tableLevel := t.TotalColumns != 0
	table := t.Table
	if tableLevel {
		table = generateTablePath(t.Table, tableID, useTableIDAsPath)
	}
	omitSchema := useTableIDAsPath && tableLevel
	return generateSchemaFilePath(t.Schema, table, t.TableVersion, t.Checksum(), omitSchema)
}
