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
	"github.com/pingcap/ticdc/pkg/errors"
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

// TableCol denotes column info stored in a schema file.
type TableCol struct {
	ID        string      `json:"ColumnId,omitempty"`
	Name      string      `json:"ColumnName" `
	Tp        string      `json:"ColumnType"`
	Default   interface{} `json:"ColumnDefault,omitempty"`
	Precision string      `json:"ColumnPrecision,omitempty"`
	Scale     string      `json:"ColumnScale,omitempty"`
	Nullable  string      `json:"ColumnNullable,omitempty"`
	IsPK      string      `json:"ColumnIsPk,omitempty"`
	Elems     []string    `json:"ColumnElems,omitempty"`
}

// FromTiColumnInfo converts from TiDB ColumnInfo to TableCol.
func (t *TableCol) FromTiColumnInfo(col *model.ColumnInfo, outputColumnID bool) {
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

// ToTiColumnInfo converts from TableCol to TiDB ColumnInfo.
func (t *TableCol) ToTiColumnInfo(colID int64) (*model.ColumnInfo, error) {
	col := new(model.ColumnInfo)

	if t.ID != "" {
		var err error
		col.ID, err = strconv.ParseInt(t.ID, 10, 64)
		if err != nil {
			return nil, errors.WrapError(errors.ErrInternalCheckFailed, err)
		}
	}

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
	if strings.Contains(t.Tp, "BLOB") || strings.Contains(t.Tp, "BINARY") {
		col.SetCharset(charset.CharsetBin)
	} else {
		col.SetCharset(charset.CharsetUTF8MB4)
	}
	setFlen := func(precision string) error {
		if len(precision) > 0 {
			flen, err := strconv.Atoi(precision)
			if err != nil {
				return errors.WrapError(errors.ErrInternalCheckFailed, err)
			}
			col.SetFlen(flen)
		}
		return nil
	}
	setDecimal := func(scale string) error {
		if len(scale) > 0 {
			decimal, err := strconv.Atoi(scale)
			if err != nil {
				return errors.WrapError(errors.ErrInternalCheckFailed, err)
			}
			col.SetDecimal(decimal)
		}
		return nil
	}
	switch col.GetType() {
	case mysql.TypeTimestamp, mysql.TypeDatetime, mysql.TypeDuration:
		err := setDecimal(t.Scale)
		if err != nil {
			return nil, err
		}
	case mysql.TypeDouble, mysql.TypeFloat, mysql.TypeNewDecimal:
		err := setFlen(t.Precision)
		if err != nil {
			return nil, err
		}
		err = setDecimal(t.Scale)
		if err != nil {
			return nil, err
		}
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeBit, mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
		mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeYear:
		err := setFlen(t.Precision)
		if err != nil {
			return nil, err
		}
	case mysql.TypeEnum, mysql.TypeSet:
		col.SetElems(t.Elems)
	}

	return col, nil
}

// SchemaFile is the payload persisted as schema_*.json.
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

// ToDDLEvent converts SchemaFile to DDLEvent.
func (t *SchemaFile) ToDDLEvent() (*commonEvent.DDLEvent, error) {
	tableInfo, err := t.ToTableInfo()
	if err != nil {
		return nil, err
	}
	return &commonEvent.DDLEvent{
		TableInfo:     tableInfo,
		FinishedTs:    t.TableVersion,
		Type:          t.Type,
		Query:         t.Query,
		SchemaName:    t.Schema,
		TableName:     t.Table,
		BlockedTables: &commonEvent.InfluencedTables{InfluenceType: commonEvent.InfluenceTypeAll},
	}, nil
}

// Build fills SchemaFile from DDLEvent.
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
		tableCol.FromTiColumnInfo(col, outputColumnID)
		t.Columns = append(t.Columns, tableCol)
	}
}

// ToTableInfo converts SchemaFile to TableInfo.
func (t *SchemaFile) ToTableInfo() (*common.TableInfo, error) {
	tidbTableInfo := &model.TableInfo{
		Name: ast.NewCIStr(t.Table),
	}
	nextMockID := int64(100) // 100 is an arbitrary number
	for _, col := range t.Columns {
		tiCol, err := col.ToTiColumnInfo(nextMockID)
		if err != nil {
			return nil, err
		}
		if mysql.HasPriKeyFlag(tiCol.GetFlag()) {
			// use PKIsHandle to make sure that the primary keys can be detected
			tidbTableInfo.PKIsHandle = true
		}
		tidbTableInfo.Columns = append(tidbTableInfo.Columns, tiCol)
		nextMockID++
	}
	info := common.NewTableInfo4Decoder(t.Schema, tidbTableInfo)
	return info, nil
}

// isTableLevel returns whether this file describes a table.
func (t *SchemaFile) isTableLevel() bool {
	if len(t.Columns) != t.TotalColumns {
		log.Panic("invalid schema file", zap.Any("schemaFile", t))
	}
	return t.TotalColumns != 0
}

// Marshal marshals SchemaFile.
func (t *SchemaFile) Marshal() ([]byte, error) {
	data, err := json.MarshalIndent(t, marshalPrefix, marshalIndent)
	if err != nil {
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}
	return data, nil
}

// marshalForChecksum marshals fields covered by the path checksum.
func (t *SchemaFile) marshalForChecksum() ([]byte, error) {
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
		return nil, errors.WrapError(errors.ErrMarshalFailed, err)
	}
	return data, nil
}

// Sum32 returns the 32-bits hash value of SchemaFile.
func (t *SchemaFile) Sum32(hasher *hash.PositionInertia) (uint32, error) {
	if hasher == nil {
		hasher = hash.NewPositionInertia()
	}
	hasher.Reset()
	data, err := t.marshalForChecksum()
	if err != nil {
		return 0, err
	}

	hasher.Write(data)
	return hasher.Sum32(), nil
}

// GenerateSchemaFilePath generates the schema file path for SchemaFile
// with optional table id path.
func (t *SchemaFile) GenerateSchemaFilePath(useTableIDAsPath bool, tableID int64) (string, error) {
	checksum, err := t.Sum32(nil)
	if err != nil {
		return "", err
	}
	if t.Schema == "" {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs("schema cannot be empty")
	}
	if t.TableVersion == 0 {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs("table version cannot be zero")
	}
	if len(t.Columns) != t.TotalColumns {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs("invalid schema file")
	}
	isTableLevel := t.TotalColumns != 0
	if !isTableLevel && t.Table != "" {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs("invalid schema file")
	}
	if useTableIDAsPath && isTableLevel && tableID <= 0 {
		return "", errors.ErrInternalCheckFailed.GenWithStackByArgs("invalid table id for table-id path")
	}

	table := t.Table
	if isTableLevel {
		tablePath, err := generateTablePath(t.Table, tableID, useTableIDAsPath)
		if err != nil {
			return "", err
		}
		table = tablePath
	}
	omitSchema := useTableIDAsPath && isTableLevel
	path, err := generateSchemaFilePath(t.Schema, table, t.TableVersion, checksum, omitSchema)
	if err != nil {
		return "", err
	}
	return path, nil
}
