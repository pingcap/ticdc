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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lake

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

func schemaFromNative(info *common.TableInfo, database, conflictKey string) (tableSchema, error) {
	if info == nil {
		return tableSchema{}, errors.ErrLakeInvalidData.GenWithStackByArgs("missing authoritative table metadata")
	}
	if info.IsPartitionTable() {
		return tableSchema{}, errors.ErrLakeInvalidData.GenWithStackByArgs("partition dispatchers are outside the single-table writer proof; partition DDL is unsupported")
	}
	s := tableSchema{database: info.GetSchemaName(), table: info.GetTableName(), primaryKey: info.GetPrimaryKeyColumnNames(), source: info.GetSchemaName() + "." + info.GetTableName()}
	if database != "" {
		s.database = database
	}
	if strings.HasSuffix(s.table, "_cdc_raw") || strings.HasSuffix(s.table, "_cdc_schema_epoch") {
		return tableSchema{}, errors.ErrLakeInvalidData.GenWithStackByArgs("source table uses a reserved CDC suffix")
	}
	for _, c := range info.GetColumns() {
		if !common.IsColCDCVisible(c) {
			continue
		}
		typ, enc, err := nativeColumnType(c)
		if err != nil {
			return tableSchema{}, err
		}
		def, expr, err := nativeDefault(c, enc)
		if err != nil {
			return tableSchema{}, err
		}
		s.columns = append(s.columns, column{name: c.Name.O, encoding: enc, sourceID: c.ID, typ: typ, nullable: !mysql.HasNotNullFlag(c.GetFlag()) || c.IsGenerated(), defaultSQL: def, defaultExpr: expr})
	}
	if conflictKey != "" {
		// Match bend-cdc's persisted single-column override. Never silently
		// replace a composite key with only one of its members.
		if len(s.primaryKey) > 1 {
			return tableSchema{}, errors.ErrLakeInvalidData.GenWithStackByArgs("conflict-key cannot override a composite source key")
		}
		actual := ""
		for _, col := range s.columns {
			if strings.EqualFold(col.name, conflictKey) {
				actual = col.name
				break
			}
		}
		if actual == "" {
			return tableSchema{}, errors.ErrLakeInvalidData.GenWithStackByArgs("conflict-key must name a visible source column")
		}
		s.primaryKey = []string{actual}
	}
	return s, s.validate()
}

func nativeColumnType(c *model.ColumnInfo) (string, columnEncoding, error) {
	signed := func(a, b string) string {
		if mysql.HasUnsignedFlag(c.GetFlag()) {
			return b
		}
		return a
	}
	switch c.GetType() {
	case mysql.TypeTiny:
		return signed("INT8", "UINT8"), scalarColumn, nil
	case mysql.TypeShort:
		return signed("INT16", "UINT16"), scalarColumn, nil
	case mysql.TypeInt24, mysql.TypeLong:
		return signed("INT32", "UINT32"), scalarColumn, nil
	case mysql.TypeLonglong:
		return signed("INT64", "UINT64"), scalarColumn, nil
	case mysql.TypeYear:
		return "INT16", scalarColumn, nil
	case mysql.TypeFloat:
		return "FLOAT32", scalarColumn, nil
	case mysql.TypeDouble:
		return "FLOAT64", scalarColumn, nil
	case mysql.TypeNewDecimal:
		p, scale := c.GetFlen(), c.GetDecimal()
		if p <= 0 || p > 76 || scale < 0 || scale > p {
			return "", 0, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid decimal metadata")
		}
		return fmt.Sprintf("DECIMAL(%d,%d)", p, scale), scalarColumn, nil
	case mysql.TypeDate, mysql.TypeNewDate:
		return "DATE", scalarColumn, nil
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		return "TIMESTAMP", scalarColumn, nil
	case mysql.TypeDuration, mysql.TypeEnum, mysql.TypeSet:
		return "VARCHAR", scalarColumn, nil
	case mysql.TypeBit:
		return "UINT64", scalarColumn, nil
	case mysql.TypeJSON:
		return "VARIANT", jsonColumn, nil
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		if c.GetCharset() == "binary" {
			return "BINARY", binaryColumn, nil
		}
		return "VARCHAR", scalarColumn, nil
	default:
		return "", 0, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported native column type")
	}
}

func nativeValues(info *common.TableInfo, row chunk.Row, location *time.Location) (map[string]any, error) {
	if location == nil {
		location = time.UTC
	}
	values := make(map[string]any, len(info.GetColumns()))
	for _, col := range info.GetColumns() {
		if !common.IsColCDCVisible(col) {
			continue
		}
		offset, ok := info.GetRowColumnsOffset()[col.ID]
		if !ok || offset >= row.Len() {
			return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("row does not match authoritative schema")
		}
		d := row.GetDatum(offset, &col.FieldType)
		var v any
		if !d.IsNull() {
			switch col.GetType() {
			case mysql.TypeEnum:
				v = d.GetMysqlEnum().Name
			case mysql.TypeSet:
				v = d.GetMysqlSet().Name
			case mysql.TypeBit:
				n, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
				if err != nil {
					return nil, errors.WrapError(errors.ErrLakeInvalidData, err, "invalid bit datum")
				}
				v = strconv.FormatUint(n, 10)
			case mysql.TypeJSON:
				v = d.GetMysqlJSON().String()
			case mysql.TypeNewDecimal:
				v = d.GetMysqlDecimal().String()
			case mysql.TypeDate, mysql.TypeNewDate, mysql.TypeDatetime:
				v = d.GetMysqlTime().String()
			case mysql.TypeTimestamp:
				stamp := d.GetMysqlTime()
				if !stamp.IsZero() {
					if err := stamp.ConvertTimeZone(location, time.UTC); err != nil {
						return nil, errors.WrapError(errors.ErrLakeInvalidData, err, "cannot normalize timestamp timezone")
					}
				}
				v = stamp.String()
			case mysql.TypeDuration:
				v = d.GetMysqlDuration().String()
			case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString, mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
				v = d.GetBytes()
			default:
				text, err := d.ToString()
				if err != nil {
					return nil, errors.WrapError(errors.ErrLakeInvalidData, err, "cannot format native datum")
				}
				v = text
			}
		}
		values[col.Name.O] = v
	}
	return values, nil
}

func normalizeChange(info *common.TableInfo, schema tableSchema, change commonEvent.RowChange, ts uint64, location *time.Location) (normalizedRow, error) {
	action := rowAction(change.RowType.String())
	image := change.Row
	if change.RowType == common.RowTypeDelete {
		image = change.PreRow
	}
	values, err := nativeValues(info, image, location)
	if err != nil {
		return normalizedRow{}, err
	}
	row, err := normalizeNativeRow(schema, action, ts, values)
	if err != nil {
		return normalizedRow{}, err
	}
	if change.RowType == common.RowTypeUpdate {
		before, err := nativeValues(info, change.PreRow, location)
		if err != nil {
			return normalizedRow{}, err
		}
		old, err := normalizeNativeRow(schema, deleteAction, ts, before)
		if err != nil {
			return normalizedRow{}, err
		}
		if old.id != row.id {
			return normalizedRow{}, errors.ErrLakeInvalidData.GenWithStackByArgs("conflict key changed in an unsplit UPDATE; use a stable conflict-key or split change mode")
		}
	}
	return row, nil
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

func nativeDefault(c *model.ColumnInfo, encoding columnEncoding) (string, bool, error) {
	value := c.GetDefaultValue()
	if c.IsGenerated() || value == nil {
		return "", c.DefaultIsExpr, nil
	}
	var text string
	switch v := value.(type) {
	case string:
		text = v
	case []byte:
		text = string(v)
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		text = fmt.Sprint(v)
	default:
		return "", false, errors.ErrLakeInvalidData.GenWithStackByArgs("unsupported column default representation")
	}
	expr := c.DefaultIsExpr
	if (c.GetType() == mysql.TypeTimestamp || c.GetType() == mysql.TypeDatetime) && strings.HasPrefix(strings.ToUpper(text), "CURRENT_TIMESTAMP") {
		expr = true
	}
	if expr {
		return "", true, nil
	} // Initial DML carries every value explicitly.
	if c.GetType() == mysql.TypeBit {
		n, err := types.BinaryLiteral([]byte(text)).ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return "", false, errors.WrapError(errors.ErrLakeInvalidData, err, "invalid bit default")
		}
		return strconv.FormatUint(n, 10), false, nil
	}
	if encoding == binaryColumn {
		return "unhex(" + sqlString(hex.EncodeToString([]byte(text))) + ")", false, nil
	}
	if encoding == jsonColumn {
		if !json.Valid([]byte(text)) {
			return "", false, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid JSON default")
		}
		return "parse_json(" + sqlString(text) + ")", false, nil
	}
	return sqlString(text), false, nil
}
