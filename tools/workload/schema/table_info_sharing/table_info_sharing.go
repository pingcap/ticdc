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

package tableinfosharing

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"workload/schema"
)

const (
	defaultRecentRowWindow = 256
	tableInfoVariantCount  = 7
)

type tableVariant int

const (
	tableVariantBase tableVariant = iota
	tableVariantDifferentDefaults
	tableVariantTypeAttrs
	tableVariantNullableAttrs
	tableVariantIndexLayout
	tableVariantGeneratedColumn
	tableVariantExtraColumn
)

type tableSpec struct {
	tableIndex int
	variant    tableVariant
}

type TableInfoSharingWorkload struct {
	tableStartIndex int
	seq             []atomic.Uint64
}

func NewTableInfoSharingWorkload(tableCount int, tableStartIndex int) schema.Workload {
	if tableCount <= 0 {
		tableCount = 1
	}
	return &TableInfoSharingWorkload{
		tableStartIndex: tableStartIndex,
		seq:             make([]atomic.Uint64, tableCount),
	}
}

func (w *TableInfoSharingWorkload) BuildCreateTableStatement(n int) string {
	spec := newTableSpec(n)
	definitions := append(spec.columnDefinitions(), spec.indexDefinitions()...)
	return fmt.Sprintf(
		"\nCREATE TABLE IF NOT EXISTS %s (\n%s\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin\n",
		tableName(n),
		strings.Join(definitions, ",\n"),
	)
}

func (w *TableInfoSharingWorkload) BuildInsertSql(tableN int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}

	startID, ok := w.allocateIDs(tableN, batchSize)
	if !ok {
		return ""
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(
		"INSERT INTO %s (id, uk_token, c_json, c_text, c_blob, c_binary, c_varbinary) VALUES ",
		tableName(tableN),
	))

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			buf.WriteString(",")
		}

		rowID := startID + uint64(i)
		jsonValue := fmt.Sprintf(`{"table":"%s","mode":"insert","id":%d}`, tableName(tableN), rowID)
		textValue := fmt.Sprintf("text-%02d-%06d", tableN, rowID)
		blobValue := []byte(fmt.Sprintf("blob-%02d-%06d", tableN, rowID))
		binaryValue := []byte(fmt.Sprintf("%08d", rowID%100000000))
		varbinaryValue := []byte(fmt.Sprintf("vb-%02d-%06d", tableN, rowID))

		buf.WriteString(fmt.Sprintf(
			"(%d,'%s',CAST('%s' AS JSON),'%s',x'%s',x'%s',x'%s')",
			rowID,
			quoteString(uniqueToken(tableN, rowID)),
			quoteString(jsonValue),
			quoteString(textValue),
			hex.EncodeToString(blobValue),
			hex.EncodeToString(binaryValue),
			hex.EncodeToString(varbinaryValue),
		))
	}
	return buf.String()
}

func (w *TableInfoSharingWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	if opts.Batch <= 0 {
		return ""
	}

	current, ok := w.currentMaxID(opts.TableIndex)
	if !ok || current == 0 {
		return ""
	}

	spec := newTableSpec(opts.TableIndex)
	startID, endID := recentRowRange(current, opts.Batch)
	marker := time.Now().UnixNano()

	return fmt.Sprintf(
		`UPDATE %s
SET c_int = c_int + 17,
    c_decimal = c_decimal + 1.250000,
    c_bool = 1 - c_bool,
    c_bit = b'%s',
    c_varchar = '%s',
    c_date = '%s',
    c_datetime = '%s',
    c_timestamp = '%s',
    c_time = '%s',
    c_year = %d,
    c_enum = '%s',
    c_set = '%s',
    c_json = CAST('%s' AS JSON),
    c_text = '%s',
    c_blob = x'%s',
    c_binary = x'%s',
    c_varbinary = x'%s'
WHERE id BETWEEN %d AND %d`,
		tableName(opts.TableIndex),
		spec.updateBitValue(marker),
		quoteString(fmt.Sprintf("updated-%02d-%d", opts.TableIndex, marker)),
		spec.updateDateValue(),
		spec.updateDatetimeValue(),
		spec.updateTimestampValue(),
		spec.updateTimeValue(),
		spec.updateYearValue(),
		spec.updateEnumValue(),
		spec.updateSetValue(),
		quoteString(fmt.Sprintf(`{"table":"%s","mode":"update","marker":%d}`, tableName(opts.TableIndex), marker)),
		quoteString(fmt.Sprintf("updated-text-%02d-%d", opts.TableIndex, marker)),
		hex.EncodeToString([]byte(fmt.Sprintf("updated-blob-%02d-%d", opts.TableIndex, marker))),
		hex.EncodeToString([]byte(fmt.Sprintf("%08d", marker%100000000))),
		hex.EncodeToString([]byte(fmt.Sprintf("updated-vb-%02d", opts.TableIndex))),
		startID,
		endID,
	)
}

func (w *TableInfoSharingWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	if opts.Batch <= 0 {
		return ""
	}

	current, ok := w.currentMaxID(opts.TableIndex)
	if !ok || current == 0 {
		return ""
	}

	startID, endID := recentRowRange(current, opts.Batch)
	return fmt.Sprintf(
		"DELETE FROM %s WHERE id BETWEEN %d AND %d LIMIT %d",
		tableName(opts.TableIndex),
		startID,
		endID,
		opts.Batch,
	)
}

func (w *TableInfoSharingWorkload) allocateIDs(tableN int, batchSize int) (uint64, bool) {
	slot, ok := w.slot(tableN)
	if !ok {
		return 0, false
	}
	next := w.seq[slot].Add(uint64(batchSize))
	return next - uint64(batchSize) + 1, true
}

func (w *TableInfoSharingWorkload) currentMaxID(tableN int) (uint64, bool) {
	slot, ok := w.slot(tableN)
	if !ok {
		return 0, false
	}
	return w.seq[slot].Load(), true
}

func (w *TableInfoSharingWorkload) slot(tableN int) (int, bool) {
	slot := tableN - w.tableStartIndex
	if slot < 0 || slot >= len(w.seq) {
		return 0, false
	}
	return slot, true
}

func newTableSpec(tableIndex int) tableSpec {
	return tableSpec{
		tableIndex: tableIndex,
		variant:    tableVariant(absInt(tableIndex) % tableInfoVariantCount),
	}
}

func (s tableSpec) columnDefinitions() []string {
	decimalType := "DECIMAL(20,6)"
	charType := "CHAR(8)"
	varcharType := "VARCHAR(64)"
	enumType := "ENUM('red','green','blue')"
	setType := "SET('x','y','z')"
	textType := "TEXT"
	blobType := "BLOB"
	binaryType := "BINARY(8)"
	varbinaryType := "VARBINARY(16)"

	if s.variant == tableVariantTypeAttrs {
		decimalType = "DECIMAL(30,10)"
		charType = "CHAR(12)"
		varcharType = "VARCHAR(96)"
		enumType = "ENUM('red','green','blue','yellow')"
		setType = "SET('x','y','z','w')"
		textType = "MEDIUMTEXT"
		blobType = "MEDIUMBLOB"
		binaryType = "BINARY(12)"
		varbinaryType = "VARBINARY(24)"
	}

	intNullability := "NOT NULL"
	stringNullability := "NOT NULL"
	if s.variant == tableVariantNullableAttrs {
		intNullability = "NULL"
		stringNullability = "NULL"
	}

	columns := []string{
		"  id BIGINT NOT NULL",
		fmt.Sprintf("  c_tinyint TINYINT %s DEFAULT %d", intNullability, s.tinyintDefault()),
		fmt.Sprintf("  c_smallint SMALLINT NOT NULL DEFAULT %d", s.smallintDefault()),
		fmt.Sprintf("  c_mediumint MEDIUMINT NOT NULL DEFAULT %d", s.mediumintDefault()),
		fmt.Sprintf("  c_int INT NOT NULL DEFAULT %d", s.intDefault()),
		fmt.Sprintf("  c_bigint BIGINT NOT NULL DEFAULT %d", s.bigintDefault()),
		fmt.Sprintf("  c_unsigned BIGINT UNSIGNED NOT NULL DEFAULT %d", s.unsignedDefault()),
		fmt.Sprintf("  c_decimal %s NOT NULL DEFAULT %s", decimalType, s.decimalDefault()),
		fmt.Sprintf("  c_float FLOAT NOT NULL DEFAULT %s", s.floatDefault()),
		fmt.Sprintf("  c_double DOUBLE NOT NULL DEFAULT %s", s.doubleDefault()),
		fmt.Sprintf("  c_bool BOOLEAN %s DEFAULT %d", intNullability, s.boolDefault()),
		fmt.Sprintf("  c_bit BIT(8) NOT NULL DEFAULT b'%s'", s.bitDefault()),
		fmt.Sprintf("  c_char %s %s DEFAULT '%s'", charType, stringNullability, s.charDefault()),
		fmt.Sprintf("  c_varchar %s NOT NULL DEFAULT '%s'", varcharType, s.varcharDefault()),
		fmt.Sprintf("  c_date DATE NOT NULL DEFAULT '%s'", s.dateDefault()),
		fmt.Sprintf("  c_datetime DATETIME NOT NULL DEFAULT '%s'", s.datetimeDefault()),
		fmt.Sprintf("  c_timestamp TIMESTAMP NOT NULL DEFAULT '%s'", s.timestampDefault()),
		fmt.Sprintf("  c_time TIME NOT NULL DEFAULT '%s'", s.timeDefault()),
		fmt.Sprintf("  c_year YEAR NOT NULL DEFAULT %d", s.yearDefault()),
		fmt.Sprintf("  c_enum %s NOT NULL DEFAULT '%s'", enumType, s.enumDefault()),
		fmt.Sprintf("  c_set %s NOT NULL DEFAULT '%s'", setType, s.setDefault()),
		"  c_json JSON NULL",
		fmt.Sprintf("  c_text %s NULL", textType),
		fmt.Sprintf("  c_blob %s NULL", blobType),
		fmt.Sprintf("  c_binary %s NULL", binaryType),
		fmt.Sprintf("  c_varbinary %s NULL", varbinaryType),
	}

	if s.variant == tableVariantGeneratedColumn {
		columns = append(columns, "  c_generated_sum BIGINT GENERATED ALWAYS AS (c_int + c_tinyint) STORED")
	}
	if s.variant == tableVariantExtraColumn {
		columns = append(
			columns,
			fmt.Sprintf("  c_extra_tag VARCHAR(32) NOT NULL DEFAULT 'extra-%02d'", s.defaultSeed()),
			fmt.Sprintf("  c_extra_score INT NOT NULL DEFAULT %d", s.defaultSeed()*100+7),
		)
	}

	return append(columns, "  uk_token VARCHAR(64) NOT NULL")
}

func (s tableSpec) indexDefinitions() []string {
	if s.variant == tableVariantIndexLayout {
		return []string{
			"  PRIMARY KEY (id)",
			"  UNIQUE KEY uk_token (uk_token)",
			"  KEY idx_temporal_reverse (c_datetime, c_date)",
			"  KEY idx_varchar_prefix (c_varchar(16))",
			"  KEY idx_enum_set_reverse (c_set, c_enum)",
		}
	}

	return []string{
		"  PRIMARY KEY (id)",
		"  UNIQUE KEY uk_token (uk_token)",
		"  KEY idx_temporal (c_date, c_datetime)",
		"  KEY idx_enum_set (c_enum, c_set)",
	}
}

func (s tableSpec) defaultSeed() int {
	return int(s.variant)
}

func (s tableSpec) tinyintDefault() int {
	return (s.defaultSeed() % 63) + 1
}

func (s tableSpec) smallintDefault() int {
	return s.defaultSeed()*10 + 1
}

func (s tableSpec) mediumintDefault() int {
	return s.defaultSeed()*100 + 1
}

func (s tableSpec) intDefault() int {
	return s.defaultSeed()*1000 + 1
}

func (s tableSpec) bigintDefault() int64 {
	return int64(s.defaultSeed())*10000 + 1
}

func (s tableSpec) unsignedDefault() uint64 {
	return uint64(s.defaultSeed())*100000 + 1
}

func (s tableSpec) decimalDefault() string {
	return fmt.Sprintf("%d.%06d", s.defaultSeed(), (s.defaultSeed()*111111)%1000000)
}

func (s tableSpec) floatDefault() string {
	return fmt.Sprintf("%d.25", s.defaultSeed())
}

func (s tableSpec) doubleDefault() string {
	return fmt.Sprintf("%d.125", s.defaultSeed())
}

func (s tableSpec) boolDefault() int {
	return s.defaultSeed() % 2
}

func (s tableSpec) bitDefault() string {
	return rotateBits(s.defaultSeed())
}

func (s tableSpec) charDefault() string {
	return fmt.Sprintf("c%07d", s.defaultSeed()%10000000)
}

func (s tableSpec) varcharDefault() string {
	return fmt.Sprintf("varchar-%02d", s.defaultSeed())
}

func (s tableSpec) dateDefault() string {
	month := monthValue(s.defaultSeed())
	day := dayValue(s.defaultSeed())
	return fmt.Sprintf("2026-%02d-%02d", month, day)
}

func (s tableSpec) datetimeDefault() string {
	month := monthValue(s.defaultSeed())
	day := dayValue(s.defaultSeed())
	hour, minute, second := clockValue(s.defaultSeed())
	return fmt.Sprintf("2026-%02d-%02d %02d:%02d:%02d", month, day, hour, minute, second)
}

func (s tableSpec) timestampDefault() string {
	return s.datetimeDefault()
}

func (s tableSpec) timeDefault() string {
	hour, minute, second := clockValue(s.defaultSeed())
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func (s tableSpec) yearDefault() int {
	return 2020 + (s.defaultSeed() % 10)
}

func (s tableSpec) enumDefault() string {
	values := []string{"red", "green", "blue"}
	return values[s.defaultSeed()%len(values)]
}

func (s tableSpec) setDefault() string {
	values := []string{"x", "x,y", "y,z", "x,z"}
	return values[s.defaultSeed()%len(values)]
}

func (s tableSpec) updateDateValue() string {
	month := monthValue(s.defaultSeed() + 5)
	day := dayValue(s.defaultSeed() + 7)
	return fmt.Sprintf("2027-%02d-%02d", month, day)
}

func (s tableSpec) updateDatetimeValue() string {
	month := monthValue(s.defaultSeed() + 5)
	day := dayValue(s.defaultSeed() + 7)
	hour, minute, second := clockValue(s.defaultSeed() + 9)
	return fmt.Sprintf("2027-%02d-%02d %02d:%02d:%02d", month, day, hour, minute, second)
}

func (s tableSpec) updateTimestampValue() string {
	return s.updateDatetimeValue()
}

func (s tableSpec) updateTimeValue() string {
	hour, minute, second := clockValue(s.defaultSeed() + 9)
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func (s tableSpec) updateYearValue() int {
	return 2030 + (s.defaultSeed() % 10)
}

func (s tableSpec) updateEnumValue() string {
	values := []string{"green", "blue", "red"}
	return values[s.defaultSeed()%len(values)]
}

func (s tableSpec) updateSetValue() string {
	values := []string{"y", "x,z", "x,y", "y,z"}
	return values[s.defaultSeed()%len(values)]
}

func (s tableSpec) updateBitValue(marker int64) string {
	return rotateBits(int(marker%251) + s.tableIndex)
}

func tableName(n int) string {
	return fmt.Sprintf("table_info_sharing_%d", n)
}

func uniqueToken(tableN int, rowID uint64) string {
	return fmt.Sprintf("tis-%02d-%020d", tableN, rowID)
}

func quoteString(v string) string {
	return strings.ReplaceAll(v, "'", "''")
}

func recentRowRange(current uint64, batch int) (uint64, uint64) {
	if current == 0 {
		return 0, 0
	}

	window := minInt(defaultRecentRowWindow, int(current))
	offset := 0
	if window > 1 {
		offset = rand.Intn(window)
	}
	end := current - uint64(offset)
	start := uint64(1)
	if end >= uint64(batch) {
		start = end - uint64(batch) + 1
	}
	return start, end
}

func rotateBits(seed int) string {
	var builder strings.Builder
	builder.Grow(8)
	for i := 0; i < 8; i++ {
		if (seed+i)%2 == 0 {
			builder.WriteByte('1')
		} else {
			builder.WriteByte('0')
		}
	}
	return builder.String()
}

func monthValue(seed int) int {
	return seed%12 + 1
}

func dayValue(seed int) int {
	return seed%28 + 1
}

func clockValue(seed int) (int, int, int) {
	return seed % 24, (seed * 3) % 60, (seed * 7) % 60
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
