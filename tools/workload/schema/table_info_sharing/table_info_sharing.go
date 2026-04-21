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

const createTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  id BIGINT NOT NULL,
  c_tinyint TINYINT NOT NULL DEFAULT %d,
  c_smallint SMALLINT NOT NULL DEFAULT %d,
  c_mediumint MEDIUMINT NOT NULL DEFAULT %d,
  c_int INT NOT NULL DEFAULT %d,
  c_bigint BIGINT NOT NULL DEFAULT %d,
  c_unsigned BIGINT UNSIGNED NOT NULL DEFAULT %d,
  c_decimal DECIMAL(20,6) NOT NULL DEFAULT %s,
  c_float FLOAT NOT NULL DEFAULT %s,
  c_double DOUBLE NOT NULL DEFAULT %s,
  c_bool BOOLEAN NOT NULL DEFAULT %d,
  c_bit BIT(8) NOT NULL DEFAULT b'%s',
  c_char CHAR(8) NOT NULL DEFAULT '%s',
  c_varchar VARCHAR(64) NOT NULL DEFAULT '%s',
  c_date DATE NOT NULL DEFAULT '%s',
  c_datetime DATETIME NOT NULL DEFAULT '%s',
  c_timestamp TIMESTAMP NOT NULL DEFAULT '%s',
  c_time TIME NOT NULL DEFAULT '%s',
  c_year YEAR NOT NULL DEFAULT %d,
  c_enum ENUM('red','green','blue') NOT NULL DEFAULT '%s',
  c_set SET('x','y','z') NOT NULL DEFAULT '%s',
  c_json JSON NULL,
  c_text TEXT NULL,
  c_blob BLOB NULL,
  c_binary BINARY(8) NULL,
  c_varbinary VARBINARY(16) NULL,
  uk_token VARCHAR(64) NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_token (uk_token),
  KEY idx_temporal (c_date, c_datetime),
  KEY idx_enum_set (c_enum, c_set)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

const (
	defaultRecentRowWindow = 256
)

type tableSpec struct {
	tableIndex int
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
	return fmt.Sprintf(
		createTableFormat,
		tableName(n),
		spec.tinyintDefault(),
		spec.smallintDefault(),
		spec.mediumintDefault(),
		spec.intDefault(),
		spec.bigintDefault(),
		spec.unsignedDefault(),
		spec.decimalDefault(),
		spec.floatDefault(),
		spec.doubleDefault(),
		spec.boolDefault(),
		spec.bitDefault(),
		spec.charDefault(),
		spec.varcharDefault(),
		spec.dateDefault(),
		spec.datetimeDefault(),
		spec.timestampDefault(),
		spec.timeDefault(),
		spec.yearDefault(),
		spec.enumDefault(),
		spec.setDefault(),
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
	return tableSpec{tableIndex: tableIndex}
}

func (s tableSpec) tinyintDefault() int {
	return (s.tableIndex % 63) + 1
}

func (s tableSpec) smallintDefault() int {
	return s.tableIndex*10 + 1
}

func (s tableSpec) mediumintDefault() int {
	return s.tableIndex*100 + 1
}

func (s tableSpec) intDefault() int {
	return s.tableIndex*1000 + 1
}

func (s tableSpec) bigintDefault() int64 {
	return int64(s.tableIndex)*10000 + 1
}

func (s tableSpec) unsignedDefault() uint64 {
	return uint64(s.tableIndex)*100000 + 1
}

func (s tableSpec) decimalDefault() string {
	return fmt.Sprintf("%d.%06d", s.tableIndex, (s.tableIndex*111111)%1000000)
}

func (s tableSpec) floatDefault() string {
	return fmt.Sprintf("%d.25", s.tableIndex)
}

func (s tableSpec) doubleDefault() string {
	return fmt.Sprintf("%d.125", s.tableIndex)
}

func (s tableSpec) boolDefault() int {
	return s.tableIndex % 2
}

func (s tableSpec) bitDefault() string {
	return rotateBits(s.tableIndex)
}

func (s tableSpec) charDefault() string {
	return fmt.Sprintf("c%07d", s.tableIndex%10000000)
}

func (s tableSpec) varcharDefault() string {
	return fmt.Sprintf("varchar-%02d", s.tableIndex)
}

func (s tableSpec) dateDefault() string {
	month := monthValue(s.tableIndex)
	day := dayValue(s.tableIndex)
	return fmt.Sprintf("2026-%02d-%02d", month, day)
}

func (s tableSpec) datetimeDefault() string {
	month := monthValue(s.tableIndex)
	day := dayValue(s.tableIndex)
	hour, minute, second := clockValue(s.tableIndex)
	return fmt.Sprintf("2026-%02d-%02d %02d:%02d:%02d", month, day, hour, minute, second)
}

func (s tableSpec) timestampDefault() string {
	return s.datetimeDefault()
}

func (s tableSpec) timeDefault() string {
	hour, minute, second := clockValue(s.tableIndex)
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func (s tableSpec) yearDefault() int {
	return 2020 + (s.tableIndex % 10)
}

func (s tableSpec) enumDefault() string {
	values := []string{"red", "green", "blue"}
	return values[s.tableIndex%len(values)]
}

func (s tableSpec) setDefault() string {
	values := []string{"x", "x,y", "y,z", "x,z"}
	return values[s.tableIndex%len(values)]
}

func (s tableSpec) updateDateValue() string {
	month := monthValue(s.tableIndex + 5)
	day := dayValue(s.tableIndex + 7)
	return fmt.Sprintf("2027-%02d-%02d", month, day)
}

func (s tableSpec) updateDatetimeValue() string {
	month := monthValue(s.tableIndex + 5)
	day := dayValue(s.tableIndex + 7)
	hour, minute, second := clockValue(s.tableIndex + 9)
	return fmt.Sprintf("2027-%02d-%02d %02d:%02d:%02d", month, day, hour, minute, second)
}

func (s tableSpec) updateTimestampValue() string {
	return s.updateDatetimeValue()
}

func (s tableSpec) updateTimeValue() string {
	hour, minute, second := clockValue(s.tableIndex + 9)
	return fmt.Sprintf("%02d:%02d:%02d", hour, minute, second)
}

func (s tableSpec) updateYearValue() int {
	return 2030 + (s.tableIndex % 10)
}

func (s tableSpec) updateEnumValue() string {
	values := []string{"green", "blue", "red"}
	return values[s.tableIndex%len(values)]
}

func (s tableSpec) updateSetValue() string {
	values := []string{"y", "x,z", "x,y", "y,z"}
	return values[s.tableIndex%len(values)]
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
