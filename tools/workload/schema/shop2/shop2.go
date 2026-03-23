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

package shop2

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"

	"workload/schema"
)

const (
	shop2DefaultKeySpace = 1_000_000
)

const createShop2ItemTable = `
CREATE TABLE IF NOT EXISTS shop2_item_%d (
  c001 bigint(20) NOT NULL AUTO_INCREMENT,
  c002 bigint(20) NOT NULL,
  c003 bigint(20) NOT NULL,
  c004 varchar(127) NOT NULL,
  c005 varchar(127) NOT NULL,
  c006 varchar(255) DEFAULT NULL,
  c007 bigint(20) DEFAULT NULL,
  c008 bigint(20) DEFAULT NULL,
  c009 bigint(20) DEFAULT NULL,
  c010 json DEFAULT NULL,
  c011 double DEFAULT NULL,
  c012 double DEFAULT NULL,
  c013 int(11) DEFAULT NULL,
  c014 int(11) DEFAULT NULL,
  c015 varchar(31) DEFAULT NULL,
  c016 int(11) DEFAULT NULL,
  c017 varchar(31) DEFAULT NULL,
  c018 varchar(127) DEFAULT NULL,
  c019 tinyint(1) DEFAULT NULL,
  c020 bigint(20) DEFAULT NULL,
  c021 int(11) DEFAULT NULL,
  c022 int(11) DEFAULT NULL,
  c023 blob DEFAULT NULL,
  c024 blob DEFAULT NULL,
  c025 blob DEFAULT NULL,
  c026 varchar(511) DEFAULT NULL,
  c027 varchar(10000) DEFAULT NULL,
  c028 varchar(10000) DEFAULT NULL,
  c029 varchar(511) DEFAULT NULL,
  c030 varchar(511) DEFAULT NULL,
  c031 json DEFAULT NULL,
  c032 json DEFAULT NULL,
  c033 varchar(127) DEFAULT NULL,
  c034 varchar(31) DEFAULT NULL,
  c035 varchar(31) DEFAULT NULL,
  c036 varchar(31) DEFAULT NULL,
  c037 varchar(31) DEFAULT NULL,
  c038 int(11) DEFAULT NULL,
  c039 int(11) DEFAULT NULL,
  c040 blob DEFAULT NULL,
  c041 varchar(2047) DEFAULT NULL,
  c042 varchar(511) DEFAULT NULL,
  c043 varchar(511) DEFAULT NULL,
  c044 varchar(511) DEFAULT NULL,
  c045 varchar(511) DEFAULT NULL,
  c046 varchar(511) DEFAULT NULL,
  c047 bigint(20) DEFAULT NULL,
  c048 bigint(20) DEFAULT NULL,
  c049 int(11) DEFAULT NULL,
  c050 int(11) DEFAULT NULL,
  c051 int(11) DEFAULT NULL,
  c052 int(11) DEFAULT NULL,
  c053 int(11) DEFAULT NULL,
  c054 double DEFAULT NULL,
  c055 int(11) DEFAULT NULL,
  c056 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  c057 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  c058 tinyint(1) DEFAULT NULL,
  c059 tinyint(1) DEFAULT NULL,
  c060 tinyint(1) DEFAULT NULL,
  c061 blob DEFAULT NULL,
  c062 blob DEFAULT NULL,
  c063 char(32) DEFAULT NULL,
  c064 char(32) DEFAULT NULL,
  c065 bigint(20) DEFAULT NULL,
  c066 varchar(31) DEFAULT NULL,
  c067 blob DEFAULT NULL,
  c068 bigint(20) DEFAULT NULL,
  c069 bigint(20) DEFAULT NULL,
  c070 bigint(20) DEFAULT NULL,
  c071 bigint(20) DEFAULT NULL,
  c072 bigint(20) DEFAULT NULL,
  c073 bigint(20) DEFAULT NULL,
  c074 bigint(20) DEFAULT NULL,
  c075 varchar(2047) DEFAULT NULL,
  c076 varchar(511) DEFAULT NULL,
  c077 varchar(2047) DEFAULT NULL,
  c078 varchar(511) DEFAULT NULL,
  c079 varchar(2047) DEFAULT NULL,
  c080 varchar(511) DEFAULT NULL,
  c081 varchar(2047) DEFAULT NULL,
  c082 varchar(511) DEFAULT NULL,
  c083 varchar(2047) DEFAULT NULL,
  c084 varchar(511) DEFAULT NULL,
  c085 varchar(2047) DEFAULT NULL,
  c086 varchar(511) DEFAULT NULL,
  c087 varchar(2047) DEFAULT NULL,
  c088 varchar(511) DEFAULT NULL,
  c089 varchar(2047) DEFAULT NULL,
  c090 varchar(511) DEFAULT NULL,
  c091 varchar(2047) DEFAULT NULL,
  c092 varchar(511) DEFAULT NULL,
  c093 varchar(2047) DEFAULT NULL,
  c094 varchar(511) DEFAULT NULL,
  c095 varchar(2047) DEFAULT NULL,
  c096 varchar(511) DEFAULT NULL,
  c097 varchar(2047) DEFAULT NULL,
  c098 varchar(511) DEFAULT NULL,
  c099 varchar(2047) DEFAULT NULL,
  c100 varchar(511) DEFAULT NULL,
  c101 varchar(2047) DEFAULT NULL,
  c102 varchar(511) DEFAULT NULL,
  c103 varchar(2047) DEFAULT NULL,
  c104 varchar(511) DEFAULT NULL,
  c105 varchar(2047) DEFAULT NULL,
  c106 varchar(511) DEFAULT NULL,
  c107 varchar(2047) DEFAULT NULL,
  c108 varchar(511) DEFAULT NULL,
  c109 varchar(2047) DEFAULT NULL,
  c110 varchar(511) DEFAULT NULL,
  c111 varchar(2047) DEFAULT NULL,
  c112 varchar(511) DEFAULT NULL,
  c113 varchar(2047) DEFAULT NULL,
  c114 varchar(511) DEFAULT NULL,
  c115 mediumblob DEFAULT NULL,
  c116 json DEFAULT NULL,
  c117 json DEFAULT NULL,
  c118 varchar(2047) DEFAULT NULL,
  c119 varchar(2047) DEFAULT NULL,
  c120 json DEFAULT NULL,
  c121 json DEFAULT NULL,
  c122 tinyint(1) DEFAULT NULL,
  c123 tinyint(1) DEFAULT NULL,
  c124 varchar(2047) DEFAULT NULL,
  c125 varchar(511) DEFAULT NULL,
  c126 varchar(2047) DEFAULT NULL,
  c127 varchar(511) DEFAULT NULL,
  c128 varchar(511) DEFAULT NULL,
  c129 mediumblob DEFAULT NULL,
  c130 varchar(2047) DEFAULT NULL,
  c131 tinyint(1) DEFAULT NULL,
  c132 bigint(20) DEFAULT NULL,
  c133 blob DEFAULT NULL,
  c134 int(11) DEFAULT NULL,
  c135 bigint(20) DEFAULT NULL,
  c136 double DEFAULT NULL,
  c137 int(11) DEFAULT NULL,
  c138 int(11) DEFAULT NULL,
  c139 timestamp DEFAULT NULL,
  c140 timestamp DEFAULT NULL,
  c141 mediumblob DEFAULT NULL,
  c142 json DEFAULT NULL,
  c143 json DEFAULT NULL,
  c144 json DEFAULT NULL,
  c145 blob DEFAULT NULL,
  c146 int(11) DEFAULT NULL,
  c147 varchar(2047) DEFAULT NULL,
  c148 varchar(2047) DEFAULT NULL,
  c149 json DEFAULT NULL,
  c150 json DEFAULT NULL,
  c151 json DEFAULT NULL,
  c152 varchar(32) DEFAULT NULL,
  PRIMARY KEY (c001) /*T![clustered_index] CLUSTERED */,
  KEY idx_c002 (c002),
  KEY idx_c003 (c003),
  KEY idx_c004 (c004)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type Shop2Workload struct {
	rowSize          int
	tableStartIndex  int
	perTableKeySpace uint64
	tableSeq         []atomic.Uint64
}

var shop2InsertColumnList = buildInsertColumnList()

func NewShop2Workload(totalRowCount uint64, rowSize int, tableCount int, tableStartIndex int) schema.Workload {
	if rowSize <= 0 {
		rowSize = 8 * 1024
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	perTableKeySpace := uint64(shop2DefaultKeySpace)
	if totalRowCount > 0 {
		perTableKeySpace = totalRowCount / uint64(tableCount)
		if perTableKeySpace == 0 {
			perTableKeySpace = 1
		}
	}

	return &Shop2Workload{
		rowSize:          rowSize,
		tableStartIndex:  tableStartIndex,
		perTableKeySpace: perTableKeySpace,
		tableSeq:         make([]atomic.Uint64, tableCount),
	}
}

func tableName(tableIndex int) string {
	return fmt.Sprintf("shop2_item_%d", tableIndex)
}

func (w *Shop2Workload) BuildCreateTableStatement(tableIndex int) string {
	return fmt.Sprintf(createShop2ItemTable, tableIndex)
}

func (w *Shop2Workload) BuildInsertSql(tableIndex int, batchSize int) string {
	seeds := make([]int64, 0, batchSize)
	for range batchSize {
		seeds = append(seeds, w.nextSeed(tableIndex))
	}
	return w.buildInsertSQL(tableIndex, seeds)
}

func (w *Shop2Workload) BuildUpdateSql(opt schema.UpdateOption) string {
	if opt.Batch <= 0 {
		return ""
	}

	seed := w.sampleSeed()
	table := tableName(opt.TableIndex)
	setText := quoteLiteral(w.textValue(511, "c026", opt.TableIndex, seed))
	setJSON := quoteLiteral(w.jsonValue("c031", opt.TableIndex, seed))
	setBlob := quoteLiteral(w.blobValue("c061", opt.TableIndex, seed, false))
	setMediumBlob := quoteLiteral(w.blobValue("c115", opt.TableIndex, seed, true))
	setTail := quoteLiteral(w.textValue(32, "c152", opt.TableIndex, seed))

	if rand.Intn(2) == 0 {
		return fmt.Sprintf(
			"UPDATE %s SET c057 = CURRENT_TIMESTAMP, c011 = c011 + 0.0100, c012 = c012 + 0.0200, c026 = %s, c031 = %s, c047 = c047 + 1, c061 = %s, c115 = %s, c152 = %s WHERE c003 = %d LIMIT %d",
			table, setText, setJSON, setBlob, setMediumBlob, setTail, w.merchantID(seed), opt.Batch)
	}

	return fmt.Sprintf(
		"UPDATE %s SET c057 = CURRENT_TIMESTAMP, c011 = c011 + 0.0100, c012 = c012 + 0.0200, c026 = %s, c031 = %s, c047 = c047 + 1, c061 = %s, c115 = %s, c152 = %s WHERE c002 = %d LIMIT %d",
		table, setText, setJSON, setBlob, setMediumBlob, setTail, w.catalogID(seed), opt.Batch)
}

func (w *Shop2Workload) BuildDeleteSql(opt schema.DeleteOption) string {
	if opt.Batch <= 0 {
		return ""
	}

	table := tableName(opt.TableIndex)
	seed := w.sampleSeed()
	if rand.Intn(2) == 0 {
		return fmt.Sprintf("DELETE FROM %s WHERE c003 = %d LIMIT %d", table, w.merchantID(seed), opt.Batch)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE c002 = %d LIMIT %d", table, w.catalogID(seed), opt.Batch)
}

func buildInsertColumnList() string {
	columns := make([]string, 0, 151)
	for idx := 2; idx <= 152; idx++ {
		columns = append(columns, columnName(idx))
	}
	return strings.Join(columns, ",")
}

func (w *Shop2Workload) buildInsertSQL(tableIndex int, seeds []int64) string {
	if len(seeds) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.Grow(len(seeds) * 2048)
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName(tableIndex))
	sb.WriteString(" (")
	sb.WriteString(shop2InsertColumnList)
	sb.WriteString(") VALUES ")

	for idx, seed := range seeds {
		if idx > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		sb.WriteString(w.generateInsertRow(tableIndex, seed))
		sb.WriteString(")")
	}
	return sb.String()
}

func (w *Shop2Workload) generateInsertRow(tableIndex int, seed int64) string {
	values := make([]string, 0, 151)

	addBigInt := func(columnSeed int64) {
		values = append(values, strconv.FormatInt(w.bigIntValue(columnSeed, seed), 10))
	}
	addInt := func(columnSeed int64) {
		values = append(values, strconv.FormatInt(w.intValue(columnSeed, seed), 10))
	}
	addTinyInt := func(columnSeed int64) {
		values = append(values, strconv.FormatInt((seed+columnSeed)%2, 10))
	}
	addDouble := func(columnSeed int64) {
		values = append(values, strconv.FormatFloat(w.doubleValue(columnSeed, seed), 'f', 4, 64))
	}
	addText := func(limit int, label string) {
		values = append(values, quoteLiteral(w.textValue(limit, label, tableIndex, seed)))
	}
	addJSON := func(label string) {
		values = append(values, quoteLiteral(w.jsonValue(label, tableIndex, seed)))
	}
	addBlob := func(label string, medium bool) {
		values = append(values, quoteLiteral(w.blobValue(label, tableIndex, seed, medium)))
	}
	addTimestamp := func(columnSeed int64) {
		values = append(values, quoteLiteral(w.timestampValue(columnSeed, seed)))
	}

	values = append(values, strconv.FormatInt(w.catalogID(seed), 10))
	values = append(values, strconv.FormatInt(w.merchantID(seed), 10))
	addText(127, "c004")
	addText(127, "c005")
	addText(255, "c006")
	addBigInt(7)
	addBigInt(8)
	addBigInt(9)
	addJSON("c010")
	addDouble(11)
	addDouble(12)
	addInt(13)
	addInt(14)
	addText(31, "c015")
	addInt(16)
	addText(31, "c017")
	addText(127, "c018")
	addTinyInt(19)
	addBigInt(20)
	addInt(21)
	addInt(22)
	addBlob("c023", false)
	addBlob("c024", false)
	addBlob("c025", false)
	addText(511, "c026")
	addText(10000, "c027")
	addText(10000, "c028")
	addText(511, "c029")
	addText(511, "c030")
	addJSON("c031")
	addJSON("c032")
	addText(127, "c033")
	addText(31, "c034")
	addText(31, "c035")
	addText(31, "c036")
	addText(31, "c037")
	addInt(38)
	addInt(39)
	addBlob("c040", false)
	addText(2047, "c041")
	for col := 42; col <= 46; col++ {
		addText(511, columnName(col))
	}
	addBigInt(47)
	addBigInt(48)
	for col := int64(49); col <= 53; col++ {
		addInt(col)
	}
	addDouble(54)
	addInt(55)
	values = append(values, "CURRENT_TIMESTAMP")
	values = append(values, "CURRENT_TIMESTAMP")
	addTinyInt(58)
	addTinyInt(59)
	addTinyInt(60)
	addBlob("c061", false)
	addBlob("c062", false)
	addText(32, "c063")
	addText(32, "c064")
	addBigInt(65)
	addText(31, "c066")
	addBlob("c067", false)
	addBigInt(68)
	addBigInt(69)
	for col := int64(70); col <= 74; col++ {
		addBigInt(col)
	}
	for col := 75; col <= 114; col += 2 {
		addText(2047, columnName(col))
		addText(511, columnName(col+1))
	}
	addBlob("c115", true)
	addJSON("c116")
	addJSON("c117")
	addText(2047, "c118")
	addText(2047, "c119")
	addJSON("c120")
	addJSON("c121")
	addTinyInt(122)
	addTinyInt(123)
	addText(2047, "c124")
	addText(511, "c125")
	addText(2047, "c126")
	addText(511, "c127")
	addText(511, "c128")
	addBlob("c129", true)
	addText(2047, "c130")
	addTinyInt(131)
	addBigInt(132)
	addBlob("c133", false)
	addInt(134)
	addBigInt(135)
	addDouble(136)
	addInt(137)
	addInt(138)
	addTimestamp(139)
	addTimestamp(140)
	addBlob("c141", true)
	addJSON("c142")
	addJSON("c143")
	addJSON("c144")
	addBlob("c145", false)
	addInt(146)
	addText(2047, "c147")
	addText(2047, "c148")
	addJSON("c149")
	addJSON("c150")
	addJSON("c151")
	addText(32, "c152")

	if len(values) != 151 {
		panic(fmt.Sprintf("unexpected shop2 values count: %d", len(values)))
	}
	return strings.Join(values, ",")
}

func (w *Shop2Workload) catalogID(rowID int64) int64 {
	return 1_000 + rowID%8_192
}

func (w *Shop2Workload) merchantID(rowID int64) int64 {
	return 10_000 + (rowID/7)%16_384
}

func (w *Shop2Workload) bigIntValue(seed int64, rowID int64) int64 {
	return seed*1_000 + rowID%1_000_000
}

func (w *Shop2Workload) intValue(seed int64, rowID int64) int64 {
	return 1 + (rowID+seed*17)%10_000
}

func (w *Shop2Workload) doubleValue(seed int64, rowID int64) float64 {
	raw := 1 + (rowID+seed*31)%50_000
	return float64(raw) / 100
}

func (w *Shop2Workload) timestampValue(seed int64, rowID int64) string {
	year := 2026 + int((rowID+seed)%2)
	month := 1 + int((rowID+seed*3)%12)
	day := 1 + int((rowID+seed*5)%28)
	hour := int((rowID + seed*7) % 24)
	minute := int((rowID + seed*11) % 60)
	second := int((rowID + seed*13) % 60)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
}

func (w *Shop2Workload) textValue(limit int, label string, tableIndex int, rowID int64) string {
	target := clampInt(w.rowSize/10, 24, 768)
	if target > limit {
		target = limit
	}
	return fillToLength(fmt.Sprintf("%s-t%d-r%d-", label, tableIndex, rowID), target)
}

func (w *Shop2Workload) jsonValue(label string, tableIndex int, rowID int64) string {
	target := clampInt(w.rowSize/24+32, 32, 384)
	header := fmt.Sprintf("{\"k\":\"%s\",\"t\":%d,\"r\":%d,\"v\":\"", label, tableIndex, rowID)
	footer := "\"}"
	payloadLen := target - len(header) - len(footer)
	if payloadLen < 0 {
		payloadLen = 0
	}
	return header + fillToLength(label, payloadLen) + footer
}

func (w *Shop2Workload) blobValue(label string, tableIndex int, rowID int64, medium bool) string {
	target := clampInt(w.rowSize/16+48, 48, 512)
	if medium {
		target = clampInt(w.rowSize/8+96, 96, 1536)
	}
	return fillToLength(fmt.Sprintf("%s-t%d-r%d-", label, tableIndex, rowID), target)
}

func fillToLength(pattern string, length int) string {
	if length <= 0 {
		return ""
	}
	if len(pattern) >= length {
		return pattern[:length]
	}

	var sb strings.Builder
	sb.Grow(length)
	for sb.Len() < length {
		remaining := length - sb.Len()
		if remaining >= len(pattern) {
			sb.WriteString(pattern)
			continue
		}
		sb.WriteString(pattern[:remaining])
	}
	return sb.String()
}

func quoteLiteral(value string) string {
	return "'" + value + "'"
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func columnName(index int) string {
	return fmt.Sprintf("c%03d", index)
}

func (w *Shop2Workload) tableSlot(tableIndex int) int {
	if len(w.tableSeq) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 || slot >= len(w.tableSeq) {
		return 0
	}
	return slot
}

func (w *Shop2Workload) nextSeed(tableIndex int) int64 {
	slot := w.tableSlot(tableIndex)
	seq := w.tableSeq[slot].Add(1)
	return w.seedForSlot(slot, seq)
}

func (w *Shop2Workload) sampleSeed() int64 {
	return int64(rand.Int63n(int64(shop2DefaultKeySpace))) + 1
}

func (w *Shop2Workload) seedForSlot(slot int, offset uint64) int64 {
	return int64(uint64(slot)*w.perTableKeySpace + offset)
}
