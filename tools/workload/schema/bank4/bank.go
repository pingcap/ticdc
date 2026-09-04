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

package bank4

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	"workload/schema"
)

const createBankTableBase = `
create table if not exists bank4_%d (
  col1 varchar(3) DEFAULT NULL,
  col2 varchar(2) DEFAULT NULL,
  col3 varchar(180) NOT NULL,
  col4 datetime NOT NULL,
  col5 varchar(2) DEFAULT NULL,
  col6 varchar(90) DEFAULT NULL,
  col7 varchar(2) DEFAULT NULL,
  col8 varchar(60) DEFAULT NULL,
  col9 varchar(14) DEFAULT NULL,
  col10 decimal(2,0) DEFAULT NULL,
  col11 decimal(4,0) DEFAULT NULL,
  col12 varchar(60) DEFAULT NULL,
  col13 decimal(2,0) DEFAULT NULL,
  col14 varchar(18) DEFAULT NULL,
  col15 varchar(14) DEFAULT NULL,
  col16 varchar(20) DEFAULT NULL,
  col17 varchar(180) DEFAULT NULL,
  col18 varchar(1) DEFAULT NULL,
  col19 varchar(1) DEFAULT NULL,
  col20 varchar(1) DEFAULT NULL,
  col21 varchar(80) DEFAULT NULL,
  col22 varchar(4) DEFAULT NULL,
  col23 decimal(15,0) DEFAULT NULL,
  col24 varchar(5) DEFAULT NULL,
  col25 varchar(26) DEFAULT NULL,
  col26 varchar(2) DEFAULT NULL,
  col27 datetime DEFAULT NULL,
  col28 decimal(3,0) DEFAULT NULL,
  col29 decimal(3,0) DEFAULT NULL,
  col30 decimal(3,0) DEFAULT NULL,
  auto_id bigint(20) NOT NULL AUTO_INCREMENT,
  KEY idx1 (col14),
  KEY idx2 (col27,col24,col22),
  KEY idx3 (col3,col24,col4),
  KEY idx4 (col21,col23),
  KEY idx5 (col15),
  PRIMARY KEY (auto_id,col3,col4) /*T![clustered_index] NONCLUSTERED */,
  KEY idx6 (col3)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

const (
	bankPartitionStartYear  = 2021
	bankPartitionStartMonth = 7
	bankPartitionEndYear    = 2031
	bankPartitionEndMonth   = 12
)

var createBankTablePartition = buildCreateBankTablePartition()

type BankWorkload struct {
	isPartitioned bool
}

func NewBankWorkload(isPartitioned bool) schema.Workload {
	return &BankWorkload{isPartitioned: isPartitioned}
}

func (c *BankWorkload) BuildCreateTableStatement(n int) string {
	createSQL := fmt.Sprintf(createBankTableBase, n)
	if c.isPartitioned {
		return createSQL + createBankTablePartition + ";"
	}
	return createSQL + ";"
}

func (c *BankWorkload) BuildInsertSql(tableN int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}

	tableName := getBankTableName(tableN)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(
		"insert into %s (col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18,col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29,col30) values",
		tableName,
	))

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			buf.WriteString(",")
		}

		n := rand.Int63()
		col1Value := "A01"
		col2Value := "B2"
		col3Value := fmt.Sprintf("acct-%d", n)
		col4Value := randomBankDatetime()

		col5Value := fmt.Sprintf("%02d", rand.Intn(100))
		col6Value := fmt.Sprintf("desc-%d", n%1000000)
		col7Value := fmt.Sprintf("%02d", rand.Intn(100))
		col8Value := fmt.Sprintf("branch-%d", n%1000000)
		col9Value := fmt.Sprintf("%014d", n%100000000000000)
		col10Value := rand.Intn(100)
		col11Value := rand.Intn(10000)
		col12Value := fmt.Sprintf("note-%d", n%1000000)
		col13Value := rand.Intn(100)
		col14Value := "acct"
		col15Value := fmt.Sprintf("i%013d", n%10000000000000)
		col16Value := fmt.Sprintf("type-%d", n%100000)
		col17Value := fmt.Sprintf("memo-%d", n)
		col18Value := fmt.Sprintf("%d", rand.Intn(2))
		col19Value := fmt.Sprintf("%d", rand.Intn(2))
		col20Value := fmt.Sprintf("%d", rand.Intn(2))
		col21Value := fmt.Sprintf("customer-%d", n%1000000)
		col22Value := "A001"
		col23Value := n % 1000000000000000
		col24Value := "B0001"
		col25Value := fmt.Sprintf("ref-%d", n%1000000000000000000)
		col26Value := "E1"
		col27Value := randomBankDatetime()
		col28Value := rand.Intn(1000)
		col29Value := rand.Intn(1000)
		col30Value := rand.Intn(1000)

		buf.WriteString(fmt.Sprintf(
			"('%s','%s','%s','%s','%s','%s','%s','%s','%s',%d,%d,'%s',%d,'%s','%s','%s','%s','%s','%s','%s','%s','%s',%d,'%s','%s','%s','%s',%d,%d,%d)",
			col1Value,
			col2Value,
			col3Value,
			col4Value,
			col5Value,
			col6Value,
			col7Value,
			col8Value,
			col9Value,
			col10Value,
			col11Value,
			col12Value,
			col13Value,
			col14Value,
			col15Value,
			col16Value,
			col17Value,
			col18Value,
			col19Value,
			col20Value,
			col21Value,
			col22Value,
			col23Value,
			col24Value,
			col25Value,
			col26Value,
			col27Value,
			col28Value,
			col29Value,
			col30Value,
		))
	}
	return buf.String()
}

func (c *BankWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	if opts.Batch <= 0 {
		return ""
	}

	tableName := getBankTableName(opts.TableIndex)
	startTime, endTime := randomBankMonthRange()
	newCol30 := rand.Intn(1000)
	newCol15 := fmt.Sprintf("u%013d", rand.Int63n(10000000000000))
	newCol27 := randomBankDatetime()

	return fmt.Sprintf(
		`UPDATE %[1]s
SET col30 = %[2]d, col15 = '%[3]s', col27 = '%[4]s'
WHERE auto_id IN (
  SELECT auto_id FROM (
    SELECT auto_id FROM %[1]s
    WHERE col4 >= '%[5]s' AND col4 < '%[6]s'
    ORDER BY auto_id DESC LIMIT %[7]d
  ) t
)`,
		tableName,
		newCol30,
		newCol15,
		newCol27,
		startTime,
		endTime,
		opts.Batch,
	)
}

func (c *BankWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	if opts.Batch <= 0 {
		return ""
	}

	deleteType := rand.Intn(3)
	tableName := getBankTableName(opts.TableIndex)

	switch deleteType {
	case 0:
		return fmt.Sprintf(
			`DELETE FROM %[1]s
WHERE auto_id IN (
  SELECT auto_id FROM (
    SELECT auto_id FROM %[1]s ORDER BY auto_id DESC LIMIT %[2]d
  ) t
)`,
			tableName, opts.Batch,
		)
	case 1:
		startTime, endTime := randomBankMonthRange()
		return fmt.Sprintf(
			"DELETE FROM %s WHERE col4 >= '%s' AND col4 < '%s' LIMIT %d",
			tableName, startTime, endTime, opts.Batch,
		)
	case 2:
		return fmt.Sprintf(
			"DELETE FROM %s WHERE col14 = 'acct' AND col22 = 'A001' LIMIT %d",
			tableName, opts.Batch,
		)
	default:
		return ""
	}
}

func (c *BankWorkload) BuildDDLSql(opts schema.DDLOption) string {
	tableName := getBankTableName(opts.TableIndex)
	return fmt.Sprintf("truncate table %s;", tableName)
}

func getBankTableName(n int) string {
	return fmt.Sprintf("bank4_%d", n)
}

func buildCreateBankTablePartition() string {
	startTotal, endTotal := bankPartitionMonthRange()

	var builder strings.Builder
	builder.WriteString("PARTITION BY RANGE COLUMNS(col4)\n(")
	for total := startTotal; total <= endTotal; total++ {
		year, month := monthIndexToYearMonth(total)
		nextYear, nextMonth := monthIndexToYearMonth(total + 1)
		if total > startTotal {
			builder.WriteString(",\n ")
		}
		builder.WriteString(fmt.Sprintf(
			"PARTITION p_%04d%02d VALUES LESS THAN ('%04d-%02d-01')",
			year, month, nextYear, nextMonth,
		))
	}
	builder.WriteString(")")
	return builder.String()
}

func bankPartitionMonthRange() (startTotal int, endTotal int) {
	startTotal = bankPartitionStartYear*12 + (bankPartitionStartMonth - 1)
	endTotal = bankPartitionEndYear*12 + (bankPartitionEndMonth - 1)
	return startTotal, endTotal
}

func monthIndexToYearMonth(total int) (year int, month int) {
	return total / 12, total%12 + 1
}

func randomBankDatetime() string {
	startTotal, endTotal := bankPartitionMonthRange()

	total := startTotal + rand.Intn(endTotal-startTotal+1)
	year, month := monthIndexToYearMonth(total)

	day := rand.Intn(28) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
}

func randomBankMonthRange() (start string, end string) {
	startTotal, endTotal := bankPartitionMonthRange()

	total := startTotal + rand.Intn(endTotal-startTotal+1)
	year, month := monthIndexToYearMonth(total)
	start = fmt.Sprintf("%04d-%02d-01 00:00:00", year, month)

	endYearValue, endMonthValue := monthIndexToYearMonth(total + 1)
	end = fmt.Sprintf("%04d-%02d-01 00:00:00", endYearValue, endMonthValue)
	return start, end
}
