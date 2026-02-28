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

package bank

import (
	"bytes"
	"fmt"
	"math/rand"

	"workload/schema"
)

const createBankTableBase = `
create table if not exists bank_%d (
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

const createBankTablePartition = `PARTITION BY RANGE COLUMNS(col4)
(PARTITION p_202107 VALUES LESS THAN ('2021-08-01'),
 PARTITION p_202108 VALUES LESS THAN ('2021-09-01'),
 PARTITION p_202109 VALUES LESS THAN ('2021-10-01'),
 PARTITION p_202110 VALUES LESS THAN ('2021-11-01'),
 PARTITION p_202111 VALUES LESS THAN ('2021-12-01'),
 PARTITION p_202112 VALUES LESS THAN ('2022-01-01'),
 PARTITION p_202201 VALUES LESS THAN ('2022-02-01'),
 PARTITION p_202202 VALUES LESS THAN ('2022-03-01'),
 PARTITION p_202203 VALUES LESS THAN ('2022-04-01'),
 PARTITION p_202204 VALUES LESS THAN ('2022-05-01'),
 PARTITION p_202205 VALUES LESS THAN ('2022-06-01'),
 PARTITION p_202206 VALUES LESS THAN ('2022-07-01'),
 PARTITION p_202207 VALUES LESS THAN ('2022-08-01'),
 PARTITION p_202208 VALUES LESS THAN ('2022-09-01'),
 PARTITION p_202209 VALUES LESS THAN ('2022-10-01'),
 PARTITION p_202210 VALUES LESS THAN ('2022-11-01'),
 PARTITION p_202211 VALUES LESS THAN ('2022-12-01'),
 PARTITION p_202212 VALUES LESS THAN ('2023-01-01'),
 PARTITION p_202301 VALUES LESS THAN ('2023-02-01'),
 PARTITION p_202302 VALUES LESS THAN ('2023-03-01'),
 PARTITION p_202303 VALUES LESS THAN ('2023-04-01'),
 PARTITION p_202304 VALUES LESS THAN ('2023-05-01'),
 PARTITION p_202305 VALUES LESS THAN ('2023-06-01'),
 PARTITION p_202306 VALUES LESS THAN ('2023-07-01'),
 PARTITION p_202307 VALUES LESS THAN ('2023-08-01'),
 PARTITION p_202308 VALUES LESS THAN ('2023-09-01'),
 PARTITION p_202309 VALUES LESS THAN ('2023-10-01'),
 PARTITION p_202310 VALUES LESS THAN ('2023-11-01'),
 PARTITION p_202311 VALUES LESS THAN ('2023-12-01'),
 PARTITION p_202312 VALUES LESS THAN ('2024-01-01'),
 PARTITION p_202401 VALUES LESS THAN ('2024-02-01'),
 PARTITION p_202402 VALUES LESS THAN ('2024-03-01'),
 PARTITION p_202403 VALUES LESS THAN ('2024-04-01'),
 PARTITION p_202404 VALUES LESS THAN ('2024-05-01'),
 PARTITION p_202405 VALUES LESS THAN ('2024-06-01'),
 PARTITION p_202406 VALUES LESS THAN ('2024-07-01'),
 PARTITION p_202407 VALUES LESS THAN ('2024-08-01'),
 PARTITION p_202408 VALUES LESS THAN ('2024-09-01'),
 PARTITION p_202409 VALUES LESS THAN ('2024-10-01'),
 PARTITION p_202410 VALUES LESS THAN ('2024-11-01'),
 PARTITION p_202411 VALUES LESS THAN ('2024-12-01'),
 PARTITION p_202412 VALUES LESS THAN ('2025-01-01'),
 PARTITION p_202501 VALUES LESS THAN ('2025-02-01'),
 PARTITION p_202502 VALUES LESS THAN ('2025-03-01'),
 PARTITION p_202503 VALUES LESS THAN ('2025-04-01'),
 PARTITION p_202504 VALUES LESS THAN ('2025-05-01'),
 PARTITION p_202505 VALUES LESS THAN ('2025-06-01'),
 PARTITION p_202506 VALUES LESS THAN ('2025-07-01'),
 PARTITION p_202507 VALUES LESS THAN ('2025-08-01'),
 PARTITION p_202508 VALUES LESS THAN ('2025-09-01'),
 PARTITION p_202509 VALUES LESS THAN ('2025-10-01'),
 PARTITION p_202510 VALUES LESS THAN ('2025-11-01'),
 PARTITION p_202511 VALUES LESS THAN ('2025-12-01'),
 PARTITION p_202512 VALUES LESS THAN ('2026-01-01'),
 PARTITION p_202601 VALUES LESS THAN ('2026-02-01'),
 PARTITION p_202602 VALUES LESS THAN ('2026-03-01'),
 PARTITION p_202603 VALUES LESS THAN ('2026-04-01'),
 PARTITION p_202604 VALUES LESS THAN ('2026-05-01'),
 PARTITION p_202605 VALUES LESS THAN ('2026-06-01'),
 PARTITION p_202606 VALUES LESS THAN ('2026-07-01'),
 PARTITION p_202607 VALUES LESS THAN ('2026-08-01'),
 PARTITION p_202608 VALUES LESS THAN ('2026-09-01'),
 PARTITION p_202609 VALUES LESS THAN ('2026-10-01'),
 PARTITION p_202610 VALUES LESS THAN ('2026-11-01'),
 PARTITION p_202611 VALUES LESS THAN ('2026-12-01'),
 PARTITION p_202612 VALUES LESS THAN ('2027-01-01'),
 PARTITION p_202701 VALUES LESS THAN ('2027-02-01'),
 PARTITION p_202702 VALUES LESS THAN ('2027-03-01'),
 PARTITION p_202703 VALUES LESS THAN ('2027-04-01')) 
`

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
		// Strategy 1: Delete the newest rows by auto_id
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
		// Strategy 2: Range delete by partition key (col4)
		startTime, endTime := randomBankMonthRange()
		return fmt.Sprintf(
			"DELETE FROM %s WHERE col4 >= '%s' AND col4 < '%s' LIMIT %d",
			tableName, startTime, endTime, opts.Batch,
		)

	case 2:
		// Strategy 3: Conditional delete by indexed columns
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
	return fmt.Sprintf("bank_%d", n)
}

func randomBankDatetime() string {
	// bank partitions cover [< 2027-04-01), so keep generated timestamps within [2021-07, 2027-03].
	const (
		startYear  = 2021
		startMonth = 7
		endYear    = 2027
		endMonth   = 3
	)
	startTotal := startYear*12 + (startMonth - 1)
	endTotal := endYear*12 + (endMonth - 1)

	total := startTotal + rand.Intn(endTotal-startTotal+1)
	year := total / 12
	month := total%12 + 1

	day := rand.Intn(28) + 1
	hour := rand.Intn(24)
	minute := rand.Intn(60)
	second := rand.Intn(60)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
}

func randomBankMonthRange() (start string, end string) {
	// Align with the partition boundary to increase the chance of partition pruning.
	const (
		startYear  = 2021
		startMonth = 7
		endYear    = 2027
		endMonth   = 3
	)
	startTotal := startYear*12 + (startMonth - 1)
	endTotal := endYear*12 + (endMonth - 1)

	total := startTotal + rand.Intn(endTotal-startTotal+1)
	year := total / 12
	month := total%12 + 1
	start = fmt.Sprintf("%04d-%02d-01 00:00:00", year, month)

	next := total + 1
	endYearValue := next / 12
	endMonthValue := next%12 + 1
	end = fmt.Sprintf("%04d-%02d-01 00:00:00", endYearValue, endMonthValue)
	return start, end
}
