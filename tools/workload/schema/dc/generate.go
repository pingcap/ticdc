// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the License);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dc

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func randInt64(min, max int64) int64 {
	if max < min {
		min, max = max, min
	}
	return min + rand.Int63n(max-min+1)
}

func randChoice(choices []string) string {
	return choices[rand.Intn(len(choices))]
}

func randDecimalStr(fracDigits int, minInt, maxInt int64) string {
	intPart := randInt64(minInt, maxInt)
	scale := int64(1)
	for i := 0; i < fracDigits; i++ {
		scale *= 10
	}
	fracPart := randInt64(0, scale-1)
	return fmt.Sprintf("%d.%0*d", intPart, fracDigits, fracPart)
}

func randVarchar(prefix string, maxLen int) string {
	raw := fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano(), randInt64(1000, 999999))
	if len(raw) > maxLen {
		return raw[:maxLen]
	}
	return raw
}

func randDateTimeAround(base time.Time, minDelta, maxDelta time.Duration) time.Time {
	if maxDelta < minDelta {
		minDelta, maxDelta = maxDelta, minDelta
	}
	delta := minDelta + time.Duration(rand.Int63n(int64(maxDelta-minDelta)+1))
	return base.Add(delta)
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func quote(s string) string {
	return "'" + escapeSQLString(s) + "'"
}

func formatDateTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatDate(t time.Time) string {
	return t.Format("2006-01-02")
}

func BuildInsertForDC(tableN int) string {
	now := time.Now()
	col1 := fmt.Sprintf("%d%d", time.Now().Unix(), randInt64(100000000, 999999999))
	col2 := randVarchar("col1", 50)
	col3 := randVarchar("col2", 100)
	col4 := randVarchar("c3", 10)
	col5 := "105"
	col6 := randVarchar("c5", 18)
	col7 := randVarchar("c6", 3)
	col8 := randVarchar("c7", 14)
	col9 := randInt64(0, 127)
	col10 := randVarchar("c9", 50)
	col11 := randVarchar("c10", 6)
	col12 := randVarchar("c11", 6)
	col13 := randDecimalStr(6, 0, 999999999999)
	col14 := randDecimalStr(6, 0, 999999999999)
	col15 := randDecimalStr(6, 0, 999999999999)
	col16 := randDecimalStr(6, 0, 999999999999)
	col17 := randDecimalStr(6, 0, 999999999999)
	col18 := randDateTimeAround(now, -24*time.Hour, 0)
	col19 := randDateTimeAround(col18, 0, 6*time.Hour)
	col20 := randDateTimeAround(col18, -6*time.Hour, 6*time.Hour)
	col21 := randDateTimeAround(col19, 0, 12*time.Hour)
	col22 := randInt64(0, 9)
	col23 := randDecimalStr(6, 0, 9999999)
	col24 := randDecimalStr(6, 0, 9999999)
	col25 := randInt64(0, 9)
	col26 := randChoice([]string{"AP", "BP", "CP", "NA"})
	col27 := randInt64(0, 9)
	col28 := randChoice([]string{"-1", "0", "01", "05"})
	col29 := randInt64(0, 9)
	col30 := randVarchar("c29", 50)
	col31 := randVarchar("c30", 16)
	col32 := now
	col33 := randVarchar("c32", 124)
	col34 := randVarchar("c33", 40)
	col35 := randDecimalStr(6, 0, 1000000)
	col36 := randInt64(0, 9)
	col37 := randDateTimeAround(now, -2*time.Hour, 2*time.Hour)
	col38 := randVarchar("c37", 50)
	col39 := randVarchar("c38", 20)
	col40 := randVarchar("c39", 50)
	col41 := randInt64(0, 9)
	col42 := randInt64(-99, 99)
	col43 := randVarchar("c42", 100)
	col44 := randVarchar("c43", 256)
	col45 := randChoice([]string{"0", "01", "02", "10"})
	col46 := randVarchar("c45", 64)
	col47 := randVarchar("c46", 128)
	col48 := randVarchar("c47", 128)
	col49 := randVarchar("c48", 128)
	col50 := randDecimalStr(6, 0, 999999)
	col51 := randChoice([]string{"NA", "android", "ios", "h5"})
	col52 := randDateTimeAround(now, -72*time.Hour, 0)
	col53 := randDecimalStr(6, 0, 999999)
	col54 := randDecimalStr(6, 0, 999999)

	cols := []string{
		"ID", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9",
		"col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19",
		"col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27", "col28", "col29",
		"col30", "col31", "col32", "col33", "col34", "col35", "col36", "col37", "col38", "col39",
		"col40", "col41", "col42", "col43", "col44", "col45", "col46", "col47", "col48", "col49",
		"col50", "col51", "col52", "col53",
	}

	values := []string{
		col1,
		quote(col2),
		quote(col3),
		quote(col4),
		quote(col5),
		quote(col6),
		quote(col7),
		quote(col8),
		fmt.Sprintf("%d", col9),
		quote(col10),
		quote(col11),
		quote(col12),
		col13, // decimal
		col14,
		col15,
		col16,
		col17,
		quote(formatDateTime(col18)),
		quote(formatDateTime(col19)),
		quote(formatDateTime(col20)),
		quote(formatDateTime(col21)),
		fmt.Sprintf("%d", col22),
		col23,
		col24,
		fmt.Sprintf("%d", col25),
		quote(col26),
		fmt.Sprintf("%d", col27),
		quote(col28),
		fmt.Sprintf("%d", col29),
		quote(col30),
		quote(col31),
		quote(formatDateTime(col32)),
		quote(col33),
		quote(col34),
		col35,
		fmt.Sprintf("%d", col36),
		quote(formatDateTime(col37)),
		quote(col38),
		quote(col39),
		quote(col40),
		fmt.Sprintf("%d", col41),
		fmt.Sprintf("%d", col42),
		quote(col43),
		quote(col44),
		quote(col45),
		quote(col46),
		quote(col47),
		quote(col48),
		quote(col49),
		col50,
		quote(col51),
		quote(formatDateTime(col52)),
		col53,
		col54,
	}

	sql := fmt.Sprintf("INSERT INTO dc_%d (%s) VALUES (%s);",
		tableN,
		strings.Join(cols, ","),
		strings.Join(values, ","),
	)
	return sql
}

func BuildInsertForT(tableN int) string {
	now := time.Now()
	col1 := randInt64(1, 9999999999) // Assuming a reasonable range for example
	col2 := randVarchar("col1", 100)
	col3 := randVarchar("col2", 100)
	col4 := formatDate(now.AddDate(0, 0, rand.Intn(30)-15)) // Random date within +/- 15 days
	col5 := formatDate(now.AddDate(0, 0, rand.Intn(30)-15))
	col6 := randInt64(0, 1000)
	col7 := formatDate(now.AddDate(0, 0, rand.Intn(30)-15))

	cols := []string{
		"id",
		"col1",
		"col2",
		"col3",
		"col4",
		"col5",
		"col6",
	}

	values := []string{
		fmt.Sprintf("%d", col1), // ID is bigint, so no quotes
		quote(col2),
		quote(col3),
		quote(col4),
		quote(col5),
		fmt.Sprintf("%d", col6), // col5 is bigint, so no quotes
		quote(col7),
	}

	sql := fmt.Sprintf("INSERT INTO t_%d (%s) VALUES (%s);",
		tableN,
		strings.Join(cols, ","),
		strings.Join(values, ","),
	)
	return sql
}

func BuildInsertForOrders(tableN int) string {
	now := time.Now()
	col1 := randInt64(1, 9999999999)
	col2 := randVarchar("col1", 20)
	col3 := randVarchar("col2", 50)
	col4 := formatDate(now.AddDate(0, 0, rand.Intn(30)-15)) // Random date within +/- 15 days
	col5 := randVarchar("col4", 20)
	col6 := randVarchar("col5", 10)
	col7 := randVarchar("col6", 50)
	col8 := randVarchar("col7", 6)
	col9 := randDecimalStr(6, 0, 999999999999)
	col10 := randDecimalStr(6, 0, 999999999999)
	col11 := randDecimalStr(0, 0, 999999999999)
	col12 := randDecimalStr(6, 0, 999999999999)
	col13 := randDecimalStr(0, 0, 999999999999)
	col14 := "0"
	col15 := "0"
	col16 := "0"
	col17 := now
	col18 := now
	col19 := randVarchar("col19", 100)
	col20 := randVarchar("col20", 2)
	col21 := "0"
	col22 := randInt64(0, 127)
	col23 := randVarchar("col22", 20)
	col24 := "0"
	col25 := "1"
	col26 := "0"
	col27 := "0"
	col28 := "-1"
	col29 := "0"
	col30 := "0"

	cols := []string{
		"ID", "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9",
		"col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18",
		"col19", "col20", "col21", "col22", "col23", "col24", "col25", "col26", "col27",
		"col28", "col29",
	}

	values := []string{
		fmt.Sprintf("%d", col1), // ID is bigint, so no quotes
		quote(col2),
		quote(col3),
		quote(col4),
		quote(col5),
		quote(col6),
		quote(col7),
		quote(col8),
		col9,
		col10,
		col11,
		col12,
		col13,
		col14, // Not NULL, so using default '0'
		col15, // Not NULL, so using default '0'
		col16, // Not NULL, so using default '0'
		quote(col17.Format("2006-01-02 15:04:05")),
		quote(col18.Format("2006-01-02 15:04:05")),
		quote(col19),
		quote(col20),
		col21, // Not NULL, so using default '0'
		fmt.Sprintf("%d", col22),
		quote(col23),
		col24, // Not NULL, so using default '0'
		col25, // Not NULL, so using default '1'
		col26, // Not NULL, so using default '0'
		col27, // Not NULL, so using default '0'
		col28, // Not NULL, so using default '-1'
		col29, // Not NULL, so using default '0'
		col30, // Using default '0'
	}

	sql := fmt.Sprintf("INSERT INTO orders_%d (%s) VALUES (%s);",
		tableN,
		strings.Join(cols, ","),
		strings.Join(values, ","),
	)

	return sql
}
