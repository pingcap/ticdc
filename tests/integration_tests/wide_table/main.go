// Copyright 2025 PingCAP, Inc.
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
	"database/sql"
	"flag"
	"fmt"
	"strings"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/tests/integration_tests/util"
)

const (
	schemaName      = "wide_table"
	tableName       = "wide_rows"
	wideColumnCount = 200
	seedRowCount    = 4
	lateRowID       = 101
	replacementID   = 5000

	columnPayloadBytes     = 96
	chunkTextBytes         = 16384
	largeBinaryBytes       = 16384
	longValueBytes         = 90000
	defaultMinRowWidthByte = 80000
)

var (
	rowWidthSQL      string
	rowWidthSQLOnce  sync.Once
	minRowWidthBytes = defaultMinRowWidthByte
)

type options struct {
	sourceHost     string
	sourcePort     int
	sourceUser     string
	sourcePassword string
	sourceDatabase string
	minRowWidth    int
}

func main() {
	opts := parseOptions()
	minRowWidthBytes = opts.minRowWidth

	sourceConfig := util.DBConfig{
		Host:     opts.sourceHost,
		User:     opts.sourceUser,
		Password: opts.sourcePassword,
		Name:     opts.sourceDatabase,
		Port:     opts.sourcePort,
	}

	sourceDB, err := util.CreateDB(sourceConfig)
	if err != nil {
		log.S().Fatal(err)
	}
	defer func() {
		if closeErr := util.CloseDB(sourceDB); closeErr != nil {
			log.S().Errorf("failed to close source database: %s\n", closeErr)
		}
	}()

	runWideTableCase(sourceDB)
}

func runWideTableCase(db *sql.DB) {
	util.MustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS %s", schemaName))
	util.MustExec(db, fmt.Sprintf("CREATE DATABASE %s", schemaName))
	util.MustExec(db, buildCreateTableSQL())

	for id := 1; id <= seedRowCount; id++ {
		insertWideRow(db, id, "seed")
		updateWideRow(db, id)
		assertRowWidth(db, id)
	}

	insertWideRow(db, lateRowID, "late")
	updateWideRow(db, lateRowID)
	assertRowWidth(db, lateRowID)

	alterWideTable(db)

	util.MustExec(db, fmt.Sprintf("UPDATE %s.%s SET c050 = CONCAT(c050, '-patched'), c150 = CONCAT(c150, '-patched') WHERE MOD(id, 2) = 0", schemaName, tableName))
	util.MustExec(db, fmt.Sprintf("DELETE FROM %s.%s WHERE id = ?", schemaName, tableName), 2)

	insertWideRow(db, replacementID, "replacement")
	updateWideRow(db, replacementID)
	assertRowWidth(db, replacementID)

	util.MustExec(db, fmt.Sprintf("DROP TABLE IF EXISTS %s.finish_mark", schemaName))
	util.MustExec(db, fmt.Sprintf("CREATE TABLE %s.finish_mark(id INT PRIMARY KEY)", schemaName))
	util.MustExec(db, fmt.Sprintf("INSERT INTO %s.finish_mark VALUES (1)", schemaName))
}

func buildCreateTableSQL() string {
	columns := make([]string, 0, wideColumnCount+4)
	columns = append(columns, "id BIGINT PRIMARY KEY")
	for i := 1; i <= wideColumnCount; i++ {
		columns = append(columns, fmt.Sprintf("c%03d VARCHAR(256) NOT NULL DEFAULT ''", i))
	}
	columns = append(columns,
		"chunk_text MEDIUMTEXT NOT NULL",
		"large_binary MEDIUMBLOB NOT NULL",
		"json_data JSON",
		"long_value MEDIUMBLOB NOT NULL",
	)
	return fmt.Sprintf("CREATE TABLE %s.%s (%s)", schemaName, tableName, strings.Join(columns, ", "))
}

func insertWideRow(db *sql.DB, id int, tag string) {
	chunk := buildChunkValue(tag, id)
	payload := buildBinaryValue(tag, id)
	jsonValue := fmt.Sprintf("{\"row\":%d,\"stage\":\"%s\"}", id, tag)
	longValue := buildLongValue(tag, id)
	sql := fmt.Sprintf("INSERT INTO %s.%s(id, chunk_text, large_binary, json_data, long_value) VALUES (?, ?, ?, CAST(? AS JSON), ?)", schemaName, tableName)
	util.MustExec(db, sql, id, chunk, payload, jsonValue, longValue)
}

func updateWideRow(db *sql.DB, id int) {
	setParts := make([]string, 0, wideColumnCount+4)
	args := make([]interface{}, 0, wideColumnCount+5)
	for col := 1; col <= wideColumnCount; col++ {
		setParts = append(setParts, fmt.Sprintf("c%03d = ?", col))
		args = append(args, buildColumnValue(id, col))
	}
	setParts = append(setParts, "chunk_text = ?")
	args = append(args, buildChunkValue("update", id))
	setParts = append(setParts, "large_binary = ?")
	args = append(args, buildBinaryValue("update", id))
	setParts = append(setParts, "json_data = CAST(? AS JSON)")
	args = append(args, fmt.Sprintf("{\"row\":%d,\"stage\":\"wide-update\",\"columns\":%d}", id, wideColumnCount))
	setParts = append(setParts, "long_value = ?")
	args = append(args, buildLongValue("update", id))
	args = append(args, id)
	sql := fmt.Sprintf("UPDATE %s.%s SET %s WHERE id = ?", schemaName, tableName, strings.Join(setParts, ", "))
	util.MustExec(db, sql, args...)
}

func alterWideTable(db *sql.DB) {
	util.MustExec(db, fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN tail_marker VARCHAR(64) NOT NULL DEFAULT ''", schemaName, tableName))
	util.MustExec(db, fmt.Sprintf("UPDATE %s.%s SET tail_marker = CONCAT('tail-', id)", schemaName, tableName))
	util.MustExec(db, fmt.Sprintf("UPDATE %s.%s SET chunk_text = CONCAT(chunk_text, '-tail'), c120 = CONCAT(c120, '-tail') WHERE id >= ?", schemaName, tableName), lateRowID)
}

func buildColumnValue(id, col int) string {
	return repeatToLength(fmt.Sprintf("row-%d-col-%03d-", id, col), columnPayloadBytes)
}

func buildChunkValue(tag string, id int) string {
	return repeatToLength(fmt.Sprintf("chunk-%s-%d|", tag, id), chunkTextBytes)
}

func buildBinaryValue(tag string, id int) string {
	return repeatToLength(fmt.Sprintf("bin-%s-%d", tag, id), largeBinaryBytes)
}

func buildLongValue(tag string, id int) string {
	return repeatToLength(fmt.Sprintf("long-%s-%d|", tag, id), longValueBytes)
}

func repeatToLength(seed string, target int) string {
	if target <= 0 {
		return ""
	}
	if seed == "" {
		seed = "x"
	}
	var builder strings.Builder
	builder.Grow(target)
	for builder.Len() < target {
		remaining := target - builder.Len()
		if remaining >= len(seed) {
			builder.WriteString(seed)
		} else {
			builder.WriteString(seed[:remaining])
		}
	}
	return builder.String()
}

func assertRowWidth(db *sql.DB, id int) {
	query := buildRowWidthSQL()
	row := db.QueryRow(query, id)
	var width int64
	if err := row.Scan(&width); err != nil {
		log.S().Fatalf("failed to scan row width: %v", err)
	}
	if width < int64(minRowWidthBytes) {
		log.S().Fatalf("row %d width %d bytes is smaller than expected %d bytes", id, width, minRowWidthBytes)
	}
	log.S().Infof("row %d width %d bytes", id, width)
}

func buildRowWidthSQL() string {
	rowWidthSQLOnce.Do(func() {
		parts := make([]string, 0, wideColumnCount+4)
		for i := 1; i <= wideColumnCount; i++ {
			parts = append(parts, fmt.Sprintf("COALESCE(OCTET_LENGTH(c%03d), 0)", i))
		}
		parts = append(parts,
			"COALESCE(OCTET_LENGTH(chunk_text), 0)",
			"COALESCE(OCTET_LENGTH(large_binary), 0)",
			"COALESCE(OCTET_LENGTH(json_data), 0)",
			"COALESCE(OCTET_LENGTH(long_value), 0)",
		)
		rowWidthSQL = fmt.Sprintf("SELECT (%s) AS width FROM %s.%s WHERE id = ?", strings.Join(parts, " + "), schemaName, tableName)
	})
	return rowWidthSQL
}

func parseOptions() options {
	opts := options{}
	flag.StringVar(&opts.sourceHost, "source-host", "127.0.0.1", "TiDB host for the upstream SQL endpoint")
	flag.IntVar(&opts.sourcePort, "source-port", 4000, "TiDB SQL port for upstream")
	flag.StringVar(&opts.sourceUser, "source-user", "root", "TiDB user name")
	flag.StringVar(&opts.sourcePassword, "source-password", "", "TiDB user password")
	flag.StringVar(&opts.sourceDatabase, "source-database", "test", "Default database to connect to")
	flag.IntVar(&opts.minRowWidth, "min-row-width", defaultMinRowWidthByte, "Minimum expected row width for validation")
	flag.Parse()
	return opts
}
