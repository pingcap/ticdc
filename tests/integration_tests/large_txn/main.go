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
	"log"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn  = flag.String("dsn", "", "MySQL DSN")
	rows = flag.Int("rows", 1000, "Number of rows per transaction")
	txns = flag.Int("txns", 10, "Number of transactions per type")
)

func main() {
	flag.Parse()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	createTable(db)

	// 1. Large Insert
	for i := 0; i < *txns; i++ {
		runLargeInsert(db, *rows, i)
	}

	// 2. Large Update
	for i := 0; i < *txns; i++ {
		runLargeUpdate(db, i)
	}

	// 3. Large Delete
	for i := 0; i < *txns; i++ {
		runLargeDelete(db, i)
	}

	// 4. Cleanup and recreate for final check
	runCleanup(db)
	for i := 0; i < *txns; i++ {
		runLargeInsert(db, *rows, i)
	}
}

func createTable(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS large_txn_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			batch_id INT,
			data LONGTEXT
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

func runLargeInsert(db *sql.DB, numRows int, batchID int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT INTO large_txn_table (batch_id, data) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	largeData := strings.Repeat("a", 1024) // 1KB data

	for i := 0; i < numRows; i++ {
		_, err := stmt.Exec(batchID, largeData)
		if err != nil {
			tx.Rollback()
			log.Fatalf("Failed to insert row: %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	log.Printf("Committed large insert transaction (batch %d, %d rows)", batchID, numRows)
}

func runLargeUpdate(db *sql.DB, batchID int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update all rows for this batch
	_, err = tx.Exec("UPDATE large_txn_table SET data = ? WHERE batch_id = ?", strings.Repeat("b", 1024), batchID)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to update rows: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	log.Printf("Committed large update transaction (batch %d)", batchID)
}

func runLargeDelete(db *sql.DB, batchID int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Delete all rows for this batch
	_, err = tx.Exec("DELETE FROM large_txn_table WHERE batch_id = ?", batchID)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to delete rows: %v", err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}
	log.Printf("Committed large delete transaction (batch %d)", batchID)
}

func runCleanup(db *sql.DB) {
	_, err := db.Exec("TRUNCATE TABLE large_txn_table")
	if err != nil {
		log.Fatalf("Failed to truncate table: %v", err)
	}
}
