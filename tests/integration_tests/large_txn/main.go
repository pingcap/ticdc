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
	"log"
	"strings"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	dsn         = flag.String("dsn", "", "MySQL DSN")
	rows        = flag.Int("rows", 1000, "Number of rows per transaction") // Reduced for faster testing
	txns        = flag.Int("txns", 10, "Number of transactions per type")  // Reduced for faster testing
	concurrency = flag.Int("concurrency", 1, "Number of concurrent workers")
)

// Data identifiers for verification
const (
	insertDataPrefix = "inserted_data_batch_"
	updateDataPrefix = "updated_data_batch_"
)

func main() {
	flag.Parse()

	if *dsn == "" {
		log.Fatal("DSN cannot be empty. Please provide a DSN using the -dsn flag.")
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(*concurrency * 2) // Allow more connections for concurrent operations
	db.SetMaxIdleConns(*concurrency)

	createTable(db)

	log.Printf("Starting interleaved operations with %d transactions, %d rows per txn, %d concurrency", *txns, *rows, *concurrency)

	var wg sync.WaitGroup
	batchSize := *txns / *concurrency
	if *txns%*concurrency != 0 {
		batchSize++ // Ensure all transactions are covered
	}

	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startBatch := workerID * batchSize
			endBatch := startBatch + batchSize
			if endBatch > *txns {
				endBatch = *txns
			}

			for batchID := startBatch; batchID < endBatch; batchID++ {
				if batchID >= *txns { // Safety check
					break
				}
				runInterleavedOperations(db, *rows, batchID)
			}
		}(i)
	}

	wg.Wait()

	log.Println("All interleaved operations completed successfully.")
	log.Println("Workload generation finished. CDC sync_diff_inspector will verify upstream-downstream consistency.")
}

func createTable(db *sql.DB) {
	log.Println("Creating table 'large_txn_table'...")
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS large_txn_table (
			id INT AUTO_INCREMENT PRIMARY KEY,
			batch_id INT NOT NULL,
			data VARCHAR(2048) NOT NULL, -- Increased size for data identifiers
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	log.Println("Table 'large_txn_table' created or already exists.")
}

func runInterleavedOperations(db *sql.DB, numRows int, batchID int) {
	// 1. Insert
	insertData := fmt.Sprintf("%s%d", insertDataPrefix, batchID)
	runLargeInsert(db, numRows, batchID, insertData)

	// 2. Update
	updateData := fmt.Sprintf("%s%d", updateDataPrefix, batchID)
	runLargeUpdate(db, batchID, updateData)

	// 3. Delete (for half of the batches to leave some data for verification)
	if batchID%2 == 0 { // Delete even batches
		runLargeDelete(db, batchID)
	}
}

func runLargeInsert(db *sql.DB, numRows int, batchID int, dataContent string) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction for insert (batch %d): %v", batchID, err)
	}

	stmt, err := tx.Prepare("INSERT INTO large_txn_table (batch_id, data) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to prepare insert statement (batch %d): %v", batchID, err)
	}
	defer stmt.Close()

	// Use a smaller data string for performance, but ensure it's unique enough
	smallData := strings.Repeat("x", 100) + dataContent

	for i := 0; i < numRows; i++ {
		_, err := stmt.Exec(batchID, smallData)
		if err != nil {
			tx.Rollback()
			log.Fatalf("Failed to insert row (batch %d, row %d): %v", batchID, i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit insert transaction (batch %d): %v", batchID, err)
	}
	log.Printf("Committed large insert transaction (batch %d, %d rows, data: %s)", batchID, numRows, dataContent)
}

func runLargeUpdate(db *sql.DB, batchID int, newDataContent string) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction for update (batch %d): %v", batchID, err)
	}

	// Use a smaller data string for performance, but ensure it's unique enough
	smallData := strings.Repeat("y", 100) + newDataContent

	_, err = tx.Exec("UPDATE large_txn_table SET data = ? WHERE batch_id = ?", smallData, batchID)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to update rows (batch %d): %v", batchID, err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit update transaction (batch %d): %v", batchID, err)
	}
	log.Printf("Committed large update transaction (batch %d, new data: %s)", batchID, newDataContent)
}

func runLargeDelete(db *sql.DB, batchID int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction for delete (batch %d): %v", batchID, err)
	}

	_, err = tx.Exec("DELETE FROM large_txn_table WHERE batch_id = ?", batchID)
	if err != nil {
		tx.Rollback()
		log.Fatalf("Failed to delete rows (batch %d): %v", batchID, err)
	}

	if err := tx.Commit(); err != nil {
		log.Fatalf("Failed to commit delete transaction (batch %d): %v", batchID, err)
	}
	log.Printf("Committed large delete transaction (batch %d)", batchID)
}
