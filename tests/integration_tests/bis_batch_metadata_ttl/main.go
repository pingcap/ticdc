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

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	schemaName = "bulk_ingestion"
	tableName  = "bis_batch_metadata"
)

var (
	dsn           = flag.String("dsn", "", "upstream TiDB DSN")
	workers       = flag.Int("workers", 32, "number of concurrent DML workers")
	operations    = flag.Int("operations", 80000, "total DML operations to run")
	seedRows      = flag.Int("seed-rows", 20000, "initial rows loaded before concurrent DML")
	metadataBytes = flag.Int("metadata-bytes", 1024, "metadata payload size in bytes")
)

type workload struct {
	db       *sql.DB
	maxID    atomic.Int64
	progress atomic.Int64
}

func main() {
	flag.Parse()
	if *dsn == "" {
		log.Fatal("missing required -dsn")
	}
	if *workers <= 0 || *operations <= 0 || *seedRows <= 0 || *metadataBytes <= 0 {
		log.Fatal("workers, operations, seed-rows, and metadata-bytes must be positive")
	}

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatalf("open db failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(*workers + 8)
	db.SetMaxIdleConns(*workers + 8)
	db.SetConnMaxLifetime(3 * time.Minute)

	w := &workload{db: db}
	w.maxID.Store(int64(*seedRows))

	w.prepareSchema()
	w.loadSeedRows()
	w.runDML()
	w.createFinishMark()
}

func (w *workload) prepareSchema() {
	mustExec(w.db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", schemaName))
	mustExec(w.db, fmt.Sprintf("CREATE DATABASE `%s`", schemaName))
	mustExec(w.db, fmt.Sprintf(`
CREATE TABLE %s.%s (
  id varchar(36) NOT NULL,
  user_id bigint(20) NOT NULL,
  entity_set_id varchar(255) NOT NULL,
  status smallint(6) DEFAULT NULL,
  metadata mediumblob DEFAULT NULL,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  client_id varchar(255) DEFAULT NULL,
  aux_data varchar(3072) DEFAULT NULL,
  callback_job_metadata varchar(3072) DEFAULT NULL,
  bmd_image_pipeline_id varchar(255) DEFAULT NULL,
  bmd_client_name varchar(255) DEFAULT NULL,
  skip_pin_operations tinyint(1) DEFAULT NULL,
  bmd_start_timestamp timestamp NULL DEFAULT NULL,
  bmd_end_timestamp timestamp NULL DEFAULT NULL,
  pinshot_start_timestamp timestamp NULL DEFAULT NULL,
  pinshot_end_timestamp timestamp NULL DEFAULT NULL,
  KEY idx_uid_esetid (user_id, entity_set_id),
  PRIMARY KEY (id) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
/*T! SHARD_ROW_ID_BITS=4 */
TTL=%s + INTERVAL 1 HOUR
TTL_ENABLE='ON'
TTL_JOB_INTERVAL='1h'`, schemaName, tableName, "`updated_at`"))
}

func (w *workload) loadSeedRows() {
	log.Printf("loading %d seed rows", *seedRows)
	stmt, err := w.db.Prepare(fmt.Sprintf(`
INSERT INTO %s.%s (
  created_at, updated_at, id, user_id, entity_set_id, status, metadata,
  client_id, aux_data, callback_job_metadata, bmd_image_pipeline_id, bmd_client_name
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, schemaName, tableName))
	if err != nil {
		log.Fatalf("prepare seed insert failed: %v", err)
	}
	defer stmt.Close()

	for id := int64(1); id <= int64(*seedRows); id++ {
		createdAt, updatedAt := seedTimestamps(id)
		_, err := stmt.Exec(
			createdAt,
			updatedAt,
			rowID(id),
			userID(id),
			entitySetID(id),
			status(id),
			payload(id, *metadataBytes),
			clientID(id),
			auxData(id),
			callbackJobMetadata(id),
			pipelineID(id),
			clientName(id),
		)
		if err != nil {
			log.Fatalf("seed insert failed, id=%d: %v", id, err)
		}
	}
}

func (w *workload) runDML() {
	log.Printf("running %d operations with %d workers", *operations, *workers)
	var nextOp atomic.Int64
	var wg sync.WaitGroup
	for workerID := 0; workerID < *workers; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			w.runWorker(workerID, &nextOp)
		}(workerID)
	}

	ticker := time.NewTicker(5 * time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		wg.Wait()
	}()

	for {
		select {
		case <-ticker.C:
			log.Printf("completed %d/%d operations", w.progress.Load(), *operations)
		case <-done:
			ticker.Stop()
			log.Printf("completed %d operations", w.progress.Load())
			return
		}
	}
}

func (w *workload) runWorker(workerID int, nextOp *atomic.Int64) {
	statusStmt, metadataStmt, ownerStmt, insertStmt := w.prepareDMLStatements()
	defer statusStmt.Close()
	defer metadataStmt.Close()
	defer ownerStmt.Close()
	defer insertStmt.Close()

	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
	for {
		op := nextOp.Add(1)
		if op > int64(*operations) {
			return
		}

		targetID := rnd.Int63n(w.maxID.Load()) + 1
		switch roll := rnd.Intn(100); {
		case roll < 45:
			_, err := statusStmt.Exec(opTimestamp(rnd), status(op), rowID(targetID))
			mustDML(err, "update status")
		case roll < 75:
			_, err := metadataStmt.Exec(payload(op, *metadataBytes), opTimestamp(rnd), status(op), rowID(targetID))
			mustDML(err, "update metadata")
		case roll < 90:
			_, err := ownerStmt.Exec(payload(op, *metadataBytes), opTimestamp(rnd), userID(op), entitySetID(op), status(op), rowID(targetID))
			mustDML(err, "update owner fields")
		default:
			id := w.maxID.Add(1)
			now := opTimestamp(rnd)
			_, err := insertStmt.Exec(
				now,
				now,
				rowID(id),
				userID(id),
				entitySetID(id),
				status(id),
				payload(id, *metadataBytes),
				clientID(id),
				auxData(id),
				callbackJobMetadata(id),
				pipelineID(id),
				clientName(id),
			)
			mustDML(err, "insert row")
		}

		w.progress.Add(1)
	}
}

func (w *workload) prepareDMLStatements() (*sql.Stmt, *sql.Stmt, *sql.Stmt, *sql.Stmt) {
	statusStmt, err := w.db.Prepare(fmt.Sprintf(`
UPDATE %s.%s
SET updated_at = ?, status = ?
WHERE id = ?`, schemaName, tableName))
	if err != nil {
		log.Fatalf("prepare status update failed: %v", err)
	}

	metadataStmt, err := w.db.Prepare(fmt.Sprintf(`
UPDATE %s.%s
SET metadata = ?, updated_at = ?, status = ?
WHERE id = ?`, schemaName, tableName))
	if err != nil {
		log.Fatalf("prepare metadata update failed: %v", err)
	}

	ownerStmt, err := w.db.Prepare(fmt.Sprintf(`
UPDATE %s.%s
SET metadata = ?, updated_at = ?, user_id = ?, entity_set_id = ?, status = ?
WHERE id = ?`, schemaName, tableName))
	if err != nil {
		log.Fatalf("prepare owner update failed: %v", err)
	}

	insertStmt, err := w.db.Prepare(fmt.Sprintf(`
INSERT INTO %s.%s (
  created_at, updated_at, id, user_id, entity_set_id, status, metadata,
  client_id, aux_data, callback_job_metadata, bmd_image_pipeline_id, bmd_client_name
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, schemaName, tableName))
	if err != nil {
		log.Fatalf("prepare insert failed: %v", err)
	}
	return statusStmt, metadataStmt, ownerStmt, insertStmt
}

func (w *workload) createFinishMark() {
	mustExec(w.db, fmt.Sprintf("DROP TABLE IF EXISTS %s.finish_mark", schemaName))
	mustExec(w.db, fmt.Sprintf("CREATE TABLE %s.finish_mark(id INT PRIMARY KEY)", schemaName))
	mustExec(w.db, fmt.Sprintf("INSERT INTO %s.finish_mark VALUES (1)", schemaName))
}

func mustExec(db *sql.DB, query string, args ...interface{}) {
	_, err := db.Exec(query, args...)
	if err != nil {
		log.Fatalf("exec failed, sql=%s, args=%v, err=%v", query, args, err)
	}
}

func mustDML(err error, name string) {
	if err != nil {
		log.Fatalf("%s failed: %v", name, err)
	}
}

func seedTimestamps(id int64) (time.Time, time.Time) {
	now := time.Now().UTC()
	if id%10 == 0 {
		expired := now.Add(-2 * time.Hour)
		return expired, expired
	}
	return now, now
}

func opTimestamp(rnd *rand.Rand) time.Time {
	now := time.Now().UTC()
	if rnd.Intn(100) < 5 {
		return now.Add(-2 * time.Hour)
	}
	return now
}

func rowID(id int64) string {
	return fmt.Sprintf("%032x", id)
}

func userID(id int64) int64 {
	return 100000000 + id%1000000
}

func entitySetID(id int64) string {
	return fmt.Sprintf("entity-set-%06d", id%10000)
}

func status(id int64) int {
	return int(id % 10)
}

func clientID(id int64) string {
	return fmt.Sprintf("client-%04d", id%1000)
}

func auxData(id int64) string {
	return boundedString(fmt.Sprintf("aux-data-%d-", id), 512)
}

func callbackJobMetadata(id int64) string {
	return boundedString(fmt.Sprintf("callback-job-metadata-%d-", id), 512)
}

func pipelineID(id int64) string {
	return fmt.Sprintf("pipeline-%04d", id%1000)
}

func clientName(id int64) string {
	return fmt.Sprintf("client-name-%04d", id%1000)
}

func payload(seed int64, size int) []byte {
	return []byte(boundedString(fmt.Sprintf("metadata-%d-", seed), size))
}

func boundedString(seed string, size int) string {
	if size <= 0 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(size)
	for builder.Len() < size {
		remaining := size - builder.Len()
		if remaining >= len(seed) {
			builder.WriteString(seed)
		} else {
			builder.WriteString(seed[:remaining])
		}
	}
	return builder.String()
}
