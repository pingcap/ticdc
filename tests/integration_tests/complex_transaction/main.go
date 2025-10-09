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
	"context"
	"database/sql"
	"flag"
	"math/rand"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	dsn         = flag.String("dsn", "", "database DSN")
	concurrency = flag.Int("concurrency", 20, "number of concurrent workers")
	totalTxns   = flag.Int64("total-txns", 200000, "total number of transactions to execute")
	numProducts = flag.Int("products", 200, "number of products")
	numUsers    = flag.Int("users", 2000, "number of users")
)

const (
	initialBalance = 10000
	initialStock   = 1000
)

func main() {
	flag.Parse()

	if *dsn == "" {
		log.Fatal("DSN must be provided")
	}

	rand.Seed(time.Now().UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	db := openDB(ctx, *dsn)
	defer db.Close()

	log.Info("Starting complex transaction workload",
		zap.Int("concurrency", *concurrency),
		zap.Int64("totalTxns", *totalTxns),
		zap.Int("products", *numProducts),
		zap.Int("users", *numUsers))

	// Initialize tables and data
	initializeTables(ctx, db)

	// Run workload
	startTime := time.Now()
	runWorkload(ctx, db)
	elapsed := time.Since(startTime)

	log.Info("Workload completed",
		zap.Duration("elapsed", elapsed),
		zap.Float64("tps", float64(*totalTxns)/elapsed.Seconds()))

	log.Info("Workload generation completed successfully")
}

func openDB(ctx context.Context, dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal("Failed to open database", zap.String("dsn", dsn), zap.Error(err))
	}

	db.SetMaxOpenConns(*concurrency * 2)
	db.SetMaxIdleConns(*concurrency)
	db.SetConnMaxLifetime(10 * time.Minute)

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		log.Fatal("Failed to ping database", zap.String("dsn", dsn), zap.Error(err))
	}

	log.Info("Database connection established", zap.String("dsn", dsn))
	return db
}

func runWorkload(ctx context.Context, db *sql.DB) {
	var (
		txnCounter   int64
		successCount int64
		failCount    int64
	)

	g, ctx := errgroup.WithContext(ctx)

	for i := 0; i < *concurrency; i++ {
		workerID := i
		g.Go(func() error {
			executor := NewWorkloadExecutor(db, workerID)

			for {
				current := atomic.LoadInt64(&txnCounter)
				if current >= *totalTxns {
					break
				}

				// Select random transaction type
				txType := selectTransactionType()

				err := executor.ExecuteTransaction(ctx, txType)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					log.Info("Transaction failed",
						zap.Int("worker", workerID),
						zap.String("type", txType),
						zap.Error(err))
					// Continue on error, don't fail the whole test
					continue
				}

				atomic.AddInt64(&successCount, 1)
				count := atomic.AddInt64(&txnCounter, 1)

				// Periodic logging
				if count%1000 == 0 {
					log.Info("Progress",
						zap.Int64("completed", count),
						zap.Int64("total", *totalTxns),
						zap.Int64("success", atomic.LoadInt64(&successCount)),
						zap.Int64("failed", atomic.LoadInt64(&failCount)))
				}
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatal("Workload execution failed", zap.Error(err))
	}

	log.Info("Workload statistics",
		zap.Int64("total", atomic.LoadInt64(&txnCounter)),
		zap.Int64("success", atomic.LoadInt64(&successCount)),
		zap.Int64("failed", atomic.LoadInt64(&failCount)))
}

// Transaction type weights (total should be 100)
var transactionTypes = []struct {
	name   string
	weight int
}{
	{"create_order", 25},       // 25% E-commerce: create order
	{"cancel_order", 10},       // 10% E-commerce: cancel order
	{"bank_transfer", 22},      // 22% Banking: transfer money
	{"bank_multi_transfer", 5}, // 5%  Banking: complex multi-transfer
	{"social_create_post", 15}, // 15% Social: create post
	{"social_interact", 12},    // 12% Social: like/comment
	{"inventory_adjust", 5},    // 5%  E-commerce: inventory adjustment
	{"user_activity", 4},       // 4%  Mixed: user activity update
	{"complex_mixed", 2},       // 2%  Complex mixed operations (reduced to minimize deadlocks)
}

func selectTransactionType() string {
	r := rand.Intn(100)
	cumulative := 0

	for _, tt := range transactionTypes {
		cumulative += tt.weight
		if r < cumulative {
			return tt.name
		}
	}

	return transactionTypes[0].name
}
