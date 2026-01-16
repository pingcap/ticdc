package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"
)

func bigTxnWorker(
	ctx context.Context,
	db *sql.DB,
	model *clusterModel,
	seed int64,
	cfg dmlConfig,
	activeWorkers *int32,
) {
	// bigTxnWorker periodically runs large insert transactions to stress:
	//   - large message paths for MQ sinks,
	//   - large commit and apply paths for MySQL sink.
	if !cfg.BigTxnEnabled || cfg.BigTxnInterval.Duration <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(seed))
	ticker := time.NewTicker(cfg.BigTxnInterval.Duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		// When the overall DML pool is heavily degraded, skip big transactions to help recovery.
		if atomic.LoadInt32(activeWorkers) <= 4 {
			continue
		}

		if len(model.splitTables) == 0 {
			continue
		}
		tbl := model.splitTables[rng.Intn(len(model.splitTables))]
		rows := cfg.BigTxnRowsMin
		if cfg.BigTxnRowsMax > cfg.BigTxnRowsMin {
			rows = cfg.BigTxnRowsMin + rng.Intn(cfg.BigTxnRowsMax-cfg.BigTxnRowsMin+1)
		}
		_ = runBigInsertTxn(ctx, db, tbl, rows)
	}
}

func runBigInsertTxn(ctx context.Context, db *sql.DB, tbl *table, rows int) error {
	// Build a single multi-row INSERT inside a transaction to create a "big txn" workload.
	tbl.mu.Lock()
	if !tbl.exists {
		tbl.mu.Unlock()
		return nil
	}
	schema := tbl.schema.clone()
	startID := tbl.nextID
	tbl.nextID += int64(rows)
	tbl.mu.Unlock()

	var cols []column
	for _, c := range schema.columns {
		if c.generated != "" {
			continue
		}
		cols = append(cols, c)
	}
	if len(cols) == 0 {
		return nil
	}

	colNames := make([]string, 0, len(cols))
	for _, c := range cols {
		colNames = append(colNames, c.name)
	}

	var valuesSQL strings.Builder
	var args []any
	for i := 0; i < rows; i++ {
		if i > 0 {
			valuesSQL.WriteString(",")
		}
		valuesSQL.WriteString("(")
		for j := range cols {
			if j > 0 {
				valuesSQL.WriteString(",")
			}
			valuesSQL.WriteString("?")
		}
		valuesSQL.WriteString(")")
		rowID := startID + int64(i)
		for _, c := range cols {
			args = append(args, buildRandomValue(rand.New(rand.NewSource(rowID)), tbl, c, rowID))
		}
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tbl.fqName(),
		backtickJoin(colNames),
		valuesSQL.String(),
	)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, stmt, args...); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func conflictWriter(
	ctx context.Context,
	db *sql.DB,
	model *clusterModel,
	seed int64,
	cfg dmlConfig,
	counters *dmlCounters,
) {
	// conflictWriter continuously upserts into a small key space to create write conflicts.
	// This targets row-level contention and duplicate key paths.
	if !cfg.KeyConflictEnabled || cfg.KeyConflictKeyspace <= 0 {
		return
	}
	rng := rand.New(rand.NewSource(seed))
	targetTables := collectConflictTables(model)
	if len(targetTables) == 0 {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		tbl := targetTables[rng.Intn(len(targetTables))]
		tbl.mu.Lock()
		exists := tbl.exists
		tbl.mu.Unlock()
		if !exists {
			_ = sleepWithContext(ctx, 200*time.Millisecond)
			continue
		}

		key := rng.Intn(cfg.KeyConflictKeyspace) + 1
		stmt := fmt.Sprintf("INSERT INTO %s (`id`,`a`,`b`,`c`,`d`,`e`,`bin`) VALUES (?,?,?,?,?,?,?) "+
			"ON DUPLICATE KEY UPDATE `a`=VALUES(`a`),`b`=VALUES(`b`),`c`=VALUES(`c`)",
			tbl.fqName(),
		)
		args := []any{
			int64(key),
			int32(rng.Intn(1_000_000)),
			randASCII(rng, 16),
			deterministicDecimal(int64(key)),
			deterministicTime(int64(key)),
			fmt.Sprintf("{\"k\":%d}", key),
			[]byte(fmt.Sprintf("%064x", key)),
		}
		_, err := db.ExecContext(ctx, stmt, args...)
		counters.record(err)
		_ = sleepWithContext(ctx, 20*time.Millisecond)
	}
}

func collectConflictTables(model *clusterModel) []*table {
	// Pick a stable target table for conflict writes to keep the workload deterministic.
	var out []*table
	for _, t := range model.churnTables {
		// Use a single, deterministic churn family (t10) which is guaranteed to have `id` PK in initial schema.
		if t.name == "t10" {
			out = append(out, t)
		}
	}
	return out
}
