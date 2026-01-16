package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

type dmlCounters struct {
	total          uint64
	success        uint64
	unknownTable   uint64
	unknownColumn  uint64
	duplicateEntry uint64
	otherError     uint64
}

func (c *dmlCounters) record(err error) {
	// DML errors are expected under concurrent DDL and are not fatal by themselves.
	// Counters are used by the health loop to infer a success rate for auto-tuning.
	atomic.AddUint64(&c.total, 1)
	if err == nil {
		atomic.AddUint64(&c.success, 1)
		return
	}
	var me *mysql.MySQLError
	if errors.As(err, &me) {
		switch me.Number {
		case 1146:
			atomic.AddUint64(&c.unknownTable, 1)
			return
		case 1054:
			atomic.AddUint64(&c.unknownColumn, 1)
			return
		case 1062:
			atomic.AddUint64(&c.duplicateEntry, 1)
			return
		}
	}
	atomic.AddUint64(&c.otherError, 1)
}

type dmlSnapshot struct {
	Total          uint64 `json:"total"`
	Success        uint64 `json:"success"`
	UnknownTable   uint64 `json:"unknown_table"`
	UnknownColumn  uint64 `json:"unknown_column"`
	DuplicateEntry uint64 `json:"duplicate_entry"`
	OtherError     uint64 `json:"other_error"`
}

func (c *dmlCounters) snapshot() dmlSnapshot {
	return dmlSnapshot{
		Total:          atomic.LoadUint64(&c.total),
		Success:        atomic.LoadUint64(&c.success),
		UnknownTable:   atomic.LoadUint64(&c.unknownTable),
		UnknownColumn:  atomic.LoadUint64(&c.unknownColumn),
		DuplicateEntry: atomic.LoadUint64(&c.duplicateEntry),
		OtherError:     atomic.LoadUint64(&c.otherError),
	}
}

func dmlWorker(
	ctx context.Context,
	db *sql.DB,
	model *clusterModel,
	seed int64,
	workerID int,
	activeWorkers *int32,
	cfg dmlConfig,
	counters *dmlCounters,
	motifStep *int32,
) {
	// dmlWorker generates best-effort DML against upstream.
	//
	// Concurrency control:
	//   - Spawn MaxWorkers goroutines, but only workerID < activeWorkers are "active".
	//   - healthAndAutotuneLoop adjusts activeWorkers based on checkpoint liveness.
	//
	// Schema handling:
	//   - DML generation reads the table schema under lock and then executes outside the lock.
	//   - DDL may race with DML, so unknown table/column errors are tracked and tolerated.
	rng := rand.New(rand.NewSource(seed + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if int32(workerID) >= atomic.LoadInt32(activeWorkers) {
			_ = sleepWithContext(ctx, 200*time.Millisecond)
			continue
		}

		tbl := model.pickTableForDML(rng, cfg.HotspotRatio)
		var (
			stmt string
			args []any
			err  error
		)

		if tbl.isMotif {
			stmt, args, err = buildMotifDML(rng, tbl, atomic.LoadInt32(motifStep))
		} else {
			stmt, args, err = buildGenericDML(rng, tbl)
		}
		if err != nil {
			// Internal generation error; keep the worker alive.
			counters.record(err)
			_ = sleepWithContext(ctx, 50*time.Millisecond)
			continue
		}
		if stmt == "" {
			_ = sleepWithContext(ctx, 20*time.Millisecond)
			continue
		}

		_, execErr := db.ExecContext(ctx, stmt, args...)
		counters.record(execErr)
	}
}

func buildGenericDML(rng *rand.Rand, tbl *table) (string, []any, error) {
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if !tbl.exists {
		return "", nil, nil
	}

	// Keep the overall mix stable:
	//   - INSERT is the dominant operation
	//   - UPDATE/DELETE provide mutation pressure
	//
	// For tables without a single-column primary key (keyless or composite PK), UPDATE/DELETE
	// fall back to bounded operations using LIMIT 1 to avoid requiring key materialization.
	switch rng.Intn(10) {
	case 0, 1:
		stmt, args, err := buildDeleteLocked(rng, tbl)
		if stmt != "" || err != nil {
			return stmt, args, err
		}
	case 2, 3:
		stmt, args, err := buildUpdateLocked(rng, tbl)
		if stmt != "" || err != nil {
			return stmt, args, err
		}
	default:
		// Fall through to INSERT.
	}
	return buildInsertLocked(rng, tbl, nil)
}

func buildMotifDML(rng *rand.Rand, tbl *table, step int32) (string, []any, error) {
	// Motif DML intentionally omits some columns to exercise default value drift
	// and schema evolution patterns coordinated by motif.go.
	tbl.mu.Lock()
	defer tbl.mu.Unlock()
	if !tbl.exists {
		return "", nil, nil
	}

	omit := map[string]struct{}{}
	for _, c := range tbl.schema.columns {
		if c.name == "site_code" {
			// Always omit site_code to exercise default drift.
			omit["site_code"] = struct{}{}
			break
		}
	}

	// Before PK is added, focus on inserts to create rows before/after default drift.
	if step < 3 {
		return buildInsertLocked(rng, tbl, omit)
	}

	// After PK is added (a, site_code), only update non-frozen rows inserted after default is unified.
	if tbl.motifUnifiedStart > 0 && tbl.nextID > tbl.motifUnifiedStart && rng.Intn(4) == 0 {
		return buildMotifUpdateAfterUnifiedLocked(rng, tbl)
	}
	return buildInsertLocked(rng, tbl, omit)
}

func buildInsertLocked(rng *rand.Rand, tbl *table, omitCols map[string]struct{}) (string, []any, error) {
	// Inserts are generated from a schema snapshot taken under table lock.
	schema := tbl.schema.clone()
	rowID := tbl.nextID
	tbl.nextID++

	var cols []column
	for _, c := range schema.columns {
		if c.generated != "" {
			continue
		}
		if omitCols != nil {
			if _, ok := omitCols[c.name]; ok {
				continue
			}
		}
		cols = append(cols, c)
	}
	if len(cols) == 0 {
		return "", nil, nil
	}

	colNames := make([]string, 0, len(cols))
	placeholders := make([]string, 0, len(cols))
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		colNames = append(colNames, c.name)
		placeholders = append(placeholders, "?")
		args = append(args, buildRandomValue(rng, tbl, c, rowID))
	}

	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tbl.fqName(),
		backtickJoin(colNames),
		strings.Join(placeholders, ","),
	)
	return stmt, args, nil
}

func buildUpdateLocked(rng *rand.Rand, tbl *table) (string, []any, error) {
	schema := tbl.schema.clone()

	var candidates []column
	for _, c := range schema.columns {
		if c.generated != "" {
			continue
		}
		if containsString(schema.primaryKey, c.name) {
			continue
		}
		candidates = append(candidates, c)
	}

	if len(schema.primaryKey) == 1 {
		// Single-column PK: do targeted updates by PK to keep the operation deterministic.
		pk := schema.primaryKey[0]
		if tbl.nextID <= 1 {
			return "", nil, nil
		}
		key := int64(rng.Intn(int(tbl.nextID-1)) + 1)
		if len(candidates) == 0 {
			// As a last resort, update the PK itself to still generate UPDATE traffic.
			// Pick a new key different from the old one.
			newKey := key + int64(rng.Intn(1024)+1)
			stmt := fmt.Sprintf("UPDATE %s SET `%s`=? WHERE `%s`=?",
				tbl.fqName(),
				pk,
				pk,
			)
			return stmt, []any{newKey, key}, nil
		}
		col := candidates[rng.Intn(len(candidates))]
		stmt := fmt.Sprintf("UPDATE %s SET `%s`=? WHERE `%s`=?",
			tbl.fqName(),
			col.name,
			pk,
		)
		args := []any{buildRandomValue(rng, tbl, col, key), key}
		return stmt, args, nil
	}

	// Keyless or composite PK tables: do a bounded update without relying on key materialization.
	if len(candidates) == 0 {
		for _, c := range schema.columns {
			if c.generated != "" {
				continue
			}
			candidates = append(candidates, c)
		}
	}
	if len(candidates) == 0 {
		return "", nil, nil
	}
	col := candidates[rng.Intn(len(candidates))]
	rowID := tbl.nextID
	if rowID <= 0 {
		rowID = 1
	}
	stmt := fmt.Sprintf("UPDATE %s SET `%s`=? LIMIT 1",
		tbl.fqName(),
		col.name,
	)
	return stmt, []any{buildRandomValue(rng, tbl, col, rowID)}, nil
}

func buildDeleteLocked(rng *rand.Rand, tbl *table) (string, []any, error) {
	schema := tbl.schema.clone()
	if len(schema.primaryKey) == 1 {
		pk := schema.primaryKey[0]
		if tbl.nextID <= 1 {
			return "", nil, nil
		}
		key := int64(rng.Intn(int(tbl.nextID-1)) + 1)
		stmt := fmt.Sprintf("DELETE FROM %s WHERE `%s`=?", tbl.fqName(), pk)
		return stmt, []any{key}, nil
	}
	// Keyless or composite PK tables: do a bounded delete without requiring keys.
	return fmt.Sprintf("DELETE FROM %s LIMIT 1", tbl.fqName()), nil, nil
}

func buildMotifUpdateAfterUnifiedLocked(rng *rand.Rand, tbl *table) (string, []any, error) {
	// This is only valid after PK evolution: PRIMARY KEY (a, site_code).
	schema := tbl.schema.clone()
	if len(schema.primaryKey) != 2 {
		return "", nil, nil
	}
	if tbl.nextID <= tbl.motifUnifiedStart {
		return "", nil, nil
	}
	a := int64(rng.Intn(int(tbl.nextID-tbl.motifUnifiedStart)) + int(tbl.motifUnifiedStart))

	stmt := fmt.Sprintf("UPDATE %s SET `b`=? WHERE `a`=? AND `site_code`=''",
		tbl.fqName(),
	)
	return stmt, []any{int32(rng.Intn(1_000_000)), a}, nil
}

func buildRandomValue(rng *rand.Rand, tbl *table, c column, rowID int64) any {
	// Keep payloads deterministic enough for triage, and keep SQL text ASCII-only by using placeholders.
	switch strings.ToUpper(c.typ.base) {
	case "BIGINT":
		if c.name == "id" {
			return rowID
		}
		return deterministicInt64(rowID)
	case "INT":
		if c.name == "a" && tbl.isMotif {
			return int32(rowID)
		}
		return int32(rng.Intn(1_000_000))
	case "VARCHAR":
		if c.name == "pad" {
			return strings.Repeat("x", 256)
		}
		return randASCII(rng, min(32, max(8, c.typ.varcharN/2)))
	case "DATETIME":
		return deterministicTime(rowID)
	case "DECIMAL":
		return deterministicDecimal(rowID)
	case "JSON":
		return fmt.Sprintf("{\"id\":%d,\"tbl\":\"%s\"}", rowID, tbl.name)
	case "VARBINARY":
		return []byte(fmt.Sprintf("%064x", rowID))
	default:
		return nil
	}
}

func randASCII(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
