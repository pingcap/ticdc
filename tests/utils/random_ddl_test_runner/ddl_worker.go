package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type ddlTrace struct {
	mu   sync.Mutex
	file *os.File
	log  *log.Logger
}

func newDDLTrace(workdir string) (*ddlTrace, error) {
	if err := os.MkdirAll(workdir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(workdir, "ddl_trace.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &ddlTrace{
		file: f,
		log:  log.New(f, "", log.LstdFlags|log.Lmicroseconds|log.LUTC),
	}, nil
}

func (t *ddlTrace) close() {
	if t == nil || t.file == nil {
		return
	}
	_ = t.file.Close()
}

func (t *ddlTrace) record(kind string, target string, sql string, err error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	status := "ok"
	msg := ""
	if err != nil {
		status = "err"
		msg = err.Error()
	}
	// Avoid printing raw SQL for DML here; DDL is expected ASCII-only.
	t.log.Printf("kind=%s target=%s status=%s sql=%q err=%q", kind, target, status, sql, msg)
}

func ddlWorker(
	ctx context.Context,
	db *sql.DB,
	model *clusterModel,
	seed int64,
	workerID int,
	activeWorkers *int32,
	selector *ddlSelector,
	trace *ddlTrace,
	logger *log.Logger,
) {
	// ddlWorker is a best-effort DDL submitter.
	//
	// Concurrency control:
	//   - Spawn MaxWorkers goroutines, but only workers with workerID < activeWorkers are "active".
	//   - healthAndAutotuneLoop adjusts activeWorkers based on checkpoint liveness.
	//
	// Correctness model:
	//   - Each DDL kind returns (sql, apply). apply updates the in-memory model and is invoked only
	//     when the DDL succeeds, so subsequent DML/DDL generation can track schema evolution.
	//   - DDL failures are expected under concurrency (e.g., conflicts, missing tables) and do not
	//     stop the worker.
	rng := rand.New(rand.NewSource(seed + int64(workerID)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if int32(workerID) >= atomic.LoadInt32(activeWorkers) {
			_ = sleepWithContext(ctx, 500*time.Millisecond)
			continue
		}

		kind := selector.pick(rng)
		var tbl *table
		if kind.name == "recover_table" {
			tbl = pickMissingTable(rng, model.churnTables)
		} else {
			tbl = model.pickTableForDomain(rng, kind.domain)
		}
		if tbl == nil || tbl.isMotif {
			_ = sleepWithContext(ctx, 100*time.Millisecond)
			continue
		}

		sqlText, apply := kind.gen(rng, tbl)
		if sqlText == "" || apply == nil {
			_ = sleepWithContext(ctx, 100*time.Millisecond)
			continue
		}

		start := time.Now()
		_, err := db.ExecContext(ctx, sqlText)
		if err == nil {
			apply()
			selector.record(kind.name)
		}
		if logger != nil {
			logger.Printf("ddl worker=%d kind=%s target=%s elapsed=%s err=%v",
				workerID, kind.name, tbl.fqName(), time.Since(start), err)
		}
		if trace != nil {
			trace.record(kind.name, tbl.fqName(), sqlText, err)
		}

		// Keep DDL submission rate bounded.
		_ = sleepWithContext(ctx, time.Second)
	}
}

func pickMissingTable(rng *rand.Rand, candidates []*table) *table {
	if len(candidates) == 0 {
		return nil
	}
	for i := 0; i < 10; i++ {
		t := candidates[rng.Intn(len(candidates))]
		t.mu.Lock()
		exists := t.exists
		t.mu.Unlock()
		if !exists {
			return t
		}
	}
	return nil
}

func ensureFileClosed(logger *log.Logger, t *ddlTrace) {
	if t == nil {
		return
	}
	t.close()
	if logger != nil {
		logger.Printf("ddl trace file closed")
	}
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp := filepath.Join(dir, fmt.Sprintf(".tmp-%d", time.Now().UnixNano()))
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
