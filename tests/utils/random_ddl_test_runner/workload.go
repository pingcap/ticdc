package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

func (r *runner) workload() error {
	// workload runs a bounded-duration stress workload against upstream only, while verifying:
	//   - the changefeed remains in "normal" state,
	//   - checkpoint continues advancing (with auto-tuning on stalls),
	//   - optional snapshot diffs at TiCDC syncpoints (MySQL sink),
	//   - optional capture failover recovery (multi-capture failover case).
	//
	// After the time budget is consumed, the runner inserts a finish mark row on upstream and
	// waits for it to appear on downstream as a "catch-up" barrier, then writes a final
	// diff_config.toml for sync_diff_inspector.
	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.Duration.Duration)
	defer cancel()

	if r.cfg.CDC.ChangefeedID == "" {
		return fmt.Errorf("cdc.changefeed_id is required for workload phase")
	}

	if err := os.MkdirAll(r.cfg.Workdir, 0o755); err != nil {
		return err
	}

	cfgSnapshot, _ := json.MarshalIndent(r.cfg, "", "  ")
	_ = os.WriteFile(filepath.Join(r.cfg.Workdir, "runner_config.snapshot.json"), cfgSnapshot, 0o644)

	r.logger.Printf("workload start: duration=%s seed=%d changefeed=%s sink=%s",
		r.cfg.Duration.Duration, r.cfg.Seed, r.cfg.CDC.ChangefeedID, r.cfg.SinkType)

	up, err := openMySQL(ctx, r.cfg.Upstream)
	if err != nil {
		return err
	}
	defer func() { _ = up.Close() }()

	down, err := openMySQL(ctx, r.cfg.Downstream)
	if err != nil {
		return err
	}
	defer func() { _ = down.Close() }()

	model := buildInitialModel(r.cfg)

	// Initialize frozen rows for the motif family (t03).
	for _, t := range model.tables {
		if !t.isMotif {
			continue
		}
		t.mu.Lock()
		for i := int64(1); i <= int64(r.cfg.Bootstrap.FrozenRowsPerTable); i++ {
			t.frozen[i] = struct{}{}
		}
		t.mu.Unlock()
	}

	trace, err := newDDLTrace(r.cfg.Workdir)
	if err != nil {
		return err
	}
	defer trace.close()

	var (
		activeDMLWorkers int32 = int32(r.cfg.DML.InitialWorkers)
		activeDDLWorkers int32 = int32(r.cfg.DDL.InitialWorkers)
		motifStep        int32 = 0
		syncpointChecked int32 = 0
	)

	dmlCounters := &dmlCounters{}
	ddlSelector := newDDLSelector(defaultDDLKinds(), 200)

	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	// DML workers.
	for i := 0; i < r.cfg.DML.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			dmlWorker(ctx, up, model, r.cfg.Seed+10_000, workerID, &activeDMLWorkers, r.cfg.DML, dmlCounters, &motifStep)
		}(i)
	}

	// DDL workers.
	for i := 0; i < r.cfg.DDL.MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ddlWorker(ctx, up, model, r.cfg.Seed+20_000, workerID, &activeDDLWorkers, ddlSelector, trace, r.logger)
		}(i)
	}

	// Primary motif controller.
	wg.Add(1)
	go func() {
		defer wg.Done()
		runPrimaryMotif(ctx, up, model, &motifStep, trace, r.logger, r.cfg.Profile)
	}()

	// Health monitor + auto-tune.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := r.healthAndAutotuneLoop(ctx, dmlCounters, &activeDMLWorkers, &activeDDLWorkers); err != nil {
			select {
			case errCh <- err:
			default:
			}
			cancel()
		}
	}()

	// Big transactions to stress large commit paths (optional, enabled by default).
	if r.cfg.DML.BigTxnEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bigTxnWorker(ctx, up, model, r.cfg.Seed+40_000, r.cfg.DML, &activeDMLWorkers)
		}()
	}

	// Key conflict writer (optional, enabled by default).
	if r.cfg.DML.KeyConflictEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conflictWriter(ctx, up, model, r.cfg.Seed+50_000, r.cfg.DML, dmlCounters)
		}()
	}

	// MySQL syncpoint diff controller (optional).
	if r.cfg.SinkType == "mysql" && r.cfg.MySQL.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.syncpointDiffLoop(ctx, up, down, model, trace, &syncpointChecked); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	// Failover controller (optional).
	if r.cfg.Failover.Enabled && len(r.cfg.Failover.CaptureAddrs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.failoverLoop(ctx, &motifStep, trace); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	r.logger.Printf("workload finished, waiting for converge: %s", r.cfg.Verify.ConvergeWait.Duration)
	time.Sleep(r.cfg.Verify.ConvergeWait.Duration)

	convergeTimeout := r.cfg.Verify.NoAdvanceHard.Duration * 2
	if convergeTimeout < 2*time.Minute {
		convergeTimeout = 2 * time.Minute
	}
	convergeCtx, convergeCancel := context.WithTimeout(context.Background(), convergeTimeout)
	defer convergeCancel()

	if err := r.createAndWaitFinishMark(convergeCtx, up, down, model); err != nil {
		return err
	}

	if r.cfg.SinkType == "mysql" && r.cfg.MySQL.Enabled && r.cfg.MySQL.MaxDiffChecks > 0 {
		need := r.cfg.MySQL.MaxDiffChecks - int(atomic.LoadInt32(&syncpointChecked))
		if need > 0 {
			r.logger.Printf("syncpoint diff: catching up after workload, need=%d", need)
			diffCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := r.ensureSyncpointDiffAfterWorkload(diffCtx, down, model, need); err != nil {
				return err
			}
		}
	}

	if err := r.writeDMLStats(dmlCounters); err != nil {
		return err
	}

	// The workload context is already done here. Use a short-lived context for final verification steps.
	diffCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := r.writeFinalDiffConfig(diffCtx, up, model); err != nil {
		return err
	}

	if r.cfg.Verify.LogScanEnabled {
		if err := scanLogsForPatterns(r.cfg.Workdir, r.cfg.Verify.PanicPatterns, r.cfg.Verify.FailOnPanicMatch, r.logger); err != nil {
			return err
		}
	}

	return nil
}

func (r *runner) healthAndAutotuneLoop(
	ctx context.Context,
	dmlCounters *dmlCounters,
	activeDMLWorkers *int32,
	activeDDLWorkers *int32,
) error {
	// This loop is the guardrail for the stress test:
	//   - It continuously checks changefeed state and checkpoint progress.
	//   - It degrades worker concurrency on stalls or low DML success rate to help recovery.
	//   - It fails the run if checkpoint does not advance for NoAdvanceHard.
	ticker := time.NewTicker(r.cfg.Verify.HealthInterval.Duration)
	defer ticker.Stop()

	var (
		lastCheckpoint uint64
		lastAdvance    = time.Now()
		prevSnap       = dmlCounters.snapshot()
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}

		st, err := r.getChangefeedStatus(ctx)
		if err != nil {
			return err
		}
		if st.State != "normal" {
			return fmt.Errorf("changefeed state is not normal: %s", st.State)
		}
		now := time.Now()
		if lastCheckpoint == 0 {
			lastCheckpoint = st.Checkpoint
			lastAdvance = now
		} else if st.Checkpoint != 0 && st.Checkpoint != lastCheckpoint {
			lastCheckpoint = st.Checkpoint
			lastAdvance = now
		}

		sinceAdvance := now.Sub(lastAdvance)
		snap := dmlCounters.snapshot()
		intervalTotal := snap.Total - prevSnap.Total
		intervalSuccess := snap.Success - prevSnap.Success
		prevSnap = snap

		successRate := 1.0
		if intervalTotal > 0 {
			successRate = float64(intervalSuccess) / float64(intervalTotal)
		}

		r.logger.Printf("health: state=%s checkpoint=%d since_advance=%s dml_total=%d dml_success=%d success_rate=%.3f active_dml=%d active_ddl=%d",
			st.State, st.Checkpoint, sinceAdvance, intervalTotal, intervalSuccess, successRate,
			atomic.LoadInt32(activeDMLWorkers), atomic.LoadInt32(activeDDLWorkers))

		res := autoTuneStep(
			sinceAdvance,
			successRate,
			atomic.LoadInt32(activeDMLWorkers),
			atomic.LoadInt32(activeDDLWorkers),
			int32(r.cfg.DML.MaxWorkers),
			int32(r.cfg.DDL.MaxWorkers),
			r.cfg.Verify.NoAdvanceSoft.Duration,
			r.cfg.Verify.NoAdvanceHard.Duration,
		)
		if res.fail {
			return fmt.Errorf("checkpoint did not advance for %s (hard=%s)", sinceAdvance, r.cfg.Verify.NoAdvanceHard.Duration)
		}
		atomic.StoreInt32(activeDMLWorkers, res.nextDML)
		atomic.StoreInt32(activeDDLWorkers, res.nextDDL)
	}
}

func (r *runner) writeDMLStats(counters *dmlCounters) error {
	snap := counters.snapshot()
	b, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(r.cfg.Workdir, "dml_stats.json"), b)
}

func (r *runner) writeFinalDiffConfig(ctx context.Context, up *sql.DB, model *clusterModel) error {
	// Write a TOML-like config by template to avoid adding new dependencies.
	//
	// Use actual upstream table existence to build the final diff table list.
	// This avoids sync_diff_inspector init failures when churn-domain tables are dropped,
	// recovered to a different name, or partially modified due to concurrent DDL.
	tables, err := listExistingBaseTables(ctx, up, model.dbs)
	if err != nil {
		// Fall back to model state when upstream introspection fails.
		// This should be rare and keeps the runner resilient to transient DB issues.
		r.logger.Printf("final diff: failed to list upstream tables, falling back to model state: err=%v", err)
		for _, t := range model.tables {
			t.mu.Lock()
			exists := t.exists
			dbName := t.db
			tableName := t.name
			t.mu.Unlock()
			if !exists {
				continue
			}
			tables = append(tables, fmt.Sprintf("%s.%s", dbName, tableName))
		}
		sort.Strings(tables)
	}

	var b strings.Builder
	b.WriteString("# diff Configuration.\n\n")
	b.WriteString("check-thread-count = 4\n\n")
	b.WriteString("export-fix-sql = true\n\n")
	b.WriteString("check-struct-only = false\n\n")
	b.WriteString("[task]\n")
	b.WriteString(fmt.Sprintf("    output-dir = %q\n\n", filepath.Join(r.cfg.Workdir, "sync_diff", "output")))
	b.WriteString("    source-instances = [\"upstream\"]\n\n")
	b.WriteString("    target-instance = \"downstream\"\n\n")
	b.WriteString("    target-check-tables = [\n")
	for i, t := range tables {
		sep := ","
		if i == len(tables)-1 {
			sep = ""
		}
		b.WriteString(fmt.Sprintf("        %q%s\n", t, sep))
	}
	b.WriteString("    ]\n\n")
	b.WriteString("[data-sources]\n")
	b.WriteString("[data-sources.upstream]\n")
	b.WriteString(fmt.Sprintf("    host = %q\n", r.cfg.Upstream.Host))
	b.WriteString(fmt.Sprintf("    port = %d\n", r.cfg.Upstream.Port))
	b.WriteString(fmt.Sprintf("    user = %q\n", r.cfg.Upstream.User))
	b.WriteString(fmt.Sprintf("    password = %q\n\n", r.cfg.Upstream.Password))

	b.WriteString("[data-sources.downstream]\n")
	b.WriteString(fmt.Sprintf("    host = %q\n", r.cfg.Downstream.Host))
	b.WriteString(fmt.Sprintf("    port = %d\n", r.cfg.Downstream.Port))
	b.WriteString(fmt.Sprintf("    user = %q\n", r.cfg.Downstream.User))
	b.WriteString(fmt.Sprintf("    password = %q\n", r.cfg.Downstream.Password))

	return os.WriteFile(filepath.Join(r.cfg.Workdir, "diff_config.toml"), []byte(b.String()), 0o644)
}

func listExistingBaseTables(ctx context.Context, db *sql.DB, dbs []string) ([]string, error) {
	var tables []string
	for _, dbName := range dbs {
		// dbName is generated by the runner and should be safe to embed in a quoted identifier.
		q := fmt.Sprintf("SHOW FULL TABLES IN `%s` WHERE Table_Type = 'BASE TABLE';", dbName)
		rows, err := db.QueryContext(ctx, q)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var tblName string
			var tblType string
			if err := rows.Scan(&tblName, &tblType); err != nil {
				_ = rows.Close()
				return nil, err
			}
			tables = append(tables, fmt.Sprintf("%s.%s", dbName, tblName))
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	sort.Strings(tables)
	return tables, nil
}

func (r *runner) createAndWaitFinishMark(ctx context.Context, up, down *sql.DB, model *clusterModel) error {
	// The finish mark is a replication barrier: the workload is already stopped, but the
	// sink / consumer may still be draining. Waiting for the finish mark to appear on
	// downstream provides a deterministic "catch up" point before running the final diff.
	if len(model.dbs) == 0 {
		return fmt.Errorf("no databases in model")
	}

	markerDB := model.dbs[0]
	const markerTable = "finish_mark"
	const markerID int64 = 1
	markerValue := r.cfg.Seed

	r.logger.Printf("converge: creating finish mark table: db=%s table=%s id=%d value=%d",
		markerDB, markerTable, markerID, markerValue)

	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (`id` BIGINT PRIMARY KEY, `v` BIGINT NOT NULL)",
		markerDB, markerTable)
	if _, err := up.ExecContext(ctx, createSQL); err != nil {
		return err
	}
	insertSQL := fmt.Sprintf("REPLACE INTO `%s`.`%s` (`id`, `v`) VALUES (?, ?)", markerDB, markerTable)
	if _, err := up.ExecContext(ctx, insertSQL, markerID, markerValue); err != nil {
		return err
	}

	r.logger.Printf("converge: waiting for finish mark to appear in downstream")

	pollTicker := time.NewTicker(2 * time.Second)
	defer pollTicker.Stop()
	healthTicker := time.NewTicker(r.cfg.Verify.HealthInterval.Duration)
	defer healthTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-healthTicker.C:
			st, err := r.getChangefeedStatus(ctx)
			if err != nil {
				return err
			}
			if st.State != "normal" {
				return fmt.Errorf("changefeed state is not normal: %s", st.State)
			}
			r.logger.Printf("converge: waiting for finish mark, checkpoint=%d", st.Checkpoint)
		case <-pollTicker.C:
			var got int64
			q := fmt.Sprintf("SELECT `v` FROM `%s`.`%s` WHERE `id` = ?", markerDB, markerTable)
			err := down.QueryRowContext(ctx, q, markerID).Scan(&got)
			if err == nil {
				if got == markerValue {
					r.logger.Printf("converge done: finish mark applied downstream")
					return nil
				}
				r.logger.Printf("converge: finish mark row value mismatch: got=%d want=%d", got, markerValue)
				continue
			}
			if err == sql.ErrNoRows {
				continue
			}
			// Table may not exist yet on MQ sinks.
			if strings.Contains(err.Error(), "doesn't exist") {
				continue
			}
			return err
		}
	}
}
