package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
)

type ddlWindow struct {
	start uint64
	end   uint64
}

const mysqlErrNoSuchTable uint16 = 1146

func (r *runner) syncpointDiffLoop(
	ctx context.Context,
	up *sql.DB,
	down *sql.DB,
	model *clusterModel,
	trace *ddlTrace,
	successCounter *int32,
) error {
	_ = up
	_ = trace

	// syncpointDiffLoop periodically runs snapshot diffs based on TiCDC syncpoints.
	//
	// Motivation:
	//   - The final diff runs at the end of the test and may not pinpoint when divergence happened.
	//   - Syncpoints provide pairs of (primary_ts on upstream, secondary_ts on downstream) that
	//     can be used for snapshot reads. Running diffs at several syncpoints helps localize issues.
	//
	// Practicality:
	//   - Snapshot diffing is fragile near DDL windows. We conservatively skip candidates that fall
	//     into TiDB DDL windows obtained from upstream /ddl/history.
	if r.cfg.MySQL.DiffInterval.Duration <= 0 {
		return nil
	}
	ticker := time.NewTicker(r.cfg.MySQL.DiffInterval.Duration)
	defer ticker.Stop()

	var (
		lastPrimary uint64
		checked     int
	)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if checked >= r.cfg.MySQL.MaxDiffChecks {
			return nil
		}

		n, err := r.runSyncpointDiffChecks(ctx, down, model, 1, &lastPrimary, false)
		if err != nil {
			return err
		}
		if n > 0 {
			checked += n
			if successCounter != nil {
				atomic.StoreInt32(successCounter, int32(checked))
			}
		}
	}
}

func (r *runner) runSyncpointDiffChecks(
	ctx context.Context,
	down *sql.DB,
	model *clusterModel,
	required int,
	lastPrimary *uint64,
	allowInDDLWindow bool,
) (int, error) {
	// Run up to "required" syncpoint diffs and update lastPrimary to advance the cursor.
	if required <= 0 {
		return 0, nil
	}
	if lastPrimary == nil {
		return 0, fmt.Errorf("lastPrimary must not be nil")
	}

	windows, err := fetchDDLWindows(ctx, r.cfg.MySQL.UpstreamStatusHost, r.cfg.MySQL.UpstreamStatusPort)
	if err != nil {
		return 0, err
	}

	checked := 0
	for tries := 0; tries < 50 && checked < required; tries++ {
		p, s, got, err := pickNextSyncpointCandidate(ctx, down, *lastPrimary)
		if err != nil {
			return checked, err
		}
		if !got {
			return checked, nil
		}
		inWindow := inDDLWindow(p, windows)
		if inWindow && !allowInDDLWindow {
			r.logger.Printf("syncpoint diff: skip primary_ts=%d (in DDL window)", p)
			*lastPrimary = p
			continue
		}

		confPath := filepath.Join(r.cfg.Workdir, fmt.Sprintf("diff_config_syncpoint_%d.toml", p))
		if err := r.writeSyncpointDiffConfig(confPath, model, p, s); err != nil {
			return checked, err
		}

		logPath := filepath.Join(r.cfg.Workdir, fmt.Sprintf("sync_diff_inspector_syncpoint_%d.log", p))
		diag, err := r.runSyncDiffInspectorWithSnapshotGuard(ctx, confPath, logPath, 3)
		if err != nil {
			if ctx.Err() != nil {
				return checked, nil
			}
			if isSkippableSyncDiffFailure(diag) {
				r.logger.Printf("syncpoint diff: skip primary_ts=%d (sync diff not applicable, see %s)", p, logPath)
				*lastPrimary = p
				continue
			}
			if inWindow {
				r.logger.Printf("syncpoint diff: skip primary_ts=%d (diff failed in DDL window, see %s)", p, logPath)
				*lastPrimary = p
				continue
			}
			return checked, err
		}

		checked++
		*lastPrimary = p
		r.logger.Printf("syncpoint diff: success primary_ts=%d secondary_ts=%d", p, s)
	}
	return checked, nil
}

func (r *runner) ensureSyncpointDiffAfterWorkload(
	ctx context.Context,
	down *sql.DB,
	model *clusterModel,
	required int,
) error {
	if required <= 0 {
		return nil
	}
	var lastPrimary uint64
	checked := 0
	for checked < required {
		n, err := r.runSyncpointDiffChecks(ctx, down, model, required-checked, &lastPrimary, true)
		if err != nil {
			return err
		}
		checked += n
		if checked >= required {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("syncpoint diff did not complete: required=%d checked=%d: %w", required, checked, ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
	return nil
}

func pickNextSyncpointCandidate(ctx context.Context, down *sql.DB, after uint64) (primary uint64, secondary uint64, ok bool, err error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	rows, err := down.QueryContext(queryCtx,
		"SELECT primary_ts, secondary_ts FROM tidb_cdc.syncpoint_v1 WHERE primary_ts > ? ORDER BY primary_ts ASC LIMIT 200",
		after,
	)
	if err != nil {
		if isNoSuchTableError(err) {
			// TiCDC creates tidb_cdc.syncpoint_v1 lazily when the first syncpoint is flushed.
			// Treat a missing table as "no candidate yet" so early periodic checks keep waiting.
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var p, s uint64
		if err := rows.Scan(&p, &s); err != nil {
			return 0, 0, false, err
		}
		return p, s, true, nil
	}
	if err := rows.Err(); err != nil {
		return 0, 0, false, err
	}
	return 0, 0, false, nil
}

func isNoSuchTableError(err error) bool {
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == mysqlErrNoSuchTable
}

func fetchDDLWindows(ctx context.Context, host string, port int) ([]ddlWindow, error) {
	// TiDB exposes recent DDL jobs via /ddl/history. We treat the job runtime as a window
	// where snapshot reads may be inconsistent across schema versions.
	u := fmt.Sprintf("http://%s:%d/ddl/history", host, port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("ddl history http %d: %s", resp.StatusCode, string(b))
	}

	var v any
	if err := json.NewDecoder(resp.Body).Decode(&v); err != nil {
		return nil, err
	}
	var windows []ddlWindow
	extractDDLWindows(v, &windows)

	sort.Slice(windows, func(i, j int) bool { return windows[i].start < windows[j].start })
	return windows, nil
}

func extractDDLWindows(v any, out *[]ddlWindow) {
	switch x := v.(type) {
	case map[string]any:
		start := parseUint64(x["real_start_ts"])
		end := parseUint64(x["FinishedTS"])
		if start != 0 {
			// For running DDL jobs, FinishedTS may be 0. Treat it as an open-ended window so
			// we can conservatively skip syncpoints that may observe inconsistent snapshots.
			if end == 0 {
				end = ^uint64(0)
			}
			*out = append(*out, ddlWindow{start: start, end: end})
		}
		for _, vv := range x {
			extractDDLWindows(vv, out)
		}
	case []any:
		for _, vv := range x {
			extractDDLWindows(vv, out)
		}
	}
}

func inDDLWindow(ts uint64, windows []ddlWindow) bool {
	for _, w := range windows {
		if ts > w.start && ts < w.end {
			return true
		}
	}
	return false
}

func (r *runner) writeSyncpointDiffConfig(path string, model *clusterModel, primary, secondary uint64) error {
	// Only diff stable domain tables.
	var stable []string
	for _, t := range model.stableTables {
		stable = append(stable, fmt.Sprintf("%s.%s", t.db, t.name))
	}
	sort.Strings(stable)

	var b strings.Builder
	b.WriteString("# diff Configuration.\n\n")
	b.WriteString("check-thread-count = 4\n\n")
	b.WriteString("export-fix-sql = true\n\n")
	b.WriteString("check-struct-only = false\n\n")
	b.WriteString("[task]\n")
	b.WriteString(fmt.Sprintf("    output-dir = %q\n\n", filepath.Join(r.cfg.Workdir, "sync_diff", fmt.Sprintf("syncpoint_%d", primary), "output")))
	b.WriteString("    source-instances = [\"upstream\"]\n\n")
	b.WriteString("    target-instance = \"downstream\"\n\n")
	b.WriteString("    target-check-tables = [\n")
	for i, t := range stable {
		sep := ","
		if i == len(stable)-1 {
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
	b.WriteString(fmt.Sprintf("    password = %q\n", r.cfg.Upstream.Password))
	b.WriteString(fmt.Sprintf("    snapshot = %q\n\n", fmt.Sprintf("%d", primary)))

	b.WriteString("[data-sources.downstream]\n")
	b.WriteString(fmt.Sprintf("    host = %q\n", r.cfg.Downstream.Host))
	b.WriteString(fmt.Sprintf("    port = %d\n", r.cfg.Downstream.Port))
	b.WriteString(fmt.Sprintf("    user = %q\n", r.cfg.Downstream.User))
	b.WriteString(fmt.Sprintf("    password = %q\n", r.cfg.Downstream.Password))
	b.WriteString(fmt.Sprintf("    snapshot = %q\n", fmt.Sprintf("%d", secondary)))

	return os.WriteFile(path, []byte(b.String()), 0o644)
}

type tailBuffer struct {
	buf []byte
	max int
}

func newTailBuffer(maxBytes int) *tailBuffer {
	return &tailBuffer{max: maxBytes}
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	if t == nil || t.max <= 0 {
		return len(p), nil
	}
	if len(p) >= t.max {
		t.buf = append(t.buf[:0], p[len(p)-t.max:]...)
		return len(p), nil
	}
	if len(t.buf)+len(p) <= t.max {
		t.buf = append(t.buf, p...)
		return len(p), nil
	}
	overflow := len(t.buf) + len(p) - t.max
	t.buf = append(t.buf[overflow:], p...)
	return len(p), nil
}

func (t *tailBuffer) String() string {
	if t == nil {
		return ""
	}
	return string(t.buf)
}

func isSkippableSyncDiffFailure(outputTail string) bool {
	// sync_diff_inspector runs snapshot reads and may fail with schema-related errors when a syncpoint
	// is observed during (or near) a DDL window. Treat those cases as "invalid syncpoint" and skip.
	s := strings.ToLower(outputTail)
	switch {
	case strings.Contains(s, "unknown column"):
		return true
	case strings.Contains(s, "no table need to be compared"):
		return true
	default:
		return false
	}
}

const (
	tidbEnableExternalTSReadVar = "tidb_enable_external_ts_read"
	externalTSReadOffParam      = tidbEnableExternalTSReadVar + "=OFF"
)

func (r *runner) runSyncDiffInspectorWithSnapshotGuard(ctx context.Context, confPath, logPath string, retries int) (string, error) {
	// sync_diff_inspector should compare only the snapshot pair from the config.
	// Keep downstream external-ts reads disabled during the diff so any connection
	// that misses its configured snapshot cannot fall back to a later syncpoint.
	downstream, err := openMySQLWithExtraParams(ctx, r.cfg.Downstream, externalTSReadOffParam)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = downstream.Close()
	}()

	original, err := queryGlobalExternalTSRead(ctx, downstream)
	if err != nil {
		return "", err
	}
	if err := setGlobalExternalTSRead(ctx, downstream, "OFF"); err != nil {
		return "", err
	}

	diag, runErr := runSyncDiffInspector(ctx, confPath, logPath, retries)
	if original == "OFF" {
		return diag, runErr
	}

	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	restoreErr := setGlobalExternalTSRead(restoreCtx, downstream, original)
	cancel()
	if restoreErr != nil {
		r.logger.Printf("syncpoint diff: failed to restore %s=%s: err=%v", tidbEnableExternalTSReadVar, original, restoreErr)
		if runErr == nil {
			return diag, restoreErr
		}
	}
	return diag, runErr
}

func queryGlobalExternalTSRead(ctx context.Context, downstream *sql.DB) (string, error) {
	var value string
	if err := downstream.QueryRowContext(ctx, "SELECT @@global."+tidbEnableExternalTSReadVar).Scan(&value); err != nil {
		return "", err
	}
	return normalizeExternalTSReadValue(value)
}

func setGlobalExternalTSRead(ctx context.Context, downstream *sql.DB, value string) error {
	normalized, err := normalizeExternalTSReadValue(value)
	if err != nil {
		return err
	}
	_, err = downstream.ExecContext(ctx, "SET GLOBAL "+tidbEnableExternalTSReadVar+" = "+normalized)
	return err
}

func normalizeExternalTSReadValue(value string) (string, error) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ON", "1", "TRUE":
		return "ON", nil
	case "OFF", "0", "FALSE":
		return "OFF", nil
	default:
		return "", fmt.Errorf("unexpected %s value: %q", tidbEnableExternalTSReadVar, value)
	}
}

func runSyncDiffInspector(ctx context.Context, confPath, logPath string, retries int) (string, error) {
	// sync_diff_inspector output can be large. Keep a tail buffer for diagnostics while
	// still appending full logs to a file in the workdir.
	if retries < 1 {
		retries = 1
	}

	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var lastTail string
	for i := 0; i < retries; i++ {
		tail := newTailBuffer(64 * 1024)
		w := io.MultiWriter(f, tail)
		cmd := exec.CommandContext(ctx, "sync_diff_inspector", "--log-level=debug", "--config="+confPath)
		cmd.Stdout = w
		cmd.Stderr = w
		err = cmd.Run()
		if err == nil {
			return "", nil
		}
		lastTail = tail.String()
		select {
		case <-ctx.Done():
			return lastTail, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return lastTail, err
}
