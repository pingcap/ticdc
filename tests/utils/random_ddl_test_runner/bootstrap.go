package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

func (r *runner) bootstrap() error {
	// bootstrap creates an identical starting point on upstream and downstream.
	//
	// Rationale:
	//   - The workload phase only writes to upstream. Downstream changes must come from TiCDC replication.
	//   - A deterministic baseline makes end-to-end diffs and triage reproducible (seeded by cfg.Seed).
	ctx := context.Background()
	r.logger.Printf("bootstrap start: workdir=%s profile=%s", r.cfg.Workdir, r.cfg.Profile)

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

	for _, dbName := range model.dbs {
		if err := execBoth(ctx, up, down,
			fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName),
			fmt.Sprintf("CREATE DATABASE `%s`", dbName),
		); err != nil {
			return err
		}
	}

	for _, tbl := range model.tables {
		createSQL := tbl.schema.createTableSQL(tbl.db, tbl.name)
		if err := execBoth(ctx, up, down, createSQL); err != nil {
			return err
		}
	}

	// Deny region merge on split candidate tables to keep region pressure stable.
	for _, tbl := range model.splitTables {
		attrsSQL := fmt.Sprintf("ALTER TABLE %s ATTRIBUTES 'merge_option=deny'", tbl.fqName())
		if err := execBoth(ctx, up, down, attrsSQL); err != nil {
			return err
		}
	}

	baseRows := r.cfg.Bootstrap.BaseRowsPerTable
	splitRows := r.cfg.Bootstrap.SplitRowsPerTable

	for _, tbl := range model.tables {
		rows := baseRows
		if tbl.domain == domainSplit {
			rows += splitRows
		}
		if err := insertInitialRows(ctx, up, down, tbl, rows); err != nil {
			return err
		}
	}

	r.logger.Printf("bootstrap done")
	return nil
}

func execBoth(ctx context.Context, up, down *sql.DB, stmts ...string) error {
	for _, s := range stmts {
		if _, err := up.ExecContext(ctx, s); err != nil {
			return err
		}
		if _, err := down.ExecContext(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

func insertInitialRows(ctx context.Context, up, down *sql.DB, tbl *table, rows int) error {
	tbl.mu.Lock()
	schema := tbl.schema.clone()
	tbl.mu.Unlock()

	// Use placeholders for values to keep SQL ASCII-only while allowing any binary/JSON payloads.
	var cols []column
	for _, c := range schema.columns {
		if c.generated != "" {
			continue
		}
		cols = append(cols, c)
	}

	colNames := make([]string, 0, len(cols))
	for _, c := range cols {
		colNames = append(colNames, c.name)
	}

	const batchSize = 200
	for start := 1; start <= rows; start += batchSize {
		end := start + batchSize - 1
		if end > rows {
			end = rows
		}

		var args []any
		var valuesSQL strings.Builder
		for i := start; i <= end; i++ {
			if i > start {
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

			rowArgs := buildDeterministicRowArgs(tbl, cols, int64(i))
			args = append(args, rowArgs...)
		}

		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			tbl.fqName(),
			backtickJoin(colNames),
			valuesSQL.String(),
		)
		if _, err := up.ExecContext(ctx, stmt, args...); err != nil {
			return err
		}
		if _, err := down.ExecContext(ctx, stmt, args...); err != nil {
			return err
		}
	}
	return nil
}

func backtickJoin(cols []string) string {
	quoted := make([]string, 0, len(cols))
	for _, c := range cols {
		quoted = append(quoted, fmt.Sprintf("`%s`", c))
	}
	return strings.Join(quoted, ",")
}

func buildDeterministicRowArgs(tbl *table, cols []column, rowID int64) []any {
	// Deterministic values make bootstrap reproducible. Avoid non-ASCII in the SQL text by
	// passing bytes/JSON via placeholders rather than embedding literals into the statement.
	args := make([]any, 0, len(cols))
	for _, c := range cols {
		switch strings.ToUpper(c.typ.base) {
		case "BIGINT":
			if c.name == "id" {
				args = append(args, rowID)
			} else {
				args = append(args, deterministicInt64(rowID))
			}
		case "INT":
			if c.name == "a" {
				args = append(args, int32(rowID))
			} else if c.name == "v" {
				args = append(args, int32(rowID%1000))
			} else {
				args = append(args, int32(deterministicInt64(rowID)%mathMaxInt32()))
			}
		case "VARCHAR":
			if c.name == "pad" {
				args = append(args, strings.Repeat("x", 256))
			} else {
				args = append(args, asciiStringFromID(fmt.Sprintf("%s_%s", tbl.name, c.name), rowID))
			}
		case "DATETIME":
			args = append(args, deterministicTime(rowID))
		case "DECIMAL":
			args = append(args, deterministicDecimal(rowID))
		case "JSON":
			args = append(args, fmt.Sprintf("{\"id\":%d,\"table\":\"%s\"}", rowID, tbl.name))
		case "VARBINARY":
			// Keep bytes deterministic; the SQL text remains ASCII-only due to placeholders.
			args = append(args, []byte(fmt.Sprintf("%064x", rowID)))
		default:
			args = append(args, nil)
		}
	}
	return args
}

func mathMaxInt32() int64 {
	return int64(^uint32(0) >> 1)
}

func sleepWithContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
