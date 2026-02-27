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
	"encoding/base64"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/util"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"go.uber.org/zap"
)

func main() {
	var (
		sinkURIString = flag.String("sink-uri", "", "Iceberg sink URI, e.g. iceberg://?warehouse=s3://bucket/wh&catalog=glue&region=us-west-2&namespace=ns")
		tidbDSN       = flag.String("tidb-dsn", "", "TiDB DSN, e.g. user:pass@tcp(host:4000)/?charset=utf8mb4")
		schemaName    = flag.String("schema", "", "TiDB schema/database name")
		tableName     = flag.String("table", "", "TiDB table name")
		whereClause   = flag.String("where", "", "Optional WHERE clause (without the 'WHERE' keyword)")
		limitRows     = flag.Int("limit", 0, "Optional LIMIT")
		batchRows     = flag.Int("batch-rows", 10000, "Max rows per Iceberg commit (0 means all rows in one commit)")
		changefeed    = flag.String("changefeed", "default/iceberg-bootstrap", "Changefeed display name as keyspace/name used for table ownership")
	)
	flag.Parse()

	if strings.TrimSpace(*sinkURIString) == "" || strings.TrimSpace(*tidbDSN) == "" || strings.TrimSpace(*schemaName) == "" || strings.TrimSpace(*tableName) == "" {
		fmt.Fprintln(os.Stderr, "missing required flags: --sink-uri, --tidb-dsn, --schema, --table")
		os.Exit(2)
	}

	keyspace, changefeedName, err := parseChangefeedDisplayName(strings.TrimSpace(*changefeed))
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --changefeed: %v\n", err)
		os.Exit(2)
	}
	changefeedID := common.NewChangeFeedIDWithName(changefeedName, keyspace)

	ctx := context.Background()

	sinkURI, err := url.Parse(strings.TrimSpace(*sinkURIString))
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse sink uri failed: %v\n", err)
		os.Exit(2)
	}

	cfg := sinkiceberg.NewConfig()
	if err := cfg.Apply(ctx, sinkURI, nil); err != nil {
		fmt.Fprintf(os.Stderr, "parse iceberg config failed: %v\n", err)
		os.Exit(2)
	}

	warehouseStorage, err := util.GetExternalStorageWithDefaultTimeout(ctx, cfg.WarehouseURI)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open iceberg warehouse failed: %v\n", err)
		os.Exit(1)
	}
	defer warehouseStorage.Close()

	writer := sinkiceberg.NewTableWriter(cfg, warehouseStorage)
	if err := writer.VerifyCatalog(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "verify iceberg catalog failed: %v\n", err)
		os.Exit(2)
	}

	db, err := sql.Open("mysql", strings.TrimSpace(*tidbDSN))
	if err != nil {
		fmt.Fprintf(os.Stderr, "open tidb connection failed: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	startTs, snapshotTimeRFC3339, err := getSnapshotPoint(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get snapshot point failed: %v\n", err)
		os.Exit(1)
	}

	if err := setTiDBSnapshot(ctx, db, snapshotTimeRFC3339); err != nil {
		fmt.Fprintf(os.Stderr, "set tidb_snapshot failed: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := clearTiDBSnapshot(ctx, db); err != nil {
			log.Warn("clear tidb_snapshot failed", zap.Error(err))
		}
	}()

	createTableSQL, err := dbutil.GetCreateTableSQL(ctx, db, strings.TrimSpace(*schemaName), strings.TrimSpace(*tableName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "get create table sql failed: %v\n", err)
		os.Exit(1)
	}
	parser, err := dbutil.GetParserForDB(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "get parser failed: %v\n", err)
		os.Exit(1)
	}
	tiTableInfo, err := dbutiltest.GetTableInfoBySQL(createTableSQL, parser)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse table info failed: %v\n", err)
		os.Exit(1)
	}

	tableInfo := common.WrapTableInfo(strings.TrimSpace(*schemaName), tiTableInfo)

	selectCols, colInfos := buildSelectColumns(tableInfo)
	if len(selectCols) == 0 {
		fmt.Fprintln(os.Stderr, "no columns found (all columns are virtual generated?)")
		os.Exit(2)
	}

	var equalityFieldIDs []int
	if cfg.Mode == sinkiceberg.ModeUpsert {
		equalityFieldIDs, err = sinkiceberg.GetEqualityFieldIDs(tableInfo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "get equality field ids failed: %v\n", err)
			os.Exit(2)
		}
		if len(equalityFieldIDs) == 0 {
			fmt.Fprintf(os.Stderr, "upsert mode requires a primary key or not null unique key\n")
			os.Exit(2)
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectCols, ","), quoteSchemaTable(strings.TrimSpace(*schemaName), strings.TrimSpace(*tableName)))
	if strings.TrimSpace(*whereClause) != "" {
		query += " WHERE " + strings.TrimSpace(*whereClause)
	}
	if *limitRows > 0 {
		query += fmt.Sprintf(" LIMIT %d", *limitRows)
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "query upstream table failed: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()

	var (
		written int
		batch   []sinkiceberg.ChangeRow
	)
	if *batchRows > 0 {
		batch = make([]sinkiceberg.ChangeRow, 0, *batchRows)
	}

	raw := make([]sql.RawBytes, len(colInfos))
	dest := make([]any, len(colInfos))
	for i := range raw {
		dest[i] = &raw[i]
	}

	commitTsStr := strconv.FormatUint(startTs, 10)
	commitBatch := func(batchRows []sinkiceberg.ChangeRow) error {
		switch cfg.Mode {
		case sinkiceberg.ModeUpsert:
			_, err := writer.Upsert(ctx, changefeedID, tableInfo, tableInfo.TableName.TableID, batchRows, nil, equalityFieldIDs, startTs)
			return err
		default:
			_, err := writer.AppendChangelog(ctx, changefeedID, tableInfo, tableInfo.TableName.TableID, batchRows, startTs)
			return err
		}
	}
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			fmt.Fprintf(os.Stderr, "scan row failed: %v\n", err)
			os.Exit(1)
		}

		cols := make(map[string]*string, len(colInfos))
		for i, c := range colInfos {
			if raw[i] == nil {
				cols[c.Name.O] = nil
				continue
			}
			v, err := formatSQLValue(raw[i], &c.FieldType)
			if err != nil {
				fmt.Fprintf(os.Stderr, "format value failed: %v\n", err)
				os.Exit(1)
			}
			cols[c.Name.O] = v
		}

		row := sinkiceberg.ChangeRow{
			Op:         "I",
			CommitTs:   commitTsStr,
			CommitTime: snapshotTimeRFC3339,
			Columns:    cols,
		}

		if *batchRows <= 0 {
			batch = append(batch, row)
			written++
			continue
		}
		batch = append(batch, row)
		written++
		if len(batch) >= *batchRows {
			if err := commitBatch(batch); err != nil {
				fmt.Fprintf(os.Stderr, "iceberg commit failed: %v\n", err)
				os.Exit(1)
			}
			batch = batch[:0]
		}
	}
	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "query rows failed: %v\n", err)
		os.Exit(1)
	}

	if len(batch) > 0 {
		if err := commitBatch(batch); err != nil {
			fmt.Fprintf(os.Stderr, "iceberg commit failed: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("bootstrap completed\n")
	fmt.Printf("rows: %d\n", written)
	fmt.Printf("snapshot commit_ts: %s\n", commitTsStr)
	fmt.Printf("snapshot commit_time: %s\n", snapshotTimeRFC3339)
	fmt.Printf("recommended changefeed start-ts: %s\n", commitTsStr)
}

func parseChangefeedDisplayName(raw string) (string, string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", "", errors.ErrSinkURIInvalid.GenWithStackByArgs("changefeed is empty")
	}
	parts := strings.Split(s, "/")
	if len(parts) == 1 {
		return "default", parts[0], nil
	}
	if len(parts) != 2 {
		return "", "", errors.ErrSinkURIInvalid.GenWithStackByArgs("changefeed must be keyspace/name")
	}
	if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", errors.ErrSinkURIInvalid.GenWithStackByArgs("changefeed must be keyspace/name")
	}
	return parts[0], parts[1], nil
}

func getSnapshotPoint(ctx context.Context, db *sql.DB) (uint64, string, error) {
	var tsoStr string
	if err := db.QueryRowContext(ctx, "select @@tidb_current_ts").Scan(&tsoStr); err != nil {
		return 0, "", errors.Trace(err)
	}
	tsoStr = strings.TrimSpace(tsoStr)
	tso, err := strconv.ParseUint(tsoStr, 10, 64)
	if err != nil {
		return 0, "", errors.Trace(err)
	}

	var tsStr string
	if err := db.QueryRowContext(ctx, "select TIDB_PARSE_TSO(?)", tsoStr).Scan(&tsStr); err != nil {
		return 0, "", errors.Trace(err)
	}

	parsed, err := time.ParseInLocation("2006-01-02 15:04:05.999999", strings.TrimSpace(tsStr), time.UTC)
	if err != nil {
		return 0, "", errors.Trace(err)
	}
	return tso, parsed.UTC().Format(time.RFC3339Nano), nil
}

func setTiDBSnapshot(ctx context.Context, db *sql.DB, snapshotTimeRFC3339 string) error {
	t, err := time.Parse(time.RFC3339Nano, snapshotTimeRFC3339)
	if err != nil {
		return errors.Trace(err)
	}
	snapshot := t.UTC().Format("2006-01-02 15:04:05.999999")
	_, err = db.ExecContext(ctx, "set @@tidb_snapshot = ?", snapshot)
	return errors.Trace(err)
}

func clearTiDBSnapshot(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "set @@tidb_snapshot = ''")
	return errors.Trace(err)
}

func buildSelectColumns(tableInfo *common.TableInfo) ([]string, []*timodel.ColumnInfo) {
	if tableInfo == nil {
		return nil, nil
	}
	cols := tableInfo.GetColumns()
	out := make([]string, 0, len(cols))
	infos := make([]*timodel.ColumnInfo, 0, len(cols))
	for _, c := range cols {
		if c == nil || c.IsVirtualGenerated() {
			continue
		}
		switch c.FieldType.GetType() {
		case mysql.TypeEnum, mysql.TypeSet:
			out = append(out, fmt.Sprintf("(%s+0) AS %s", quoteIdent(c.Name.O), quoteIdent(c.Name.O)))
		default:
			out = append(out, quoteIdent(c.Name.O))
		}
		infos = append(infos, c)
	}
	return out, infos
}

func quoteSchemaTable(schemaName string, tableName string) string {
	return fmt.Sprintf("%s.%s", quoteIdent(schemaName), quoteIdent(tableName))
}

func quoteIdent(ident string) string {
	s := strings.ReplaceAll(ident, "`", "``")
	return "`" + s + "`"
}

func formatSQLValue(raw sql.RawBytes, ft *types.FieldType) (*string, error) {
	if raw == nil {
		return nil, nil
	}
	if ft == nil {
		v := string(raw)
		return &v, nil
	}

	b := []byte(raw)
	var value string
	switch ft.GetType() {
	case mysql.TypeBit:
		// For BIT columns, TiDB/MySQL often returns bytes rather than ascii digits.
		// Convert big-endian bytes into a uint64 decimal string.
		if u, err := strconv.ParseUint(string(b), 10, 64); err == nil {
			value = strconv.FormatUint(u, 10)
			return &value, nil
		}
		var u uint64
		for _, by := range b {
			u = (u << 8) | uint64(by)
		}
		value = strconv.FormatUint(u, 10)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		value = base64.StdEncoding.EncodeToString(b)
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			value = base64.StdEncoding.EncodeToString(b)
		} else {
			value = string(b)
		}
	default:
		value = string(b)
	}
	return &value, nil
}
