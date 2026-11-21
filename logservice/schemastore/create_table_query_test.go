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

package schemastore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/expression/exprstatic"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

// TestCreateTablesSQLModeSemantics compares TiDB table metadata built using the
// original session modes and the normalized SQL after event splitting.
func TestCreateTablesSQLModeSemantics(t *testing.T) {
	for _, tc := range []struct {
		name           string
		firstMode      mysql.SQLMode
		secondMode     mysql.SQLMode
		secondSQL      string
		timestampInUTC bool
	}{
		{
			name:       "escaped default and comments",
			secondMode: mysql.ModeNoBackslashEscapes,
			secondSQL:  `CREATE TABLE t2 (id INT PRIMARY KEY, s VARCHAR(32) DEFAULT '\n' COMMENT '\t') COMMENT='\r'`,
		},
		{
			name:      "backslash escapes after no backslash escapes",
			firstMode: mysql.ModeNoBackslashEscapes,
			secondSQL: `CREATE TABLE t2 (id INT PRIMARY KEY, s VARCHAR(32) DEFAULT '\n')`,
		},
		{
			name:       "generated expression",
			secondMode: mysql.ModeNoBackslashEscapes,
			secondSQL:  `CREATE TABLE t2 (id INT PRIMARY KEY, s VARCHAR(32) AS (CONCAT('x\n', id)))`,
		},
		{
			name:       "index comment",
			secondMode: mysql.ModeNoBackslashEscapes,
			secondSQL:  `CREATE TABLE t2 (id INT PRIMARY KEY, s VARCHAR(32), KEY idx(s) COMMENT '\n')`,
		},
		{
			name:       "enum elements",
			secondMode: mysql.ModeNoBackslashEscapes,
			secondSQL:  `CREATE TABLE t2 (id INT PRIMARY KEY, s ENUM('a\n', 'b\t'))`,
		},
		{
			name:           "unrelated timestamp context",
			secondMode:     mysql.ModeNoBackslashEscapes,
			secondSQL:      `CREATE TABLE t2 (id INT PRIMARY KEY, ts TIMESTAMP DEFAULT '2020-01-01 00:00:00') COMMENT='\n'`,
			timestampInUTC: true,
		},
		{
			name:       "partition boundary",
			secondMode: mysql.ModeNoBackslashEscapes,
			secondSQL:  `CREATE TABLE t2 (id INT, s VARCHAR(32), PRIMARY KEY(id, s)) PARTITION BY RANGE COLUMNS(s) (PARTITION p0 VALUES LESS THAN ('z\n') COMMENT '\t', PARTITION p1 VALUES LESS THAN (MAXVALUE))`,
		},
		{
			name:      "misleading separator",
			firstMode: mysql.ModeNoBackslashEscapes,
			secondSQL: `CREATE TABLE t2 (id INT PRIMARY KEY) COMMENT='x\'; SELECT 1; z'`,
		},
		{
			name:      "misleading CREATE",
			firstMode: mysql.ModeNoBackslashEscapes,
			secondSQL: `CREATE TABLE t2 (id INT PRIMARY KEY) COMMENT='x\'; CREATE TABLE injected (id INT); z'`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buildTableInfo := func(query string, mode mysql.SQLMode) *model.TableInfo {
				p := parser.New()
				p.SetSQLMode(mode)
				stmt, err := p.ParseOneStmt(query, "", "")
				require.NoError(t, err)
				ctx := metabuild.NewContext(metabuild.WithExprCtx(exprstatic.NewExprContext(
					exprstatic.WithEvalCtx(exprstatic.NewEvalContext(exprstatic.WithSQLMode(mode))),
				)))
				info, err := ddl.BuildTableInfoWithStmt(ctx, stmt.(*ast.CreateTableStmt), "utf8mb4", "utf8mb4_bin", nil)
				require.NoError(t, err)
				return info
			}
			const firstSQL = "CREATE TABLE t1 (id INT PRIMARY KEY)"
			first := buildTableInfo(firstSQL, tc.firstMode)
			second := buildTableInfo(tc.secondSQL, tc.secondMode)
			if tc.timestampInUTC {
				// A +08:00 upstream session stores this default in UTC. Its
				// unrelated conversion must not prevent resolving the comment.
				require.NoError(t, second.Columns[1].SetDefaultValue("2019-12-31 16:00:00"))
			}
			switch tc.name {
			case "escaped default and comments":
				require.Equal(t, `\n`, second.Columns[1].GetDefaultValue())
				require.Equal(t, `\r`, second.Comment)
			case "backslash escapes after no backslash escapes":
				require.Equal(t, "\n", second.Columns[1].GetDefaultValue())
			}

			// Reproduce TiDB's merge: only the first job's mode survives.
			job := &model.Job{
				Type: model.ActionCreateTables, SchemaID: 1, SQLMode: tc.firstMode,
				Query:      firstSQL + "; " + tc.secondSQL + ";",
				BinlogInfo: &model.HistoryInfo{MultipleTableInfos: []*model.TableInfo{first, second}},
			}
			raw, err := buildPersistedDDLEventForCreateTables(buildPersistedDDLEventFuncArgs{
				job:         job,
				databaseMap: map[int64]*BasicDatabaseInfo{job.SchemaID: {Name: "test"}},
			})
			require.NoError(t, err)
			require.Equal(t, []string{firstSQL, tc.secondSQL}, raw.OriginalQueries)
			event, ok, err := buildDDLEvent(&raw, nil, common.DDLSpanTableID)
			require.NoError(t, err)
			require.True(t, ok)
			queries, err := commonEvent.SplitQueries(event.Query)
			require.NoError(t, err)
			require.Len(t, queries, 2)

			for i, query := range queries {
				actual := buildTableInfo(query, mysql.ModeNone)
				expected := job.BinlogInfo.MultipleTableInfos[i]
				require.Equal(t, expected.Comment, actual.Comment)
				require.Len(t, actual.Columns, len(expected.Columns))
				for j, column := range actual.Columns {
					if tc.timestampInUTC && column.GetType() == mysql.TypeTimestamp {
						require.Equal(t, "2020-01-01 00:00:00", column.GetDefaultValue())
					} else {
						require.Equal(t, expected.Columns[j].GetDefaultValue(), column.GetDefaultValue())
					}
					require.Equal(t, expected.Columns[j].Comment, column.Comment)
					require.Equal(t, expected.Columns[j].GetElems(), column.GetElems())
					require.Equal(t, expected.Columns[j].GeneratedExprString, column.GeneratedExprString)
				}
				require.Len(t, actual.Indices, len(expected.Indices))
				for j, index := range actual.Indices {
					require.Equal(t, expected.Indices[j].Comment, index.Comment)
				}
				if expected.Partition != nil {
					require.NotNil(t, actual.Partition)
					for j, partition := range actual.Partition.Definitions {
						require.Equal(t, expected.Partition.Definitions[j].LessThan, partition.LessThan)
						require.Equal(t, expected.Partition.Definitions[j].Comment, partition.Comment)
					}
				}
			}
		})
	}
}

func TestCreateTablesManyQuotedSemicolons(t *testing.T) {
	query := "CREATE TABLE t (s TEXT DEFAULT '" + strings.Repeat(";", 50000) + "')"
	job := buildCreateTablesJobForTest(1, []int64{100, 101}, []string{"t", "t2"}, 20)
	job.Query = query + "; CREATE TABLE t2 (id INT PRIMARY KEY);"
	raw, err := buildPersistedDDLEventForCreateTables(buildPersistedDDLEventFuncArgs{
		job: job, databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{query, "CREATE TABLE t2 (id INT PRIMARY KEY)"}, raw.OriginalQueries)
}

func TestCreateTablesGBKQuery(t *testing.T) {
	// 0x5c is part of a GBK character, not an escape for the closing quote.
	query := "CREATE TABLE t (s VARCHAR(8) DEFAULT '\x81\x5c')"
	job := buildCreateTablesJobForTest(1, []int64{100, 101}, []string{"t", "t2"}, 20)
	job.Query = query + "; CREATE TABLE t2 (id INT PRIMARY KEY);"
	job.Charset, job.Collate = "gbk", "gbk_bin"
	raw, err := buildPersistedDDLEventForCreateTables(buildPersistedDDLEventFuncArgs{
		job: job, databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
	})
	require.NoError(t, err)
	require.Equal(t, []string{query, "CREATE TABLE t2 (id INT PRIMARY KEY)"}, raw.OriginalQueries)
}

func BenchmarkParseCreateTableQueries(b *testing.B) {
	for _, count := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			job := &model.Job{Query: "CREATE TABLE t (s TEXT DEFAULT '" + strings.Repeat(";", count) + "'); CREATE TABLE t2(id INT);"}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				_, err := parseCreateTableQueries(job)
				require.NoError(b, err)
			}
		})
	}
}
