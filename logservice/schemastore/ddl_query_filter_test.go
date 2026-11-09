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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemastore

import (
	"bytes"
	"regexp"
	"strings"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

// TestDDLIgnoreSQLUsesOriginalQuery verifies matching before SQL normalization and after persistence.
func TestDDLIgnoreSQLUsesOriginalQuery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		query   string
		pattern string
		ignore  bool
	}{
		{"unquoted identifiers", "ALTER TABLE t ADD COLUMN c INT", "^ALTER TABLE t ADD COLUMN", true},
		{"quoted pattern", "ALTER TABLE t ADD COLUMN c INT", "^ALTER TABLE `t` ADD COLUMN", false},
		{"optional keyword", "ALTER TABLE t ADD c INT", "add column", false},
		{"whitespace", "ALTER   TABLE t ADD COLUMN c INT", "^ALTER TABLE", false},
		{"case insensitive", "alter table t add column c int", "^ALTER TABLE t ADD COLUMN", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := buildCreateTableJobForTest(1, 100, "t", 20)
			job.Type = model.ActionAddColumn
			job.Query = tc.query
			raw, err := buildPersistedDDLEventForNormalDDLOnSingleTable(buildPersistedDDLEventFuncArgs{
				job:         job,
				databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
				tableMap:    map[int64]*BasicTableInfo{100: {SchemaID: 1, Name: "t"}},
			})
			require.NoError(t, err)

			storage := newPersistentStorageForTest(t.TempDir(), nil)
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			require.NoError(t, writePersistedDDLEvent(storage.db, &raw))
			snap := storage.db.NewSnapshot()
			defer snap.Close()
			replayed := readPersistedDDLEvent(snap, raw.FinishedTs)
			require.Equal(t, job.Query, replayed.OriginalQuery)

			f, err := filter.NewFilter(&config.FilterConfig{
				Rules: []string{"test.t"},
				EventFilters: []*config.EventFilterRule{{
					Matcher:   []string{"test.t"},
					IgnoreSQL: []string{tc.pattern},
				}},
			}, "UTC", false, false)
			require.NoError(t, err)
			event, ok, err := buildDDLEvent(&replayed, f, common.DDLSpanTableID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.ignore, event.NotSync)
			require.Equal(t, "ALTER TABLE `t` ADD COLUMN `c` INT", event.Query)
		})
	}
}

// TestDDLIgnoreSQLForRewrittenQueries checks upstream matching when execution SQL is rebuilt.
func TestDDLIgnoreSQLForRewrittenQueries(t *testing.T) {
	rename := buildRenameTableJobForTest(1, 100, "t_new", 20, &model.InvolvingSchemaInfo{
		Database: "test", Table: "t",
	})
	rename.Query = "ALTER TABLE t RENAME TO t_new"
	drop := buildDropTableJobForTest(1, 100, 20)
	drop.Query = "DROP TABLE t, t2"
	dropView := buildDropViewJobForTest(1, 20)
	dropView.TableName, dropView.Query = "t", "DROP VIEW t, t2"
	exchange := buildExchangePartitionJobForTest(1, 100, 101, "t2", []int64{100}, 20)
	exchange.Query = "ALTER TABLE t2 EXCHANGE PARTITION p0 WITH TABLE t"

	for _, tc := range []struct {
		name          string
		job           *model.Job
		expectedQuery string
	}{
		{"rename table", rename, "RENAME TABLE `test`.`t` TO `test`.`t_new`"},
		{"drop table", drop, "DROP TABLE `test`.`t`"},
		{"drop view", dropView, "DROP VIEW `test`.`t`"},
		{"exchange partition", exchange, "ALTER TABLE `test`.`t2` EXCHANGE PARTITION `p0` WITH TABLE `test`.`t`"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := allDDLHandlers[tc.job.Type].buildPersistedDDLEventFunc(buildPersistedDDLEventFuncArgs{
				job:         tc.job,
				databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
				tableMap: map[int64]*BasicTableInfo{
					100: {SchemaID: 1, Name: "t"},
					101: {SchemaID: 1, Name: "t2"},
				},
				partitionMap: map[int64]BasicPartitionInfo{101: {102: nil}},
			})
			require.NoError(t, err)
			require.Equal(t, tc.job.Query, raw.OriginalQuery)
			require.NotEqual(t, raw.OriginalQuery, raw.Query)
			if tc.job.Type == model.ActionExchangeTablePartition {
				raw.ExtraTableInfo = common.WrapTableInfo("test", newEligibleTableInfoForTest(100, "t"))
			}
			for _, match := range []struct {
				name   string
				query  string
				ignore bool
			}{
				{"upstream SQL", tc.job.Query, true},
				{"rewritten SQL", raw.Query, false},
			} {
				t.Run(match.name, func(t *testing.T) {
					f, err := filter.NewFilter(&config.FilterConfig{
						Rules: []string{"test.*"},
						EventFilters: []*config.EventFilterRule{{
							Matcher:   []string{"test.*"},
							IgnoreSQL: []string{"^" + regexp.QuoteMeta(match.query) + "$"},
						}},
					}, "UTC", false, false)
					require.NoError(t, err)
					event, ok, err := buildDDLEvent(&raw, f, common.DDLSpanTableID)
					require.NoError(t, err)
					require.True(t, ok)
					require.Equal(t, match.ignore, event.NotSync)
					require.Equal(t, tc.expectedQuery, event.Query)
				})
			}
		})
	}
}

// TestDDLIgnoreSQLForBatchedQueries checks upstream multi-table rename SQL with per-table matching.
func TestDDLIgnoreSQLForBatchedQueries(t *testing.T) {
	jobs := []*model.Job{
		buildRenameTablesJobForTest(
			[]int64{1, 1}, []int64{1, 1}, []int64{100, 101},
			[]string{"test", "test"}, []string{"t", "t2"}, []string{"t_new", "t2_new"}, 20),
	}
	jobs[0].Query = "RENAME   TABLE t TO t_new, t2 TO t2_new"
	for _, job := range jobs {
		t.Run(job.Type.String(), func(t *testing.T) {
			raw, err := allDDLHandlers[job.Type].buildPersistedDDLEventFunc(buildPersistedDDLEventFuncArgs{
				job:         job,
				databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
				tableMap: map[int64]*BasicTableInfo{
					100: {SchemaID: 1, Name: "t"},
					101: {SchemaID: 1, Name: "t2"},
				},
			})
			require.NoError(t, err)
			require.Equal(t, job.Query, raw.OriginalQuery)
			require.NotEqual(t, raw.OriginalQuery, raw.Query)
			queries, err := commonEvent.SplitQueries(raw.Query)
			require.NoError(t, err)
			require.Len(t, queries, 2)
			firstPattern := "^" + regexp.QuoteMeta(strings.TrimSuffix(queries[0], ";")) + "$"
			secondPattern := "^" + regexp.QuoteMeta(strings.TrimSuffix(queries[1], ";")) + "$"
			batchPattern := "^" + regexp.QuoteMeta(job.Query) + "$"
			for _, tc := range []struct {
				name    string
				matcher string
				pattern string
				kept    []int
			}{
				{"no match", "test.*", "^UNRELATED$", []int{0, 1}},
				{"upstream batch", "test.*", batchPattern, nil},
				{"upstream batch scoped to first table", "test.t", batchPattern, []int{1}},
				{"upstream batch scoped to second table", "test.t2", batchPattern, []int{0}},
				{"upstream batch outside matcher", "test.other", batchPattern, []int{0, 1}},
				{"rewritten batch", "test.*", "^" + regexp.QuoteMeta(raw.Query) + "$", []int{0, 1}},
				{"first rewritten statement", "test.*", firstPattern, []int{0, 1}},
				{"second rewritten statement", "test.*", secondPattern, []int{0, 1}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					f, err := filter.NewFilter(&config.FilterConfig{
						Rules: []string{"test.*"},
						EventFilters: []*config.EventFilterRule{{
							Matcher:   []string{tc.matcher},
							IgnoreSQL: []string{tc.pattern},
						}},
					}, "UTC", false, false)
					require.NoError(t, err)
					event, ok, err := buildDDLEvent(&raw, f, common.DDLSpanTableID)
					require.NoError(t, err)
					require.True(t, ok)
					require.False(t, event.NotSync)
					var expectedQueries []string
					require.Len(t, event.MultipleTableInfos, len(tc.kept))
					for i, index := range tc.kept {
						expectedQueries = append(expectedQueries, queries[index])
						require.Equal(t, int64(100+index), event.MultipleTableInfos[i].TableName.TableID)
						require.Equal(t, []string{"t", "t2"}[index], event.TableNameChange.DropName[i].TableName)
					}
					require.Equal(t, strings.Join(expectedQueries, ""), event.Query)
					// Ignored SQL must still carry the complete table lifecycle changes.
					require.Len(t, event.TableNameChange.AddName, 2)
					require.ElementsMatch(t, []int64{common.DDLSpanTableID, 100, 101}, event.BlockedTables.TableIDs)
					require.Len(t, event.TableNameChange.DropName, 2)
				})
			}
		})
	}
}

// TestDDLIgnoreSQLForCreateTables checks each upstream CREATE independently after persistence.
func TestDDLIgnoreSQLForCreateTables(t *testing.T) {
	const firstQuery = "create  table t(a int primary key)"
	const secondQuery = "CREATE TABLE t2 (a INT PRIMARY KEY)"
	for _, tc := range []struct {
		name    string
		queries []string
		pattern string
		sqlMode mysql.SQLMode
		kept    []int
	}{
		{"no match", []string{firstQuery, secondQuery}, "^UNRELATED$", 0, []int{0, 1}},
		{"all statements", []string{firstQuery, secondQuery}, "^CREATE", 0, nil},
		{"rewritten statement", []string{firstQuery, secondQuery}, "^CREATE TABLE `t`", 0, []int{0, 1}},
		{"anchored first statement", []string{firstQuery, secondQuery}, "^" + regexp.QuoteMeta(firstQuery) + "$", 0, []int{1}},
		{"prefix does not ignore next table", []string{firstQuery, secondQuery}, "^create  table t\\(", 0, []int{1}},
		{"anchored second statement", []string{firstQuery, secondQuery}, "^" + regexp.QuoteMeta(secondQuery) + "$", 0, []int{0}},
		{"semicolon in string and comment", []string{
			"create table t(a int primary key, s varchar(8) default ';') /* original; comment */", secondQuery,
		}, "^create table t\\(.*default ';'\\) /\\* original; comment \\*/$", 0, []int{1}},
		{"ANSI quotes", []string{
			`CREATE TABLE "t" (a INT PRIMARY KEY)`, `CREATE TABLE "t2" (a INT PRIMARY KEY)`,
		}, `^CREATE TABLE "t2" \(a INT PRIMARY KEY\)$`, mysql.ModeANSIQuotes, []int{0}},
		{"mixed ANSI quotes and quoted string", []string{
			`CREATE TABLE "t" (a INT PRIMARY KEY)`, `CREATE TABLE t2 (a INT PRIMARY KEY, s VARCHAR(8) DEFAULT "kept")`,
		}, `^CREATE TABLE t2 .* DEFAULT "kept"\)$`, mysql.ModeANSIQuotes, []int{0}},
		{"mixed quoted string and ANSI quotes", []string{
			`CREATE TABLE t (a INT PRIMARY KEY, s VARCHAR(8) DEFAULT "kept")`, `CREATE TABLE "t2" (a INT PRIMARY KEY)`,
		}, `^CREATE TABLE "t2" \(a INT PRIMARY KEY\)$`, 0, []int{0}},
		{"mixed backslash modes", []string{
			`CREATE TABLE t (a INT PRIMARY KEY, s VARCHAR(20) DEFAULT '\')`,
			`CREATE TABLE t2 (a INT PRIMARY KEY, s VARCHAR(20) DEFAULT 'it\'s; kept')`,
		}, "^" + regexp.QuoteMeta(`CREATE TABLE t2 (a INT PRIMARY KEY, s VARCHAR(20) DEFAULT 'it\'s; kept')`) + "$", mysql.ModeNoBackslashEscapes, []int{0}},
		{"mixed ANSI and no backslash modes", []string{
			`CREATE TABLE "t" (a INT PRIMARY KEY)`, `CREATE TABLE t2 (a INT PRIMARY KEY, s VARCHAR(8) DEFAULT "\")`,
		}, "^" + regexp.QuoteMeta(`CREATE TABLE t2 (a INT PRIMARY KEY, s VARCHAR(8) DEFAULT "\")`) + "$", mysql.ModeANSIQuotes, []int{0}},
		{"semicolon in line comment", []string{
			"CREATE TABLE t(a INT PRIMARY KEY) -- original; comment\n COMMENT='source'", secondQuery,
		}, "^" + regexp.QuoteMeta("CREATE TABLE t(a INT PRIMARY KEY) -- original; comment\n COMMENT='source'") + "$", 0, []int{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := buildCreateTablesJobWithQueryForTest(1, []int64{100, 101}, []string{"t", "t2"}, tc.queries, 20)
			// TiDB separates batched jobs with a semicolon and a space.
			job.Query = strings.Join(tc.queries, "; ") + ";"
			job.SQLMode = tc.sqlMode
			raw, err := buildPersistedDDLEventForCreateTables(buildPersistedDDLEventFuncArgs{
				job:         job,
				databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
			})
			require.NoError(t, err)
			require.Equal(t, tc.queries, raw.OriginalQueries)
			storage := newPersistentStorageForTest(t.TempDir(), nil)
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			require.NoError(t, writePersistedDDLEvent(storage.db, &raw))
			snap := storage.db.NewSnapshot()
			defer snap.Close()
			replayed := readPersistedDDLEvent(snap, raw.FinishedTs)
			require.Equal(t, job.Query, replayed.OriginalQuery)
			require.Equal(t, tc.queries, replayed.OriginalQueries)
			f, err := filter.NewFilter(&config.FilterConfig{
				Rules: []string{"test.*"},
				EventFilters: []*config.EventFilterRule{{
					Matcher:   []string{"test.*"},
					IgnoreSQL: []string{tc.pattern},
				}},
			}, "UTC", false, false)
			require.NoError(t, err)
			event, ok, err := buildDDLEvent(&replayed, f, common.DDLSpanTableID)
			require.NoError(t, err)
			require.True(t, ok)
			require.False(t, event.NotSync)
			queries, err := commonEvent.SplitQueries(raw.Query)
			require.NoError(t, err)
			require.Len(t, event.MultipleTableInfos, len(tc.kept))
			var expectedQueries []string
			for i, index := range tc.kept {
				require.Equal(t, int64(100+index), event.MultipleTableInfos[i].TableName.TableID)
				expectedQueries = append(expectedQueries, queries[index])
			}
			require.Equal(t, strings.Join(expectedQueries, ""), event.Query)
			require.Len(t, event.NeedAddedTables, 2)
			require.Len(t, event.TableNameChange.AddName, 2)
		})
	}
}

// TestCreateTablesInvalidQueries verifies parse errors and mismatched statement counts are rejected.
func TestCreateTablesInvalidQueries(t *testing.T) {
	for _, query := range []string{
		"CREATE TABLE t(a INT PRIMARY KEY); CREATE TABLE t2(a INT PRIMARY KEY;",
		"CREATE TABLE t(a INT PRIMARY KEY);",
	} {
		job := buildCreateTablesJobForTest(1, []int64{100, 101}, []string{"t", "t2"}, 20)
		job.Query = query
		_, err := buildPersistedDDLEventForCreateTables(buildPersistedDDLEventFuncArgs{
			job:         job,
			databaseMap: map[int64]*BasicDatabaseInfo{1: {Name: "test"}},
		})
		require.Error(t, err)
		code, ok := cerror.RFCCode(err)
		require.True(t, ok)
		require.Equal(t, cerror.ErrTiDBUnexpectedJobMeta.RFCCode(), code)
	}
}

// TestDDLIgnoreSQLLegacyPersistedQuery verifies the fallback for records without original_query.
func TestDDLIgnoreSQLLegacyPersistedQuery(t *testing.T) {
	const query = "ALTER TABLE `t` ADD COLUMN `c` INT"
	// Older records have a normalized query but no original_query field.
	payload := msgp.AppendMapHeader(nil, 1)
	payload = msgp.AppendString(payload, "query")
	payload = msgp.AppendString(payload, query)
	var raw, decoded PersistedDDLEvent
	rest, err := raw.UnmarshalMsg(payload)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Empty(t, raw.OriginalQuery)
	require.NoError(t, decoded.DecodeMsg(msgp.NewReader(bytes.NewReader(payload))))
	require.Equal(t, raw, decoded)

	raw.Type = byte(model.ActionAddColumn)
	raw.SchemaName, raw.TableName = "test", "t"
	f, err := filter.NewFilter(&config.FilterConfig{
		Rules: []string{"test.t"},
		EventFilters: []*config.EventFilterRule{{
			Matcher:   []string{"test.t"},
			IgnoreSQL: []string{"^ALTER TABLE `t` ADD COLUMN"},
		}},
	}, "UTC", false, false)
	require.NoError(t, err)
	event, ok, err := buildDDLEventCommon(&raw, f, WithoutTiDBOnly)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, event.NotSync)
	require.Equal(t, query, event.Query)
}
