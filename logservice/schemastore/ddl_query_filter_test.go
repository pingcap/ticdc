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
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

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

func TestDDLIgnoreSQLForRewrittenQueries(t *testing.T) {
	rename := buildRenameTableJobForTest(1, 100, "t_new", 20, &model.InvolvingSchemaInfo{
		Database: "test", Table: "t",
	})
	rename.Query = "ALTER TABLE t RENAME TO t_new"
	renames := buildRenameTablesJobForTest(
		[]int64{1, 1}, []int64{1, 1}, []int64{100, 101},
		[]string{"test", "test"}, []string{"t", "t2"}, []string{"t_new", "t2_new"}, 20)
	renames.Query = "RENAME TABLE t TO t_new, t2 TO t2_new"
	creates := buildCreateTablesJobForTest(1, []int64{100, 101}, []string{"t", "t2"}, 20)
	drop := buildDropTableJobForTest(1, 100, 20)
	drop.Query = "DROP TABLE t, t2"
	dropView := buildDropViewJobForTest(1, 20)
	dropView.TableName, dropView.Query = "t", "DROP VIEW t, t2"
	exchange := buildExchangePartitionJobForTest(1, 100, 101, "t2", []int64{100}, 20)
	exchange.Query = "ALTER TABLE t2 EXCHANGE PARTITION p0 WITH TABLE t"

	for _, tc := range []struct {
		name          string
		job           *model.Job
		pattern       string
		expectedQuery string
		notSync       bool
	}{
		{
			name: "rename table", job: rename,
			pattern:       "^ALTER TABLE t RENAME",
			expectedQuery: "RENAME TABLE `test`.`t` TO `test`.`t_new`", notSync: true,
		},
		{
			name: "rename tables", job: renames,
			pattern:       "^RENAME TABLE `test`.`t` TO `test`.`t_new`$",
			expectedQuery: "RENAME TABLE `test`.`t2` TO `test`.`t2_new`;",
		},
		{
			name: "create tables", job: creates,
			pattern:       "^CREATE TABLE `t` ",
			expectedQuery: "CREATE TABLE `t2` (`a` INT PRIMARY KEY);",
		},
		{
			name: "drop table", job: drop,
			pattern:       "^DROP TABLE `test`.`t`$",
			expectedQuery: "DROP TABLE `test`.`t`", notSync: true,
		},
		{
			name: "drop view", job: dropView,
			pattern:       "^DROP VIEW `test`.`t`$",
			expectedQuery: "DROP VIEW `test`.`t`", notSync: true,
		},
		{
			name: "exchange partition", job: exchange,
			pattern:       "^ALTER TABLE `test`.`t2` EXCHANGE PARTITION `p0` WITH TABLE `test`.`t`$",
			expectedQuery: "ALTER TABLE `test`.`t2` EXCHANGE PARTITION `p0` WITH TABLE `test`.`t`", notSync: true,
		},
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
			if tc.job.Type == model.ActionExchangeTablePartition {
				raw.ExtraTableInfo = common.WrapTableInfo("test", newEligibleTableInfoForTest(100, "t"))
			}
			f, err := filter.NewFilter(&config.FilterConfig{
				Rules: []string{"test.*"},
				EventFilters: []*config.EventFilterRule{{
					Matcher:   []string{"test.*"},
					IgnoreSQL: []string{tc.pattern},
				}},
			}, "UTC", false, false)
			require.NoError(t, err)
			event, ok, err := buildDDLEvent(&raw, f, common.DDLSpanTableID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.notSync, event.NotSync)
			require.Equal(t, tc.expectedQuery, event.Query)
			if tc.job.Type == model.ActionRenameTables || tc.job.Type == model.ActionCreateTables {
				require.Len(t, event.MultipleTableInfos, 1)
				require.Equal(t, int64(101), event.MultipleTableInfos[0].TableName.TableID)
			}
		})
	}
}

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
