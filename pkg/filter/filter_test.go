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

package filter

import (
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

// Helper to create a model.ColumnInfo
func newColumnInfo(id int64, name string, tp byte, flag uint) *model.ColumnInfo {
	ft := types.NewFieldType(tp)
	ft.AddFlag(flag)
	return &model.ColumnInfo{
		ID:        id,
		Name:      ast.NewCIStr(name),
		FieldType: *ft,
		State:     model.StatePublic,
		Version:   model.CurrLatestColumnInfoVersion,
	}
}

// Helper to create a common.TableInfo for testing
func mustNewCommonTableInfo(schema, table string, cols []*model.ColumnInfo, indices []*model.IndexInfo) *common.TableInfo {
	ti := &model.TableInfo{
		ID:      time.Now().UnixNano(),
		Name:    ast.NewCIStr(table),
		Columns: cols,
		Indices: indices,
		State:   model.StatePublic,
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
	}

	for i, col := range ti.Columns {
		col.Offset = i
	}

	for _, col := range ti.Columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			ti.PKIsHandle = true
			break
		}
	}
	return common.WrapTableInfo(schema, ti)
}

// mustNewModelTableInfo is a helper to create a model.TableInfo for testing.
func mustNewModelTableInfo(table string, cols []*model.ColumnInfo, indices []*model.IndexInfo) *model.TableInfo {
	ti := &model.TableInfo{
		ID:      time.Now().UnixNano(),
		Name:    ast.NewCIStr(table),
		Columns: cols,
		Indices: indices,
		State:   model.StatePublic,
		Charset: "utf8mb4",
		Collate: "utf8mb4_bin",
	}

	for i, col := range ti.Columns {
		col.Offset = i
	}

	for _, col := range ti.Columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			ti.PKIsHandle = true
			break
		}
	}
	return ti
}

// Helper to create a model.IndexInfo
func newIndexInfo(name string, cols []*model.IndexColumn, isPrimary, isUnique bool) *model.IndexInfo {
	return &model.IndexInfo{
		Name:    ast.NewCIStr(name),
		Columns: cols,
		Primary: isPrimary,
		Unique:  isUnique,
		State:   model.StatePublic,
	}
}

// datumsToChunkRow converts datums to a chunk.Row for testing.
// Note: This is a simplified helper for testing purposes. A real implementation might exist in a helper package.
func datumsToChunkRow(datums []types.Datum, ti *common.TableInfo) chunk.Row {
	fieldTypes := make([]*types.FieldType, len(ti.GetColumns()))
	for i, col := range ti.GetColumns() {
		fieldTypes[i] = &col.FieldType
	}
	chk := chunk.NewChunkWithCapacity(fieldTypes, 1)
	for i, d := range datums {
		chk.AppendDatum(i, &d)
	}
	return chk.GetRow(0)
}

func TestShouldUseDefaultRules(t *testing.T) {
	t.Parallel()

	filter, err := NewFilter(config.NewDefaultFilterConfig(), "", false, false)
	require.Nil(t, err)
	require.True(t, filter.ShouldIgnoreTable("information_schema", "", nil))
	require.True(t, filter.ShouldIgnoreTable("information_schema", "statistics", nil))
	require.True(t, filter.ShouldIgnoreTable("performance_schema", "", nil))
	require.True(t, filter.ShouldIgnoreTable(TiDBWorkloadSchema, "hist_snapshots", nil))
	require.False(t, filter.ShouldIgnoreTable("metric_schema", "query_duration", nil))
	require.False(t, filter.ShouldIgnoreTable("sns", "user", nil))
	require.True(t, filter.ShouldIgnoreTable("tidb_cdc", "repl_mark_a_a", nil))
	require.True(t, filter.ShouldIgnoreTable("lightning_task_info", "conflict_records", nil))
}

func TestShouldUseCustomRules(t *testing.T) {
	t.Parallel()

	filter, err := NewFilter(&config.FilterConfig{
		Rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
	}, "", false, false)
	require.Nil(t, err)
	require.True(t, filter.ShouldIgnoreTable("other", "", nil))
	require.True(t, filter.ShouldIgnoreTable("other", "what", nil))
	require.False(t, filter.ShouldIgnoreTable("sns", "", nil))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order", nil))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order", nil))
	require.True(t, filter.ShouldIgnoreTable("ecom", "test", nil))
	require.True(t, filter.ShouldIgnoreTable("sns", "log", nil))
	require.True(t, filter.ShouldIgnoreTable("information_schema", "", nil))

	filter, err = NewFilter(&config.FilterConfig{
		// 1. match all schema and table
		// 2. do not match test.season
		// 3. match all table of schema school
		// 4. do not match table school.teacher
		Rules: []string{"*.*", "!test.season", "school.*", "!school.teacher"},
	}, "", false, false)
	require.True(t, filter.ShouldIgnoreTable("test", "season", nil))
	require.False(t, filter.ShouldIgnoreTable("other", "", nil))
	require.False(t, filter.ShouldIgnoreTable("school", "student", nil))
	require.True(t, filter.ShouldIgnoreTable("school", "teacher", nil))
	require.Nil(t, err)

	filter, err = NewFilter(&config.FilterConfig{
		// 1. match all schema and table
		// 2. do not match test.season
		// 3. match all table of schema school
		// 4. do not match table school.teacher
		Rules: []string{"*.*", "!test.t1", "!test.t2"},
	}, "", false, false)
	require.False(t, filter.ShouldIgnoreTable("test", "season", nil))
	require.True(t, filter.ShouldIgnoreTable("test", "t1", nil))
	require.True(t, filter.ShouldIgnoreTable("test", "t2", nil))
	require.Nil(t, err)
}

func TestShouldIgnoreTable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		rules     []string
		schema    string
		table     string
		shouldBeA bool // A stands for accept
	}{
		{
			name: "Replicate all tables",
			rules: []string{
				"*.*",
			},
			schema: "test", table: "t1", shouldBeA: true,
		},
		{
			name: "Replicate all tables in test1",
			rules: []string{
				"test1.*",
			},
			schema: "test1", table: "t1", shouldBeA: true,
		},
		{
			name: "Replicate all tables in test1, but input is test2",
			rules: []string{
				"test1.*",
			},
			schema: "test2", table: "t1", shouldBeA: false,
		},
		{
			name: "Replicate all tables except scm1.tbl2",
			rules: []string{
				"*.*",
				"!scm1.tbl2",
			},
			schema: "scm1", table: "tbl2", shouldBeA: false,
		},
		{
			name: "Replicate all tables except scm1.tbl2, but input is scm1.tbl1",
			rules: []string{
				"*.*",
				"!scm1.tbl2",
			},
			schema: "scm1", table: "tbl1", shouldBeA: true,
		},
		{
			name: "Only replicate tables scm1.tbl2 and scm1.tbl3",
			rules: []string{
				"scm1.tbl2",
				"scm1.tbl3",
			},
			schema: "scm1", table: "tbl2", shouldBeA: true,
		},
		{
			name: "Only replicate tables scm1.tbl2 and scm1.tbl3, but input is scm1.tbl1",
			rules: []string{
				"scm1.tbl2",
				"scm1.tbl3",
			},
			schema: "scm1", table: "tbl1", shouldBeA: false,
		},
		{
			name: "Replicate tables whose names start with tidb_ in scm1",
			rules: []string{
				"scm1.tidb_*",
			},
			schema: "scm1", table: "tidb_t1", shouldBeA: true,
		},
		{
			name: "Replicate tables whose names start with tidb_ in scm1, but input is scm1.t1",
			rules: []string{
				"scm1.tidb_*",
			},
			schema: "scm1", table: "t1", shouldBeA: false,
		},
		{
			name:   "Ignore system schemas",
			rules:  []string{"*.*"},
			schema: "mysql", table: "users", shouldBeA: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &config.FilterConfig{
				Rules: c.rules,
			}
			f, err := NewFilter(cfg, "UTC", false, false)
			require.NoError(t, err)
			require.Equal(t, !c.shouldBeA, f.ShouldIgnoreTable(c.schema, c.table, nil))
		})
	}

	// Test case insensitive
	cfg := &config.FilterConfig{Rules: []string{"TEST.*"}}
	f, err := NewFilter(cfg, "UTC", false, false) // case-insensitive
	require.NoError(t, err)
	require.False(t, f.ShouldIgnoreTable("test", "t1", nil))
	require.False(t, f.ShouldIgnoreTable("Test", "t1", nil))

	f, err = NewFilter(cfg, "UTC", true, false) // case-sensitive
	require.NoError(t, err)
	require.True(t, f.ShouldIgnoreTable("test", "t1", nil))
	require.False(t, f.ShouldIgnoreTable("TEST", "t1", nil))
}

func TestShouldDiscardDDL(t *testing.T) {
	t.Parallel()

	cfg := &config.FilterConfig{
		Rules: []string{"test.*"},
	}

	f, err := NewFilter(cfg, "UTC", false, false)
	require.NoError(t, err)

	// DDL not in whitelist, should discard.
	require.True(t, f.ShouldDiscardDDL("test", "t", model.ActionAlterCacheTable, nil))

	// DDL for a table that doesn't match the rule, should discard.
	require.True(t, f.ShouldDiscardDDL("otherdb", "t1", model.ActionCreateTable, nil))

	// DDL (create table) does not match any discard rule, should not discard.
	require.False(t, f.ShouldDiscardDDL("test", "t1", model.ActionCreateTable, nil))

	// DDL on system schema, should discard.
	require.True(t, f.ShouldDiscardDDL("mysql", "t1", model.ActionCreateTable, nil))
}

func TestShouldIgnoreDDL(t *testing.T) {
	t.Parallel()

	cfg := &config.FilterConfig{
		Rules:            []string{"test.*"},
		IgnoreTxnStartTs: []uint64{100},
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.t1"},
				IgnoreEvent: []bf.EventType{bf.DropTable},
			},
			{
				Matcher:   []string{"test.t2"},
				IgnoreSQL: []string{"^DROP"},
			},
		},
	}

	f, err := NewFilter(cfg, "UTC", false, false)
	require.NoError(t, err)

	// DDL matches an event filter rule (drop table), should ignore.
	ignore, err := f.ShouldIgnoreDDL("test", "t1", "DROP TABLE t1", model.ActionDropTable, nil, 0)
	require.NoError(t, err)
	require.True(t, ignore)

	// DDL (create table) does not match event filter, should not ignore.
	ignore, err = f.ShouldIgnoreDDL("test", "t1", "CREATE TABLE t1(id int)", model.ActionCreateTable, nil, 0)
	require.NoError(t, err)
	require.False(t, ignore)

	// DDL (create table) matches IgnoreTxnStartTs, should ignore.
	ignore, err = f.ShouldIgnoreDDL("test", "t1", "CREATE TABLE t1(id int)", model.ActionCreateTable, nil, 100)
	require.NoError(t, err)
	require.True(t, ignore)

	// DDL matches an SQL regex rule, should ignore.
	ignore, err = f.ShouldIgnoreDDL("test", "t2", "DROP TABLE t2", model.ActionDropTable, nil, 0)
	require.NoError(t, err)
	require.True(t, ignore)

	// DDL does not match any rule, should not ignore.
	ignore, err = f.ShouldIgnoreDDL("test", "t3", "CREATE TABLE t3(id int)", model.ActionCreateTable, nil, 0)
	require.NoError(t, err)
	require.False(t, ignore)
}

func TestShouldIgnoreDML(t *testing.T) {
	t.Parallel()
	cfg := &config.FilterConfig{
		// Rules allow test.t1, test.t2, test.t3
		Rules:            []string{"test.*", "!test.t4"},
		IgnoreTxnStartTs: []uint64{100},
		EventFilters: []*config.EventFilterRule{
			{ // Rule 1: ignore insert on t2
				Matcher:     []string{"test.t2"},
				IgnoreEvent: []bf.EventType{bf.InsertEvent},
			},
			{ // Rule 2: ignore delete on t3
				Matcher:     []string{"test.t3"},
				IgnoreEvent: []bf.EventType{bf.DeleteEvent},
			},
		},
	}
	f, err := NewFilter(cfg, "UTC", false, false)
	require.NoError(t, err)

	ti1 := mustNewCommonTableInfo("test", "t1", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)
	ti2 := mustNewCommonTableInfo("test", "t2", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)
	ti3 := mustNewCommonTableInfo("test", "T3", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)
	ti4 := mustNewCommonTableInfo("test", "t4", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)

	// Dummy row data for cases where it doesn't affect logic but is required by function signature.
	dummyRowData := datumsToChunkRow(types.MakeDatums(int64(1)), ti1)
	emptyRow := chunk.Row{}

	// Case 1: DML on `test.t4`, should be ignored by table filter (highest precedence).
	ignore, err := f.ShouldIgnoreDML(common.RowTypeInsert, emptyRow, dummyRowData, ti4, 0)
	require.NoError(t, err)
	require.True(t, ignore, "Should be ignored by table filter")

	// Case 2: INSERT on `test.t2`, should be ignored by SQL event filter (second precedence).
	ignore, err = f.ShouldIgnoreDML(common.RowTypeInsert, emptyRow, dummyRowData, ti2, 0)
	require.NoError(t, err)
	require.True(t, ignore, "Should be ignored by SQL event filter")

	// Case 3: UPDATE on `test.t2`, should pass SQL event filter.
	updatePreRow := datumsToChunkRow(types.MakeDatums(int64(1)), ti2)
	updateRow := datumsToChunkRow(types.MakeDatums(int64(2)), ti2)
	ignore, err = f.ShouldIgnoreDML(common.RowTypeUpdate, updatePreRow, updateRow, ti2, 0)
	require.NoError(t, err)
	require.False(t, ignore, "Should pass SQL event filter as no expression is configured")

	// Case 4: DML on `test.t1`, should pass all filters.
	insertRow := datumsToChunkRow(types.MakeDatums(int64(1)), ti1)
	ignore, err = f.ShouldIgnoreDML(common.RowTypeInsert, emptyRow, insertRow, ti1, 0)
	require.NoError(t, err)
	require.False(t, ignore, "Should pass all filters")

	// Case 5: DELETE on `test.t3`, should be ignored by SQL event filter.
	deleteRow := datumsToChunkRow(types.MakeDatums(int64(1)), ti3)
	ignore, err = f.ShouldIgnoreDML(common.RowTypeDelete, deleteRow, emptyRow, ti3, 0)
	require.NoError(t, err)
	require.True(t, ignore, "Should be ignored by SQL event filter")
}

func TestShouldIgnoreDMLCaseSensitivity(t *testing.T) {
	t.Parallel()

	cfg := &config.FilterConfig{
		Rules: []string{"test.*"},
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.T2"}, // Uppercase T
				IgnoreEvent: []bf.EventType{bf.InsertEvent},
			},
		},
	}

	ti2Lower := mustNewCommonTableInfo("test", "t2", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)
	ti2Upper := mustNewCommonTableInfo("test", "T2", []*model.ColumnInfo{newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag)}, nil)
	rowData := datumsToChunkRow(types.MakeDatums(int64(1)), ti2Lower)
	emptyRow := chunk.Row{}

	cases := []struct {
		name          string
		caseSensitive bool
		tableInfo     *common.TableInfo
		shouldIgnore  bool
		msg           string
	}{
		{"sensitive_lowercase_miss", true, ti2Lower, false, "Case-sensitive: DML on 'test.t2' should not be ignored by rule for 'test.T2'"},
		{"sensitive_uppercase_hit", true, ti2Upper, true, "Case-sensitive: DML on 'test.T2' should be ignored by rule for 'test.T2'"},
		{"insensitive_lowercase_hit", false, ti2Lower, true, "Case-insensitive: DML on 'test.t2' should be ignored by rule for 'test.T2'"},
		{"insensitive_uppercase_hit", false, ti2Upper, true, "Case-insensitive: DML on 'test.T2' should be ignored by rule for 'test.T2'"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := NewFilter(cfg, "UTC", tc.caseSensitive, false)
			require.NoError(t, err)
			ignore, err := f.ShouldIgnoreDML(common.RowTypeInsert, emptyRow, rowData, tc.tableInfo, 0)
			require.NoError(t, err)
			require.Equal(t, tc.shouldIgnore, ignore, tc.msg)
		})
	}
}

func TestShouldIgnoreDDLCaseSensitivity(t *testing.T) {
	t.Parallel()

	cfg := &config.FilterConfig{
		Rules: []string{"test.*"},
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.T1"}, // Uppercase T
				IgnoreEvent: []bf.EventType{bf.DropTable},
			},
		},
	}

	cases := []struct {
		name          string
		caseSensitive bool
		schema        string
		table         string
		query         string
		shouldIgnore  bool
		msg           string
	}{
		{"sensitive_lowercase_miss", true, "test", "t1", "DROP TABLE t1", false, "Case-sensitive: DDL on 'test.t1' should not be ignored by rule for 'test.T1'"},
		{"sensitive_uppercase_hit", true, "test", "T1", "DROP TABLE T1", true, "Case-sensitive: DDL on 'test.T1' should be ignored by rule for 'test.T1'"},
		{"insensitive_lowercase_hit", false, "test", "t1", "DROP TABLE t1", true, "Case-insensitive: DDL on 'test.t1' should be ignored by rule for 'test.T1'"},
		{"insensitive_uppercase_hit", false, "test", "T1", "DROP TABLE T1", true, "Case-insensitive: DDL on 'test.T1' should be ignored by rule for 'test.T1'"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := NewFilter(cfg, "UTC", tc.caseSensitive, false)
			require.NoError(t, err)
			ignore, err := f.ShouldIgnoreDDL(tc.schema, tc.table, tc.query, model.ActionDropTable, nil, 0)
			require.NoError(t, err)
			require.Equal(t, tc.shouldIgnore, ignore, tc.msg)
		})
	}
}

func TestShouldIgnoreStartTs(t *testing.T) {
	tests := []struct {
		name     string
		ignoreTs []uint64
		ts       uint64
		want     bool
	}{
		{
			name:     "empty ignore list",
			ignoreTs: []uint64{},
			ts:       123,
			want:     false,
		},
		{
			name:     "ts in ignore list",
			ignoreTs: []uint64{100, 200, 300},
			ts:       200,
			want:     true,
		},
		{
			name:     "ts not in ignore list",
			ignoreTs: []uint64{100, 200, 300},
			ts:       400,
			want:     false,
		},
		{
			name:     "single element equal",
			ignoreTs: []uint64{999},
			ts:       999,
			want:     true,
		},
		{
			name:     "single element not equal",
			ignoreTs: []uint64{999},
			ts:       1000,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &filter{ignoreTxnStartTs: tt.ignoreTs}
			got := f.shouldIgnoreStartTs(tt.ts)
			if got != tt.want {
				t.Errorf("shouldIgnoreStartTs(%d) = %v, want %v", tt.ts, got, tt.want)
			}
		})
	}
}

func TestIsEligible(t *testing.T) {
	t.Parallel()

	// 1. Table with PK
	tiWithPK := mustNewModelTableInfo("t1",
		[]*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, mysql.PriKeyFlag),
		}, nil)

	// 2. Table with UK on not-null column
	tiWithUK := mustNewModelTableInfo("t2",
		[]*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, mysql.NotNullFlag),
		},
		[]*model.IndexInfo{
			newIndexInfo("uk_id", []*model.IndexColumn{{Name: ast.NewCIStr("id"), Offset: 0}}, false, true),
		})

	// 3. Table with UK on nullable column (ineligible)
	tiWithNullableUK := mustNewModelTableInfo("t3",
		[]*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, 0),
		},
		[]*model.IndexInfo{
			newIndexInfo("uk_id", []*model.IndexColumn{{Name: ast.NewCIStr("id"), Offset: 0}}, false, true),
		})

	// 4. Table with no PK or UK (ineligible)
	tiNoKey := mustNewModelTableInfo("t4",
		[]*model.ColumnInfo{
			newColumnInfo(1, "id", mysql.TypeLong, 0),
		}, nil)

	// 5. View (eligible)
	tiView := mustNewModelTableInfo("v1", nil, nil)
	tiView.View = &model.ViewInfo{}

	// 6. Sequence (ineligible)
	tiSeq := mustNewModelTableInfo("s1", nil, nil)
	tiSeq.Sequence = &model.SequenceInfo{}

	cfg := config.NewDefaultFilterConfig()
	// test with forceReplicate = false
	f, err := NewFilter(cfg, "UTC", true, false)
	require.NoError(t, err)
	filterImpl := f.(*filter)
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithPK)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithUK)))
	require.False(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithNullableUK)))
	require.False(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiNoKey)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiView)))
	require.False(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiSeq)))

	// test with forceReplicate = true
	f, err = NewFilter(cfg, "UTC", true, true)
	require.NoError(t, err)
	filterImpl = f.(*filter)
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithPK)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithUK)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiWithNullableUK)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiNoKey)))
	require.True(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiView)))
	require.False(t, filterImpl.IsEligibleTable(common.WrapTableInfo("test", tiSeq)), "Sequence should always be ineligible")
}

func TestIsAllowedDDL(t *testing.T) {
	require.Len(t, ddlWhiteListMap, 40)
	type testCase struct {
		model.ActionType
		allowed bool
	}
	testCases := make([]testCase, 0, len(ddlWhiteListMap))
	for ddlType := range ddlWhiteListMap {
		testCases = append(testCases, testCase{ddlType, true})
	}
	testCases = append(testCases, testCase{model.ActionCreateSequence, false})
	testCases = append(testCases, testCase{model.ActionAlterSequence, false})
	testCases = append(testCases, testCase{model.ActionDropSequence, false})
	for _, tc := range testCases {
		require.Equal(t, tc.allowed, isAllowedDDL(tc.ActionType), "%#v", tc)
	}
}

func TestIsSchemaDDL(t *testing.T) {
	cases := []struct {
		actionType model.ActionType
		isSchema   bool
	}{
		{model.ActionCreateSchema, true},
		{model.ActionDropSchema, true},
		{model.ActionModifySchemaCharsetAndCollate, true},
		{model.ActionCreateTable, false},
		{model.ActionDropTable, false},
		{model.ActionTruncateTable, false},
		{model.ActionAddColumn, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.isSchema, IsSchemaDDL(tc.actionType), "%#v", tc)
	}
}

// Helper functions to create test configurations
func createTestFilterConfig() *eventpb.FilterConfig {
	return &eventpb.FilterConfig{
		CaseSensitive:  false,
		ForceReplicate: false,
		FilterConfig: &eventpb.InnerFilterConfig{
			Rules:            []string{"*.*"},
			IgnoreTxnStartTs: []uint64{},
			EventFilters:     []*eventpb.EventFilterRule{},
		},
	}
}

func createTestFilterConfigWithRules(rules []string) *eventpb.FilterConfig {
	cfg := createTestFilterConfig()
	cfg.FilterConfig.Rules = rules
	return cfg
}

func createTestFilterConfigWithEventFilters(eventFilters []*eventpb.EventFilterRule) *eventpb.FilterConfig {
	cfg := createTestFilterConfig()
	cfg.FilterConfig.EventFilters = eventFilters
	return cfg
}

func createTestEventFilterRule() *eventpb.EventFilterRule {
	return &eventpb.EventFilterRule{
		Matcher:                  []string{"test.*"},
		IgnoreEvent:              []string{},
		IgnoreSql:                []string{},
		IgnoreInsertValueExpr:    "",
		IgnoreUpdateNewValueExpr: "",
		IgnoreUpdateOldValueExpr: "",
		IgnoreDeleteValueExpr:    "",
	}
}

func createTestChangeFeedID(name string) common.ChangeFeedID {
	return common.NewChangeFeedIDWithName(name, common.DefaultKeyspaceNamme)
}

// Helper functions to verify filter instances
func assertFilterInstancesEqual(t *testing.T, filter1, filter2 Filter) {
	// Use pointer comparison to verify they are the same instance
	require.Equal(t, filter1, filter2, "Filter instances should be the same")
}

func assertFilterInstancesDifferent(t *testing.T, filter1, filter2 Filter) {
	// Use pointer comparison to verify they are different instances
	require.NotEqual(t, filter1, filter2, "Filter instances should be different")
}

func TestSharedFilterStorage_SameConfigSharesFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := createTestFilterConfig()
	timeZone := "UTC"

	// First call - should create new filter
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with same config - should return same filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return the same filter instance
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_SameConfigContentSharesFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig() // Create separate config object with same content
	timeZone := "UTC"

	// First call - should create new filter
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with different config object but same content - should return same filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return the same filter instance due to content equality
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_DifferentConfigCreatesNewFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.CaseSensitive = true // Different configuration
	timeZone := "UTC"

	// First call with cfg1
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with cfg2 - should create new filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify both calls return different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_DifferentChangeFeedIDCreatesIndependentFilter(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID1 := createTestChangeFeedID("test-changefeed-1")
	changeFeedID2 := createTestChangeFeedID("test-changefeed-2")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfigWithRules([]string{"test.*"}) // Create different config content
	timeZone := "UTC"

	// Call with different changeFeedID and different config objects with different content
	filter1, err := storage.GetOrSetFilter(changeFeedID1, cfg1, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	filter2, err := storage.GetOrSetFilter(changeFeedID2, cfg2, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify different changeFeedID with different config content creates different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_CaseSensitiveFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.CaseSensitive = true
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_ForceReplicateFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.ForceReplicate = true
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_RulesFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfigWithRules([]string{"*.*"})
	cfg2 := createTestFilterConfigWithRules([]string{"test.*"})
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_IgnoreTxnStartTsFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfig()
	cfg2.FilterConfig.IgnoreTxnStartTs = []uint64{123456}
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_EventFiltersFieldDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg1 := createTestFilterConfig()
	cfg2 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{createTestEventFilterRule()})
	timeZone := "UTC"

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_ComplexEventFiltersDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	timeZone := "UTC"

	// Create first config with one event filter
	eventFilter1 := createTestEventFilterRule()
	eventFilter1.Matcher = []string{"test1.*"}
	cfg1 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{eventFilter1})

	// Create second config with different event filter
	eventFilter2 := createTestEventFilterRule()
	eventFilter2.Matcher = []string{"test2.*"}
	cfg2 := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{eventFilter2})

	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg1, timeZone)
	require.NoError(t, err)

	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg2, timeZone)
	require.NoError(t, err)

	assertFilterInstancesDifferent(t, filter1, filter2)
}

func TestSharedFilterStorage_EmptyConfigTest(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := &eventpb.FilterConfig{
		CaseSensitive:  false,
		ForceReplicate: false,
		FilterConfig: &eventpb.InnerFilterConfig{
			Rules:            []string{},
			IgnoreTxnStartTs: []uint64{},
			EventFilters:     []*eventpb.EventFilterRule{},
		},
	}
	timeZone := "UTC"

	filter, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
	require.NoError(t, err)
	require.NotNil(t, filter)
}

func TestSharedFilterStorage_ConcurrentAccessTest(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	cfg := createTestFilterConfig()
	timeZone := "UTC"

	// Test concurrent access with same config
	done := make(chan bool, 2)
	var filter1, filter2 Filter
	var err1, err2 error

	go func() {
		filter1, err1 = storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
		done <- true
	}()

	go func() {
		filter2, err2 = storage.GetOrSetFilter(changeFeedID, cfg, timeZone)
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
	<-done

	require.NoError(t, err1)
	require.NoError(t, err2)
	require.NotNil(t, filter1)
	require.NotNil(t, filter2)

	// Both should return the same filter instance due to sharing
	assertFilterInstancesEqual(t, filter1, filter2)
}

func TestSharedFilterStorage_TimeZoneDifference(t *testing.T) {
	storage := &SharedFilterStorage{
		m: make(map[common.ChangeFeedID]FilterWithConfig),
	}

	changeFeedID := createTestChangeFeedID("test-changefeed")
	// Create config with expression filters to make timezone difference more apparent
	cfg := createTestFilterConfigWithEventFilters([]*eventpb.EventFilterRule{
		{
			Matcher:                  []string{"test.*"},
			IgnoreEvent:              []string{},
			IgnoreSql:                []string{},
			IgnoreInsertValueExpr:    "id > 1000",
			IgnoreUpdateNewValueExpr: "",
			IgnoreUpdateOldValueExpr: "",
			IgnoreDeleteValueExpr:    "",
		},
	})
	timeZone1 := "UTC"
	timeZone2 := "Asia/Shanghai"

	// First call with UTC timezone
	filter1, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone1)
	require.NoError(t, err)
	require.NotNil(t, filter1)

	// Second call with different timezone - should create new filter
	filter2, err := storage.GetOrSetFilter(changeFeedID, cfg, timeZone2)
	require.NoError(t, err)
	require.NotNil(t, filter2)

	// Verify different timezone creates different filter instances
	assertFilterInstancesDifferent(t, filter1, filter2)
}
