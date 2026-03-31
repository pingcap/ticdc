// Copyright 2024 PingCAP, Inc.
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

package sqlmodel

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TestGenSQLInsertWithRouting tests that GenSQL for INSERT uses target schema/table
// when routing is configured via TableInfo.CloneWithRouting().
func TestGenSQLInsertWithRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)

	// Clone TableInfo with routing: test.t -> target_db.target_table
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil, // preValues (nil for INSERT)
		[]interface{}{int64(1), "test_value"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLInsert)
	require.Contains(t, sql, "`target_db`.`target_table`", "INSERT should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "INSERT should not contain source schema.table")
	require.Len(t, args, 2)

	// Test REPLACE
	sql, args = row.GenSQL(DMLReplace)
	require.Contains(t, sql, "`target_db`.`target_table`", "REPLACE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "REPLACE should not contain source schema.table")
	require.Len(t, args, 2)
}

// TestGenSQLDeleteWithRouting tests that GenSQL for DELETE uses target schema/table
func TestGenSQLDeleteWithRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "test_value"}, // preValues
		nil,                                   // postValues (nil for DELETE)
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLDelete)
	require.Contains(t, sql, "DELETE FROM `target_db`.`target_table`", "DELETE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "DELETE should not contain source schema.table")
	require.Greater(t, len(args), 0)
}

// TestGenSQLUpdateWithRouting tests that GenSQL for UPDATE uses target schema/table
func TestGenSQLUpdateWithRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old_value"},
		[]interface{}{int64(1), "new_value"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := row.GenSQL(DMLUpdate)
	require.Contains(t, sql, "UPDATE `target_db`.`target_table`", "UPDATE should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "UPDATE should not contain source schema.table")
	require.Len(t, args, 3, "UPDATE should have 3 args: new id, new name, old id (WHERE)")
	// Args: SET id=?, name=? WHERE id=?
	require.Equal(t, []interface{}{int64(1), "new_value", int64(1)}, args)
}

// TestGenSQLWithSchemaOnlyRouting tests routing where only schema changes
func TestGenSQLWithSchemaOnlyRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)
	routedTableInfo := sourceTableInfo.CloneWithRouting("prod", "users")

	// Test INSERT
	insertRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "alice"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source schema.table")

	// Test DELETE
	deleteRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "alice"},
		nil,
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source schema.table")

	// Test UPDATE
	updateRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "alice"},
		[]interface{}{int64(1), "bob"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`prod`.`users`", "Should use target schema and table")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source schema.table")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "bob", int64(1)}, args)
}

// TestGenSQLWithTableOnlyRouting tests routing where only table name changes
func TestGenSQLWithTableOnlyRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)
	routedTableInfo := sourceTableInfo.CloneWithRouting("test", "new_table")

	// Test INSERT
	insertRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "data"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source table name")

	// Test DELETE
	deleteRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "data"},
		nil,
		routedTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source table name")

	// Test UPDATE
	updateRow := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old_data"},
		[]interface{}{int64(1), "new_data"},
		routedTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`test`.`new_table`", "Should use original schema with target table name")
	require.NotContains(t, sql, "`test`.`t`", "Should not contain source table name")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "new_data", int64(1)}, args)
}

// TestGenSQLWithoutRouting verifies that when no routing is configured,
// source schema/table names are used.
func TestGenSQLWithoutRouting(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)

	// Test INSERT without routing
	insertRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "test"},
		sourceTableInfo,
		nil,
		nil,
	)

	sql, _ := insertRow.GenSQL(DMLInsert)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")

	// Test DELETE
	deleteRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "test"},
		nil,
		sourceTableInfo,
		nil,
		nil,
	)

	sql, _ = deleteRow.GenSQL(DMLDelete)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")

	// Test UPDATE
	updateRow := NewRowChange(
		&sourceTableInfo.TableName,
		nil,
		[]interface{}{int64(1), "old"},
		[]interface{}{int64(1), "new"},
		sourceTableInfo,
		nil,
		nil,
	)

	sql, args := updateRow.GenSQL(DMLUpdate)
	require.Contains(t, sql, "`test`.`t`", "Should use source schema and table")
	require.Len(t, args, 3)
	require.Equal(t, []interface{}{int64(1), "new", int64(1)}, args)
}

// TestTargetTableID verifies that TargetTableID returns the correct quoted target string
func TestTargetTableID(t *testing.T) {
	sourceTableInfo := getSharedTableInfo(t)
	routedTableInfo := sourceTableInfo.CloneWithRouting("target_db", "target_table")

	row := NewRowChange(
		&routedTableInfo.TableName,
		nil,
		nil,
		[]interface{}{int64(1), "test"},
		routedTableInfo,
		nil,
		nil,
	)

	require.Equal(t, "`target_db`.`target_table`", row.TargetTableID())
}

func mockTableInfo(t *testing.T, sql string) *common.TableInfo {
	p := parser.New()
	se := metabuild.NewContext()
	node, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	dbChs, dbColl := charset.GetDefaultCharsetAndCollate()
	rawTi, err := ddl.BuildTableInfoWithStmt(se, node.(*ast.CreateTableStmt), dbChs, dbColl, nil)
	require.NoError(t, err)
	return common.WrapTableInfo("db", rawTi)
}

type dpanicSuite struct {
	suite.Suite
}

func (s *dpanicSuite) SetupSuite() {
	_, _, err := log.InitLogger(&log.Config{Level: "debug"})
	s.NoError(err)
}

func TestDpanicSuite(t *testing.T) {
	suite.Run(t, new(dpanicSuite))
}

func TestNewRowChange(t *testing.T) {
	t.Parallel()

	source := &common.TableName{Schema: "db", Table: "tbl"}
	target := &common.TableName{Schema: "db", Table: "tbl_routed"}
	sourceTI := mockTableInfo(t, "CREATE TABLE tbl (id INT PRIMARY KEY, name INT)")
	targetTI := mockTableInfo(t, "CREATE TABLE tbl_routed (id INT PRIMARY KEY, name INT)")
	tiSession := util.NewSessionCtx(map[string]string{
		"time_zone": "+08:00",
	})

	expected := &RowChange{
		sourceTable:     source,
		targetTable:     target,
		preValues:       []interface{}{1, 2},
		postValues:      []interface{}{1, 3},
		sourceTableInfo: sourceTI,
		targetTableInfo: targetTI,
		tiSessionCtx:    tiSession,
		tp:              RowChangeUpdate,
		whereHandle:     nil,
	}

	actual := NewRowChange(source, target, []interface{}{1, 2}, []interface{}{1, 3}, sourceTI, targetTI, tiSession)
	require.Equal(t, expected, actual)

	actual.lazyInitWhereHandle()
	require.NotNil(t, actual.whereHandle)

	// test some arguments of NewRowChange can be nil

	expected.targetTable = expected.sourceTable
	expected.targetTableInfo = expected.sourceTableInfo
	expected.tiSessionCtx = util.ZeroSessionCtx
	expected.whereHandle = nil
	actual = NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{1, 3}, sourceTI, nil, nil)
	require.Equal(t, expected, actual)
}

func (s *dpanicSuite) TestRowChangeType() {
	change := &RowChange{preValues: []interface{}{1}}
	change.calculateType()
	s.Equal(RowChangeDelete, change.tp)
	change = &RowChange{preValues: []interface{}{1}, postValues: []interface{}{2}}
	change.calculateType()
	s.Equal(RowChangeUpdate, change.tp)
	change = &RowChange{postValues: []interface{}{1}}
	change.calculateType()
	s.Equal(RowChangeInsert, change.tp)

	s.Panics(func() {
		change = &RowChange{}
		change.calculateType()
	})
}

func (s *dpanicSuite) TestGenDelete() {
	source := &common.TableName{Schema: "db", Table: "tb1"}
	target := &common.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		preValues       []interface{}

		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)",
			"CREATE TABLE tb2 (id INT PRIMARY KEY, name INT, extra VARCHAR(20))",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `id` = ? LIMIT 1",
			[]interface{}{1},
		},
		{
			"CREATE TABLE tb1 (c INT, c2 INT UNIQUE)",
			"CREATE TABLE tb2 (c INT, c2 INT UNIQUE)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `c2` = ? LIMIT 1",
			[]interface{}{2},
		},
		// next 2 cases test NULL value
		{
			"CREATE TABLE tb1 (c INT, c2 INT UNIQUE)",
			"CREATE TABLE tb2 (c INT, c2 INT UNIQUE)",
			[]interface{}{1, nil},

			"DELETE FROM `db`.`tb2` WHERE `c` = ? AND `c2` IS ? LIMIT 1",
			[]interface{}{1, nil},
		},
		{
			"CREATE TABLE tb1 (c INT, c2 INT)",
			"CREATE TABLE tb2 (c INT, c2 INT)",
			[]interface{}{1, nil},

			"DELETE FROM `db`.`tb2` WHERE `c` = ? AND `c2` IS ? LIMIT 1",
			[]interface{}{1, nil},
		},
		// next 2 cases test using downstream table to generate WHERE
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, user_id INT NOT NULL UNIQUE)",
			"CREATE TABLE tb2 (new_id INT PRIMARY KEY, id INT, user_id INT NOT NULL UNIQUE)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `user_id` = ? LIMIT 1",
			[]interface{}{2},
		},
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (new_id INT PRIMARY KEY, id INT, c2 INT)",
			[]interface{}{1, 2},

			"DELETE FROM `db`.`tb2` WHERE `id` = ? AND `c2` = ? LIMIT 1",
			[]interface{}{1, 2},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(s.T(), c.sourceCreateSQL)
		targetTI := mockTableInfo(s.T(), c.targetCreateSQL)
		change := NewRowChange(source, target, c.preValues, nil, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLDelete)
		s.Equal(c.expectedSQL, sql)
		s.Equal(c.expectedArgs, args)
	}

	// a RowChangeUpdate can still generate DELETE SQL
	sourceTI := mockTableInfo(s.T(), "CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)")
	change := NewRowChange(source, nil, []interface{}{1, 2}, []interface{}{3, 4}, sourceTI, nil, nil)
	sql, args := change.GenSQL(DMLDelete)
	s.Equal("DELETE FROM `db`.`tb1` WHERE `id` = ? LIMIT 1", sql)
	s.Equal([]interface{}{1}, args)

	change = NewRowChange(source, nil, nil, []interface{}{3, 4}, sourceTI, nil, nil)
	s.Panics(func() {
		change.GenSQL(DMLDelete)
	})
}

func (s *dpanicSuite) TestGenUpdate() {
	source := &common.TableName{Schema: "db", Table: "tb1"}
	target := &common.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		preValues       []interface{}
		postValues      []interface{}

		expectedSQL  string
		expectedArgs []interface{}
	}{
		{
			"CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)",
			"CREATE TABLE tb2 (id INT PRIMARY KEY, name INT, extra VARCHAR(20))",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `id` = ?, `name` = ? WHERE `id` = ? LIMIT 1",
			[]interface{}{3, 4, 1},
		},
		{
			"CREATE TABLE tb1 (id INT UNIQUE, name INT)",
			"CREATE TABLE tb2 (id INT UNIQUE, name INT)",
			[]interface{}{nil, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `id` = ?, `name` = ? WHERE `id` IS ? AND `name` = ? LIMIT 1",
			[]interface{}{3, 4, nil, 2},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (c INT, c2 INT)",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ?, `c2` = ? WHERE `c` = ? AND `c2` = ? LIMIT 1",
			[]interface{}{3, 4, 1, 2},
		},
		// next 2 cases test generated column
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ? WHERE `c` = ? LIMIT 1",
			[]interface{}{3, 1},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT)",
			[]interface{}{1, 2},
			[]interface{}{3, 4},

			"UPDATE `db`.`tb2` SET `c` = ?, `c2` = ? WHERE `c` = ? LIMIT 1",
			[]interface{}{3, 4, 1},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(s.T(), c.sourceCreateSQL)
		targetTI := mockTableInfo(s.T(), c.targetCreateSQL)
		change := NewRowChange(source, target, c.preValues, c.postValues, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLUpdate)
		s.Equal(c.expectedSQL, sql)
		s.Equal(c.expectedArgs, args)
	}

	sourceTI := mockTableInfo(s.T(), "CREATE TABLE tb1 (id INT PRIMARY KEY, name INT)")
	change := NewRowChange(source, nil, nil, []interface{}{3, 4}, sourceTI, nil, nil)
	s.Panics(func() {
		change.GenSQL(DMLUpdate)
	})
}

func (s *dpanicSuite) TestExpressionIndex() {
	source := &common.TableName{Schema: "db", Table: "tb1"}
	sql := `CREATE TABLE tb1 (
    	id INT PRIMARY KEY,
    	j JSON,
    	UNIQUE KEY j_index ((cast(json_extract(j,'$[*]') as signed array)), id)
)`
	ti := mockTableInfo(s.T(), sql)
	change := NewRowChange(source, nil, nil, []interface{}{1, `[1,2,3]`}, ti, nil, nil)
	sql, args := change.GenSQL(DMLInsert)
	s.Equal("INSERT INTO `db`.`tb1` (`id`,`j`) VALUES (?,?)", sql)
	s.Equal([]interface{}{1, `[1,2,3]`}, args)
	require.Equal(s.T(), 2, change.ColumnCount())

	change2 := NewRowChange(source, nil, []interface{}{1, `[1,2,3]`}, []interface{}{1, `[1,2,3,4]`}, ti, nil, nil)
	sql, args = change2.GenSQL(DMLUpdate)
	s.Equal("UPDATE `db`.`tb1` SET `id` = ?, `j` = ? WHERE `id` = ? LIMIT 1", sql)
	s.Equal([]interface{}{1, `[1,2,3,4]`, 1}, args)
}

func TestGenInsert(t *testing.T) {
	t.Parallel()

	source := &common.TableName{Schema: "db", Table: "tb1"}
	target := &common.TableName{Schema: "db", Table: "tb2"}

	cases := []struct {
		sourceCreateSQL string
		targetCreateSQL string
		postValues      []interface{}

		expectedInsertSQL      string
		expectedReplaceSQL     string
		expectedInsertOnDupSQL string
		expectedArgs           []interface{}
	}{
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT)",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT, extra VARCHAR(20))",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"REPLACE INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`),`c2`=VALUES(`c2`)",
			[]interface{}{1, 2},
		},
		// next 2 cases test generated column
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`) VALUES (?)",
			"REPLACE INTO `db`.`tb2` (`c`) VALUES (?)",
			"INSERT INTO `db`.`tb2` (`c`) VALUES (?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`)",
			[]interface{}{1},
		},
		{
			"CREATE TABLE tb1 (c INT PRIMARY KEY, c2 INT AS (c+1))",
			"CREATE TABLE tb2 (c INT PRIMARY KEY, c2 INT)",
			[]interface{}{1, 2},

			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"REPLACE INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?)",
			"INSERT INTO `db`.`tb2` (`c`,`c2`) VALUES (?,?) ON DUPLICATE KEY UPDATE `c`=VALUES(`c`),`c2`=VALUES(`c2`)",
			[]interface{}{1, 2},
		},
	}

	for _, c := range cases {
		sourceTI := mockTableInfo(t, c.sourceCreateSQL)
		targetTI := mockTableInfo(t, c.targetCreateSQL)
		change := NewRowChange(source, target, nil, c.postValues, sourceTI, targetTI, nil)
		sql, args := change.GenSQL(DMLInsert)
		require.Equal(t, c.expectedInsertSQL, sql)
		require.Equal(t, c.expectedArgs, args)
		sql, args = change.GenSQL(DMLReplace)
		require.Equal(t, c.expectedReplaceSQL, sql)
		require.Equal(t, c.expectedArgs, args)
		sql, args = change.GenSQL(DMLInsertOnDuplicateUpdate)
		require.Equal(t, c.expectedInsertOnDupSQL, sql)
		require.Equal(t, c.expectedArgs, args)
	}
}
