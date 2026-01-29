// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package mysql

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/stretchr/testify/require"
)

func newTestTableSchemaStore(tables []*event.SchemaTableName) *commonEvent.TableSchemaStore {
	schemaByName := make(map[string]*heartbeatpb.SchemaInfo)
	nextSchemaID := int64(1)
	nextTableID := int64(1)

	for _, tbl := range tables {
		schema := schemaByName[tbl.SchemaName]
		if schema == nil {
			schema = &heartbeatpb.SchemaInfo{
				SchemaID:   nextSchemaID,
				SchemaName: tbl.SchemaName,
			}
			nextSchemaID++
			schemaByName[tbl.SchemaName] = schema
		}
		schema.Tables = append(schema.Tables, &heartbeatpb.TableInfo{
			TableID:   nextTableID,
			TableName: tbl.TableName,
		})
		nextTableID++
	}

	schemaInfos := make([]*heartbeatpb.SchemaInfo, 0, len(schemaByName))
	for _, schemaInfo := range schemaByName {
		schemaInfos = append(schemaInfos, schemaInfo)
	}
	return commonEvent.NewTableSchemaStore(schemaInfos, common.MysqlSinkType, true)
}

func TestProgressTableWriterFlushSingleBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	setTestClusterID(t, "cluster-single")

	writer := NewProgressTableWriter(context.Background(), db, common.NewChangeFeedIDWithName("cf", "ks"), 10, 1*time.Millisecond)
	tables := []*event.SchemaTableName{
		{SchemaName: "db1", TableName: "t1"},
		{SchemaName: "db1", TableName: "t2"},
	}
	tableSchemaStore := newTestTableSchemaStore(tables)
	writer.SetTableSchemaStore(tableSchemaStore)

	expectProgressTableInit(mock)
	expectProgressInsert(mock, "ks/cf", "cluster-single", 42, tables)

	err = writer.Flush(42)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProgressTableWriterFlushMultiBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	setTestClusterID(t, "cluster-multi")

	writer := NewProgressTableWriter(context.Background(), db, common.NewChangeFeedIDWithName("cf", "ks"), 2, 1*time.Millisecond)
	allTables := []*event.SchemaTableName{
		{SchemaName: "db1", TableName: "t1"},
		{SchemaName: "db1", TableName: "t2"},
		{SchemaName: "db1", TableName: "t3"},
	}
	writer.SetTableSchemaStore(newTestTableSchemaStore(allTables))

	expectProgressTableInit(mock)
	expectProgressInsert(mock, "ks/cf", "cluster-multi", 99, allTables[:2])
	expectProgressInsert(mock, "ks/cf", "cluster-multi", 99, allTables[2:])

	err = writer.Flush(99)
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func setTestClusterID(t *testing.T, id string) {
	orig := config.GetGlobalServerConfig()
	cfg := orig.Clone()
	cfg.ClusterID = id
	config.StoreGlobalServerConfig(cfg)
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(orig)
	})
}

func expectProgressTableInit(mock sqlmock.Sqlmock) {
	mock.ExpectExec("CREATE DATABASE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`").
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS `" + filter.TiCDCSystemSchema + "`.`" + progressTableName + "`").
		WillReturnResult(sqlmock.NewResult(0, 0))
}

func expectProgressInsert(mock sqlmock.Sqlmock, changefeed, cluster string, checkpoint uint64, tables []*event.SchemaTableName) {
	args := make([]driver.Value, 0, len(tables)*5)
	for _, tbl := range tables {
		args = append(args, changefeed, cluster, tbl.SchemaName, tbl.TableName, checkpoint)
	}
	mock.ExpectExec("INSERT INTO `" + filter.TiCDCSystemSchema + "`.`" + progressTableName + "`").
		WithArgs(args...).
		WillReturnResult(sqlmock.NewResult(0, int64(len(tables))))
}
