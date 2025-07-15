// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	ticonfig "github.com/pingcap/tidb/pkg/config"
	tiddl "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	// NOTE: Do not remove the `test_driver` import.
	// For details, refer to: https://github.com/pingcap/parser/issues/43
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// CAUTION:
// ALL METHODS IN THIS FILE ARE FOR TESTING ONLY!!!
// DO NOT USE THEM IN OTHER PLACES.

// EventTestHelper is a test helper for generating test events
type EventTestHelper struct {
	t       testing.TB
	tk      *testkit.TestKit
	storage kv.Storage
	domain  *domain.Domain
	mounter Mounter

	tableInfos map[string]*common.TableInfo
	// each partition table's partition ID, Name -> ID.
	partitionIDs map[string]map[string]int64
}

// NewEventTestHelperWithTimeZone creates a SchemaTestHelper with time zone
func NewEventTestHelperWithTimeZone(t testing.TB, tz *time.Location) *EventTestHelper {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	ticonfig.UpdateGlobal(func(conf *ticonfig.Config) {
		conf.AlterPrimaryKey = true
	})
	session.SetSchemaLease(time.Second)
	session.DisableStats4Test()
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	domain.SetStatsUpdating(true)
	tk := testkit.NewTestKit(t, store)
	return &EventTestHelper{
		t:            t,
		tk:           tk,
		storage:      store,
		domain:       domain,
		mounter:      NewMounter(tz, config.GetDefaultReplicaConfig().Integrity),
		tableInfos:   make(map[string]*common.TableInfo),
		partitionIDs: make(map[string]map[string]int64),
	}
}

// NewEventTestHelper creates a SchemaTestHelper
func NewEventTestHelper(t testing.TB) *EventTestHelper {
	return NewEventTestHelperWithTimeZone(t, time.Local)
}

func (s *EventTestHelper) ApplyJob(job *timodel.Job) {
	var tableInfo *timodel.TableInfo
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil {
		tableInfo = job.BinlogInfo.TableInfo
	} else {
		// Just retrieve the schema name for a DDL job that does not contain TableInfo.
		// Currently supported by cdc are: ActionCreateSchema, ActionDropSchema,
		// and ActionModifySchemaCharsetAndCollate.
		tableInfo = &timodel.TableInfo{
			Version: uint16(job.BinlogInfo.FinishedTS),
		}
	}

	info := common.WrapTableInfo(job.SchemaName, tableInfo)
	info.InitPrivateFields()
	key := toTableInfosKey(info.GetSchemaName(), info.GetTableName())
	if tableInfo.Partition != nil {
		if _, ok := s.partitionIDs[key]; !ok {
			s.partitionIDs[key] = make(map[string]int64)
		}
		for _, partition := range tableInfo.Partition.Definitions {
			s.partitionIDs[key][partition.Name.O] = partition.ID
		}
	}
	log.Info("apply job", zap.String("jobKey", key), zap.Any("job", job))
	s.tableInfos[key] = info
}

func (s *EventTestHelper) GetModelTableInfo(job *timodel.Job) *timodel.TableInfo {
	return job.BinlogInfo.TableInfo
}

func (s *EventTestHelper) GetTableInfo(job *timodel.Job) *common.TableInfo {
	table := ""
	if job.BinlogInfo != nil && job.BinlogInfo.TableInfo != nil {
		table = job.BinlogInfo.TableInfo.Name.O
	}
	key := toTableInfosKey(job.SchemaName, table)
	log.Info("apply job", zap.String("jobKey", key), zap.Any("job", job))
	return s.tableInfos[key]
}

// DDL2Job executes the DDL stmt and returns the DDL job
func (s *EventTestHelper) DDL2Job(ddl string) *timodel.Job {
	s.tk.MustExec(ddl)
	jobs, err := tiddl.GetLastNHistoryDDLJobs(s.GetCurrentMeta(), 1)
	require.Nil(s.t, err)
	require.Len(s.t, jobs, 1)
	// Set State from Synced to Done.
	// Because jobs are put to history queue after TiDB alter its state from
	// Done to Synced.
	jobs[0].State = timodel.JobStateDone
	res := jobs[0]

	if res.Type == timodel.ActionExchangeTablePartition {
		upperQuery := strings.ToUpper(res.Query)
		idx1 := strings.Index(upperQuery, "EXCHANGE PARTITION") + len("EXCHANGE PARTITION")
		idx2 := strings.Index(upperQuery, "WITH TABLE")

		// Note that partition name should be parsed from original query, not the upperQuery.
		partName := strings.TrimSpace(res.Query[idx1:idx2])
		partName = strings.Replace(partName, "`", "", -1)
		res.Query = fmt.Sprintf("ALTER TABLE `%s`.`%s` EXCHANGE PARTITION `%s` WITH TABLE `%s`.`%s`",
			res.InvolvingSchemaInfo[0].Database, res.InvolvingSchemaInfo[0].Table, partName, res.SchemaName, res.TableName)

		if strings.HasSuffix(upperQuery, "WITHOUT VALIDATION") {
			res.Query += " WITHOUT VALIDATION"
		}
	}

	s.ApplyJob(res)
	if res.Type != timodel.ActionRenameTables {
		return res
	}

	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	// we assume the old schema name is same as the new schema name here.
	// for example, "ALTER TABLE RENAME test.t1 TO test.t1, test.t2 to test.t22", schema name is "test"
	schema := strings.Split(strings.Split(strings.Split(res.Query, ",")[1], " ")[1], ".")[0]
	tableNum := len(res.BinlogInfo.MultipleTableInfos)
	oldSchemaIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaIDs[i] = res.SchemaID
	}
	oldTableIDs := make([]int64, tableNum)
	for i := 0; i < tableNum; i++ {
		oldTableIDs[i] = res.BinlogInfo.MultipleTableInfos[i].ID
	}
	newTableNames := make([]ast.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		newTableNames[i] = res.BinlogInfo.MultipleTableInfos[i].Name
	}
	oldSchemaNames := make([]ast.CIStr, tableNum)
	for i := 0; i < tableNum; i++ {
		oldSchemaNames[i] = ast.NewCIStr(schema)
	}
	newSchemaIDs := oldSchemaIDs

	args := []interface{}{
		oldSchemaIDs, newSchemaIDs,
		newTableNames, oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.NoError(s.t, err)
	res.RawArgs = rawArgs
	return res
}

func (s *EventTestHelper) DDL2Event(ddl string) *DDLEvent {
	job := s.DDL2Job(ddl)
	info := s.GetTableInfo(job)
	return &DDLEvent{
		SchemaID:   job.SchemaID,
		TableID:    info.TableName.TableID,
		SchemaName: job.SchemaName,
		TableName:  job.TableName,
		Query:      job.Query,
		Type:       byte(job.Type),
		TableInfo:  info,
		FinishedTs: job.BinlogInfo.FinishedTS,
	}
}

func (s *EventTestHelper) DML2BatchEvent(schema, table string, dmls ...string) *BatchDMLEvent {
	key := toTableInfosKey(schema, table)
	log.Info("dml2batchEvent", zap.String("key", key))
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	batchDMLEvent := NewBatchDMLEvent()
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	physicalTableID := tableInfo.TableName.TableID
	for _, dml := range dmls {
		dmlEvent := NewDMLEvent(did, physicalTableID, ts-1, ts+1, tableInfo)
		batchDMLEvent.AppendDMLEvent(dmlEvent)
		rawKvs := s.DML2RawKv(physicalTableID, ts, dml)
		for _, rawKV := range rawKvs {
			err := dmlEvent.AppendRow(rawKV, s.mounter.DecodeToChunk)
			require.NoError(s.t, err)
		}
	}
	return batchDMLEvent
}

func (s *EventTestHelper) DML2Event4PartitionTable(schema, table, partition, dml string) *DMLEvent {
	key := toTableInfosKey(schema, table)
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)

	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	physicalTableID := s.partitionIDs[key][partition]
	dmlEvent := NewDMLEvent(did, physicalTableID, ts-1, ts+1, tableInfo)
	dmlEvent.SetRows(chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1))
	rawKvs := s.DML2RawKv(physicalTableID, ts, dml)
	for _, rawKV := range rawKvs {
		err := dmlEvent.AppendRow(rawKV, s.mounter.DecodeToChunk)
		require.NoError(s.t, err)
	}
	return dmlEvent
}

// DML2Event execute the dml(s) and return the corresponding DMLEvent.
// Note:
// 1. It dose not support `delete` since the key value cannot be found
// after the query executed.
// 2. You must execute create table statement before calling this function.
// 3. You must set the preRow of the DMLEvent by yourself, since we can not get it from TiDB.
func (s *EventTestHelper) DML2Event(schema, table string, dmls ...string) *DMLEvent {
	key := toTableInfosKey(schema, table)
	log.Info("dml2event", zap.String("key", key))
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	physicalTableID := tableInfo.TableName.TableID
	dmlEvent := NewDMLEvent(did, physicalTableID, ts-1, ts+1, tableInfo)
	dmlEvent.SetRows(chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1))

	rawKvs := s.DML2RawKv(physicalTableID, ts, dmls...)
	for _, rawKV := range rawKvs {
		err := dmlEvent.AppendRow(rawKV, s.mounter.DecodeToChunk)
		require.NoError(s.t, err)
	}
	return dmlEvent
}

func (s *EventTestHelper) DML2UpdateEvent(schema, table string, dml ...string) (*DMLEvent, *common.RawKVEntry) {
	if len(dml) != 2 {
		log.Fatal("DML2UpdateEvent must have 2 dml statements, the first one is insert, the second one is update", zap.Any("dml", dml))
	}

	lowerInsert := strings.ToLower(dml[0])
	lowerUpdate := strings.ToLower(dml[1])

	if !strings.Contains(lowerInsert, "insert") || !strings.Contains(lowerUpdate, "update") {
		log.Fatal("DML2UpdateEvent must have 2 dml statements, the first one is insert, the second one is update", zap.Any("dml", dml))
	}

	key := toTableInfosKey(schema, table)
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	physicalTableID := tableInfo.TableName.TableID
	dmlEvent := NewDMLEvent(did, physicalTableID, ts-1, ts+1, tableInfo)
	dmlEvent.SetRows(chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1))

	rawKvs := s.DML2RawKv(physicalTableID, ts, dml...)

	raw := &common.RawKVEntry{
		OpType:   common.OpTypePut,
		Key:      rawKvs[1].Key,
		Value:    rawKvs[1].Value,
		OldValue: rawKvs[0].Value,
		StartTs:  rawKvs[0].StartTs,
		CRTs:     rawKvs[1].CRTs,
	}

	dmlEvent.AppendRow(raw, s.mounter.DecodeToChunk)

	return dmlEvent, raw
}

// DML2DeleteEvent use a insert event to generate the delete event for this event
func (s *EventTestHelper) DML2DeleteEvent(schema, table string, dml string, deleteDml string) *DMLEvent {
	if !strings.Contains(strings.ToLower(dml), "insert") {
		log.Fatal("event for DML2DeleteEvent must be insert", zap.Any("dml", dml))
	}

	if !strings.Contains(strings.ToLower(deleteDml), "delete") {
		log.Fatal("the 'deleteDml' parameter for DML2DeleteEvent must be a DELETE statement", zap.Any("deleteDml", deleteDml))
	}

	key := toTableInfosKey(schema, table)
	log.Info("dml2event", zap.String("key", key))
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	did := common.NewDispatcherID()
	ts := tableInfo.UpdateTS()
	physicalTableID := tableInfo.TableName.TableID
	dmlEvent := NewDMLEvent(did, physicalTableID, ts-1, ts+1, tableInfo)
	dmlEvent.SetRows(chunk.NewChunkWithCapacity(tableInfo.GetFieldSlice(), 1))

	rawKv := s.DML2RawKv(physicalTableID, ts, dml)

	raw := &common.RawKVEntry{
		OpType:   common.OpTypeDelete,
		Key:      rawKv[0].Key,
		Value:    nil,
		OldValue: rawKv[0].Value,
		StartTs:  rawKv[0].StartTs,
		CRTs:     rawKv[0].CRTs,
	}
	err := dmlEvent.AppendRow(raw, s.mounter.DecodeToChunk)
	require.NoError(s.t, err)

	_ = s.DML2RawKv(physicalTableID, ts, deleteDml)

	return dmlEvent
}

// execute delete dml to clear the data record
func (s *EventTestHelper) ExecuteDeleteDml(schema, table string, dml string) {
	if !strings.Contains(strings.ToLower(dml), "delete") {
		log.Fatal("dml for ExecuteDeleteDml must be a DELETE statement", zap.Any("deleteDml", dml))
	}

	key := toTableInfosKey(schema, table)
	tableInfo, ok := s.tableInfos[key]
	require.True(s.t, ok)
	ts := tableInfo.UpdateTS()
	physicalTableID := tableInfo.TableName.TableID

	_ = s.DML2RawKv(physicalTableID, ts, dml)
}

func (s *EventTestHelper) DML2RawKv(physicalTableID int64, ddlFinishedTs uint64, dmls ...string) []*common.RawKVEntry {
	var rawKVs []*common.RawKVEntry
	for i, dml := range dmls {
		s.tk.MustExec(dml)
		key, value := s.getLastKeyValue(physicalTableID)
		rawKV := &common.RawKVEntry{
			OpType:   common.OpTypePut,
			Key:      key,
			Value:    value,
			OldValue: nil,
			StartTs:  ddlFinishedTs + uint64(i),
			CRTs:     ddlFinishedTs + uint64(i+1),
		}
		rawKVs = append(rawKVs, rawKV)
	}
	return rawKVs
}

func (s *EventTestHelper) getLastKeyValue(tableID int64) (key, value []byte) {
	txn, err := s.storage.Begin()
	require.NoError(s.t, err)
	defer txn.Rollback() //nolint:errcheck

	start, end := common.GetTableRange(tableID)
	iter, err := txn.Iter(start, end)
	require.NoError(s.t, err)
	defer iter.Close()
	for iter.Valid() {
		key = iter.Key()
		value = iter.Value()
		err = iter.Next()
		require.NoError(s.t, err)
	}
	return key, value
}

// Storage returns the tikv storage
func (s *EventTestHelper) Storage() kv.Storage {
	return s.storage
}

// Tk returns the TestKit
func (s *EventTestHelper) Tk() *testkit.TestKit {
	return s.tk
}

// GetCurrentMeta return the current meta snapshot
func (s *EventTestHelper) GetCurrentMeta() meta.Reader {
	ver, err := s.storage.CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(s.t, err)
	return meta.NewReader(s.storage.GetSnapshot(ver))
}

// Close closes the helper
func (s *EventTestHelper) Close() {
	s.domain.Close()
	s.storage.Close() //nolint:errcheck
}

func toTableInfosKey(schema, table string) string {
	return schema + "." + table
}

// SplitQueries takes a string containing multiple SQL statements and splits them into individual SQL statements.
// This function is designed for scenarios like batch creation of tables, where multiple `CREATE TABLE` statements
// might be combined into a single query string.
func SplitQueries(queries string) ([]string, error) {
	// Note: The parser is not thread-safe, so we create a new instance of the parser for each use.
	// However, the overhead of creating a new parser is minimal, so there is no need to worry about performance.
	p := parser.New()
	stmts, warns, err := p.ParseSQL(queries)
	for _, w := range warns {
		log.Warn("parse sql warnning", zap.Error(w))
	}
	if err != nil {
		return nil, errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
	}

	var res []string
	for _, stmt := range stmts {
		var sb strings.Builder
		err := stmt.Restore(&format.RestoreCtx{
			Flags: format.DefaultRestoreFlags,
			In:    &sb,
		})
		if err != nil {
			return nil, errors.WrapError(errors.ErrTiDBUnexpectedJobMeta, err)
		}
		// The (ast.Node).Restore function generates a SQL string representation of the AST (Abstract Syntax Tree) node.
		// By default, the resulting SQL string does not include a trailing semicolon ";".
		// Therefore, we explicitly append a semicolon here to ensure the SQL statement is complete.
		sb.WriteByte(';')
		res = append(res, sb.String())
	}

	return res, nil
}

func BatchDML(dml *DMLEvent) *BatchDMLEvent {
	return &BatchDMLEvent{
		DMLEvents: []*DMLEvent{dml},
		TableInfo: dml.TableInfo,
		Rows:      dml.Rows,
	}
}
