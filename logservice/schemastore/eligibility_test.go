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
	"bytes"
	"testing"
	"testing/synctest"

	"github.com/cockroachdb/pebble"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
	"github.com/tinylib/msgp/msgp"
)

func TestPersistedReplicationKeyCompatibility(t *testing.T) {
	// Older records used this name and did not contain a key-loss flag.
	payload := msgp.AppendMapHeader(nil, 1)
	payload = msgp.AppendString(payload, "table_became_eligible")
	payload = msgp.AppendBool(payload, true)
	var event PersistedDDLEvent
	rest, err := event.UnmarshalMsg(payload)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.True(t, event.TableAcquiredReplicationKey)
	require.False(t, event.TableLostReplicationKey)
	var decoded PersistedDDLEvent
	require.NoError(t, decoded.DecodeMsg(msgp.NewReader(bytes.NewReader(payload))))
	require.Equal(t, event, decoded)
}

func TestGetTableInfoAtTsWaitsForRegistration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newEmptyVersionedTableInfoStore(100)
		storage := &persistentStorage{
			tableInfoStoreMap: map[int64]*versionedTableInfoStore{100: store},
		}
		info := common.WrapTableInfo("test", newEligibleTableInfoForTest(100, "a"))
		done := make(chan struct{})
		go func() {
			defer close(done)
			actual, err := storage.getTableInfoAtTs(100, 10)
			require.NoError(t, err)
			require.Same(t, info, actual)
		}()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("table info read completed before registration initialized the store")
		default:
		}
		store.addInitialTableInfo(info, 0)
		store.setTableInfoInitialized()
		<-done
	})
}

func TestDDLReplicationKeyFiltering(t *testing.T) {
	rawEvent := &PersistedDDLEvent{
		Type: byte(model.ActionAddPrimaryKey), SchemaID: 1, SchemaName: "test",
		TableID: 100, TableName: "a", TableInfo: newEligibleTableInfoForTest(100, "a"),
		Query: "ALTER TABLE test.a ADD PRIMARY KEY (a)", FinishedTs: 20,
		TableAcquiredReplicationKey: true,
	}
	for _, tc := range []struct {
		name           string
		noFilter       bool
		forceReplicate bool
		excludeTable   bool
		ignoreDDL      bool
		triggerEvent   bool
		tableEvent     bool
		addTable       bool
		lostKey        bool
		dropTable      bool
	}{
		{name: "default replication", triggerEvent: true, addTable: true},
		{name: "force replication", forceReplicate: true, tableEvent: true},
		{name: "nil filter", noFilter: true, tableEvent: true},
		{name: "excluded table", excludeTable: true},
		{name: "excluded table with force replication", forceReplicate: true, excludeTable: true},
		{name: "ignored DDL", ignoreDDL: true, triggerEvent: true, addTable: true},
		{name: "ignored DDL with force replication", forceReplicate: true, ignoreDDL: true, tableEvent: true},
		{name: "lost key", lostKey: true, triggerEvent: true, tableEvent: true, dropTable: true},
		{name: "lost key force replication", lostKey: true, forceReplicate: true, tableEvent: true},
		{name: "lost key nil filter", lostKey: true, noFilter: true, tableEvent: true},
		{name: "lost key excluded table", lostKey: true, excludeTable: true},
		{name: "lost key excluded force replication", lostKey: true, excludeTable: true, forceReplicate: true},
		{name: "lost key ignored DDL", lostKey: true, ignoreDDL: true, triggerEvent: true, tableEvent: true, dropTable: true},
		{name: "lost key ignored force replication", lostKey: true, ignoreDDL: true, forceReplicate: true, tableEvent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := *rawEvent
			if tc.lostKey {
				raw.Type = byte(model.ActionDropPrimaryKey)
				raw.Query = "ALTER TABLE test.a DROP PRIMARY KEY"
				raw.TableAcquiredReplicationKey = false
				raw.TableLostReplicationKey = true
				raw.ExtraTableInfo = common.WrapTableInfo(raw.SchemaName, raw.TableInfo)
				raw.TableInfo = raw.TableInfo.Clone()
				raw.TableInfo.PKIsHandle = false
				raw.TableInfo.Indices = nil
			}
			rawEvent := &raw
			var tableFilter filter.Filter
			if !tc.noFilter {
				cfg := &config.FilterConfig{Rules: []string{"test.*"}}
				if tc.excludeTable {
					cfg.Rules = append(cfg.Rules, "!test.a")
				}
				if tc.ignoreDDL {
					cfg.EventFilters = []*config.EventFilterRule{{
						Matcher: []string{"test.a"}, IgnoreEvent: []bf.EventType{bf.AddPrimaryKey, bf.DropPrimaryKey},
					}}
				}
				var err error
				tableFilter, err = filter.NewFilter(cfg, "", false, tc.forceReplicate)
				require.NoError(t, err)
			}
			for _, caller := range []struct {
				name    string
				tableID int64
				want    bool
			}{
				{"table trigger", common.DDLSpanTableID, tc.triggerEvent},
				{"table dispatcher", rawEvent.TableID, tc.tableEvent},
			} {
				t.Run(caller.name, func(t *testing.T) {
					event, ok, err := buildDDLEvent(rawEvent, tableFilter, caller.tableID)
					require.NoError(t, err)
					require.Equal(t, caller.want, ok)
					if !ok {
						return
					}
					require.Equal(t, rawEvent.Type, event.Type)
					require.Equal(t, rawEvent.Query, event.Query)
					require.Equal(t, rawEvent.FinishedTs, event.FinishedTs)
					require.Equal(t, tc.ignoreDDL, event.NotSync)
					if tc.addTable {
						// Ignoring the downstream DDL must not prevent scheduling the table.
						require.Len(t, event.NeedAddedTables, 1)
						require.Equal(t, rawEvent.TableID, event.NeedAddedTables[0].TableID)
						require.Equal(t, rawEvent.SchemaID, event.NeedAddedTables[0].SchemaID)
						require.Equal(t, []int64{common.DDLSpanTableID}, event.BlockedTables.TableIDs)
						require.NotNil(t, event.TableNameChange)
					} else if tc.dropTable {
						require.Empty(t, event.NeedAddedTables)
						require.Equal(t, []int64{rawEvent.TableID}, event.NeedDroppedTables.TableIDs)
						require.ElementsMatch(t, []int64{rawEvent.TableID, common.DDLSpanTableID}, event.BlockedTables.TableIDs)
						require.False(t, event.TableInfo.HasPKOrNotNullUK)
						require.Equal(t, []commonEvent.SchemaTableName{{SchemaName: "test", TableName: "a"}}, event.TableNameChange.DropName)
					} else {
						require.Empty(t, event.NeedAddedTables)
						require.Nil(t, event.NeedDroppedTables)
						require.Equal(t, []int64{rawEvent.TableID}, event.BlockedTables.TableIDs)
						require.Nil(t, event.TableNameChange)
					}
				})
			}
		})
	}
}

func TestDDLReplicationKeyTransitions(t *testing.T) {
	for _, tc := range []struct {
		name       string
		createSQL  string
		loseSQL    string
		acquireSQL string
		retainsKey bool
	}{
		{"primary key", "create table a (id bigint primary key nonclustered, v int)", "alter table a drop primary key", "alter table a add primary key (id) nonclustered", false},
		{"unique key", "create table a (id bigint not null, unique key uk(id), v int)", "alter table a drop index uk", "alter table a add unique key uk(id)", false},
		{"drop key column", "create table a (id bigint not null unique, v int)", "alter table a drop column id", "alter table a add primary key(v) nonclustered", false},
		{"nullable key", "create table a (id bigint not null unique, v int)", "alter table a modify column id bigint", "alter table a modify column id bigint not null", false},
		{"multi schema", "create table a (id bigint not null, unique key uk(id), v int)", "alter table a drop index uk, add column x int", "alter table a add unique key uk(id), drop column x", false},
		{"partition primary key", "create table a (id bigint primary key nonclustered, v int) partition by hash(id) partitions 2", "alter table a drop primary key", "alter table a add primary key (id) nonclustered", false},
		{"partition unique key", "create table a (id bigint not null, unique key uk(id), v int) partition by hash(id) partitions 2", "alter table a drop index uk", "alter table a add unique key uk(id)", false},
		{"another usable key", "create table a (id bigint primary key nonclustered, v bigint not null unique)", "alter table a drop primary key", "alter table a add primary key (id) nonclustered", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			helper := commonEvent.NewEventTestHelper(t)
			t.Cleanup(helper.Close)
			helper.Tk().MustExec("use test")
			create := helper.DDL2Job(tc.createSQL)
			lose := helper.DDL2Job(tc.loseSQL)
			acquire := helper.DDL2Job(tc.acquireSQL)
			path := t.TempDir()
			storage := newPersistentStorageForTest(path, []mockDBInfo{{
				dbInfo: &model.DBInfo{ID: create.SchemaID, Name: parser_model.NewCIStr("test")},
			}})
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			require.NoError(t, storage.handleDDLJob(create))
			physicalIDs := []int64{create.TableID}
			if isPartitionTable(create.BinlogInfo.TableInfo) {
				physicalIDs = getAllPartitionIDs(create.BinlogInfo.TableInfo)
			}
			for _, id := range physicalIDs {
				require.NoError(t, storage.registerTable(id, create.BinlogInfo.FinishedTS))
			}
			require.NoError(t, storage.handleDDLJob(lose))
			// Deliberately leave the old stores registered while the key is added
			// back, reproducing dispatchers surviving a previous key loss.
			require.NoError(t, storage.handleDDLJob(acquire))
			for _, reload := range []bool{false, true} {
				if reload {
					require.NoError(t, storage.close())
					storage = loadPersistentStorageFromPathForTest(path, acquire.BinlogInfo.FinishedTS+1)
				}
				snap := storage.db.NewSnapshot()
				loss := readPersistedDDLEvent(snap, lose.BinlogInfo.FinishedTS)
				gain := readPersistedDDLEvent(snap, acquire.BinlogInfo.FinishedTS)
				require.NoError(t, snap.Close())
				require.Equal(t, !tc.retainsKey, loss.TableLostReplicationKey)
				require.False(t, loss.TableAcquiredReplicationKey)
				require.Equal(t, !tc.retainsKey, gain.TableAcquiredReplicationKey)
				require.False(t, gain.TableLostReplicationKey)
				for _, forceReplicate := range []bool{false, true} {
					f, err := filter.NewFilter(&config.FilterConfig{Rules: []string{"test.*"}}, "", false, forceReplicate)
					require.NoError(t, err)
					events, err := storage.fetchTableTriggerDDLEvents(f, create.BinlogInfo.FinishedTS, 10)
					require.NoError(t, err)
					if forceReplicate || tc.retainsKey {
						require.Empty(t, events)
					} else {
						require.Len(t, events, 2)
						lossEvent := events[0]
						require.Equal(t, loss.Query, lossEvent.Query)
						require.False(t, lossEvent.NotSync)
						require.False(t, lossEvent.TableInfo.HasPKOrNotNullUK)
						require.ElementsMatch(t, physicalIDs, lossEvent.NeedDroppedTables.TableIDs)
						require.ElementsMatch(t, append(append([]int64{}, physicalIDs...), common.DDLSpanTableID), lossEvent.BlockedTables.TableIDs)
						require.Empty(t, lossEvent.NeedAddedTables)
						require.Len(t, events[1].NeedAddedTables, len(physicalIDs))
						require.Nil(t, events[1].NeedDroppedTables)
					}
					for _, id := range physicalIDs {
						tableEvents, err := storage.fetchTableDDLEvents(common.NewDispatcherID(), id, f, create.BinlogInfo.FinishedTS, acquire.BinlogInfo.FinishedTS)
						require.NoError(t, err)
						if forceReplicate || tc.retainsKey {
							require.Len(t, tableEvents, 2)
							for _, event := range tableEvents {
								require.Empty(t, event.NeedAddedTables)
								require.Nil(t, event.NeedDroppedTables)
							}
						} else {
							require.Len(t, tableEvents, 1)
							// The old dispatcher receives only the loss barrier, never
							// the re-acquisition DDL or its NeedAddedTables.
							require.Equal(t, events[0].FinishedTs, tableEvents[0].FinishedTs)
							require.Equal(t, events[0].BlockedTables, tableEvents[0].BlockedTables)
							require.Equal(t, events[0].NeedDroppedTables, tableEvents[0].NeedDroppedTables)
							require.Empty(t, tableEvents[0].NeedAddedTables)
						}
					}
				}
			}
		})
	}
}

func TestEnrichPersistedDDLEventLookupError(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	t.Cleanup(helper.Close)
	helper.Tk().MustExec("use test")
	a := helper.DDL2Job("create table a (id int not null)")
	p := helper.DDL2Job("create table p (id int not null) partition by hash(id) partitions 2")
	jobs := []*model.Job{
		helper.DDL2Job("alter table a add primary key(id) nonclustered"),
		helper.DDL2Job("alter table a drop primary key"),
		helper.DDL2Job("alter table p exchange partition p0 with table a without validation"),
	}
	for _, job := range jobs {
		t.Run(job.Type.String(), func(t *testing.T) {
			storage := newPersistentStorageForTest(t.TempDir(), []mockDBInfo{{
				dbInfo: &model.DBInfo{ID: job.SchemaID, Name: parser_model.NewCIStr("test")},
				tables: []*model.TableInfo{a.BinlogInfo.TableInfo, p.BinlogInfo.TableInfo},
			}})
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			// Make historical schemas unavailable, without acquiring p.mu in a
			// hook: handleDDLJob must release it before the lookup and return error.
			storage.gcTs = job.BinlogInfo.FinishedTS
			err := storage.handleDDLJob(job)
			require.ErrorIs(t, err, errors.ErrSnapshotLostByGC)
			key, err := ddlJobKey(job.BinlogInfo.FinishedTS)
			require.NoError(t, err)
			_, closer, err := storage.db.Get(key)
			if closer != nil {
				require.NoError(t, closer.Close())
			}
			require.ErrorIs(t, err, pebble.ErrNotFound)
			require.Empty(t, storage.tablesDDLHistory)
			require.Empty(t, storage.tableTriggerDDLHistory)
		})
	}
}

func TestDDLTableBecomesEligible(t *testing.T) {
	for _, tc := range []struct {
		name           string
		createSQL      string
		alterSQL       string
		becameEligible bool
	}{
		{"primary key", "create table a (pk bigint not null)", "alter table a add primary key (pk)", true},
		{"unique index", "create table a (pk bigint not null)", "create unique index uk on a (pk)", true},
		{"partition primary key", "create table a (pk bigint not null) partition by hash(pk) partitions 2", "alter table a add primary key (pk)", true},
		{"partition unique index", "create table a (pk bigint not null) partition by hash(pk) partitions 2", "create unique index uk on a (pk)", true},
		{"multi schema change", "create table a (pk bigint not null)", "alter table a add unique index uk (pk), add column v int", true},
		{"not null unique column", "create table a (pk bigint unique)", "alter table a modify column pk bigint not null", true},
		{"already eligible", "create table a (pk bigint primary key)", "create unique index uk on a (pk)", false},
		{"non unique index", "create table a (pk bigint not null)", "create index idx on a (pk)", false},
		{"nullable unique index", "create table a (pk bigint)", "create unique index uk on a (pk)", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			helper := commonEvent.NewEventTestHelper(t)
			t.Cleanup(helper.Close)
			helper.Tk().MustExec("use test")
			createJob := helper.DDL2Job(tc.createSQL)
			alterJob := helper.DDL2Job(tc.alterSQL)
			expectedQuery, err := transformDDLJobQuery(alterJob)
			require.NoError(t, err)
			likeJob := helper.DDL2Job("create table b like a")
			dbPath := t.TempDir()
			storage := newPersistentStorageForTest(dbPath, []mockDBInfo{{
				dbInfo: &model.DBInfo{ID: createJob.SchemaID, Name: parser_model.NewCIStr("test")},
			}})
			t.Cleanup(func() { require.NoError(t, storage.close()) })
			require.NoError(t, storage.handleDDLJob(createJob))
			require.NoError(t, storage.handleDDLJob(alterJob))
			require.NoError(t, storage.handleDDLJob(likeJob))

			for _, reload := range []bool{false, true} {
				if reload {
					require.NoError(t, storage.close())
					storage = loadPersistentStorageFromPathForTest(dbPath, likeJob.BinlogInfo.FinishedTS+1)
				}
				for _, forceReplicate := range []bool{false, true} {
					tableFilter, err := filter.NewFilter(&config.FilterConfig{Rules: []string{"test.*"}}, "", false, forceReplicate)
					require.NoError(t, err)
					events, err := storage.fetchTableTriggerDDLEvents(tableFilter, createJob.BinlogInfo.FinishedTS, 10)
					require.NoError(t, err)
					if !tableFilter.IsEligibleTable(common.WrapTableInfo("test", alterJob.BinlogInfo.TableInfo)) {
						require.Empty(t, events)
						continue
					}

					physicalIDs := []int64{alterJob.TableID}
					if isPartitionTable(alterJob.BinlogInfo.TableInfo) {
						physicalIDs = getAllPartitionIDs(alterJob.BinlogInfo.TableInfo)
					}
					if tc.becameEligible && !forceReplicate {
						require.Len(t, events, 2)
						require.Equal(t, byte(alterJob.Type), events[0].Type)
						require.Equal(t, expectedQuery, events[0].Query)
						require.Equal(t, alterJob.BinlogInfo.FinishedTS, events[0].FinishedTs)
						require.Equal(t, &commonEvent.InfluencedTables{
							InfluenceType: commonEvent.InfluenceTypeNormal,
							TableIDs:      []int64{common.DDLSpanTableID},
						}, events[0].BlockedTables)
						var expectedTables []commonEvent.Table
						for _, id := range physicalIDs {
							expectedTables = append(expectedTables, commonEvent.Table{
								SchemaID: alterJob.SchemaID, TableID: id, Splitable: isSplitable(alterJob.BinlogInfo.TableInfo),
							})
						}
						require.ElementsMatch(t, expectedTables, events[0].NeedAddedTables)
						require.Equal(t, &commonEvent.TableNameChange{
							AddName: []commonEvent.SchemaTableName{{SchemaName: "test", TableName: "a"}},
						}, events[0].TableNameChange)
						events = events[1:]
					} else {
						require.Len(t, events, 1)
					}
					require.Equal(t, byte(model.ActionCreateTable), events[0].Type)
					require.ElementsMatch(t, append(physicalIDs, common.DDLSpanTableID), events[0].BlockedTables.TableIDs)

					for _, id := range physicalIDs {
						if forceReplicate {
							// Existing dispatchers must receive the ALTER exactly once without adding tables again.
							tableEvents, err := storage.fetchTableDDLEvents(common.NewDispatcherID(), id, tableFilter,
								createJob.BinlogInfo.FinishedTS, alterJob.BinlogInfo.FinishedTS)
							require.NoError(t, err)
							require.Len(t, tableEvents, 1)
							require.Empty(t, tableEvents[0].NeedAddedTables)
							require.ElementsMatch(t, physicalIDs, tableEvents[0].BlockedTables.TableIDs)
						}
						// A newly registered dispatcher can load the eligible schema and participate in CREATE LIKE.
						require.NoError(t, storage.registerTable(id, alterJob.BinlogInfo.FinishedTS))
						info, err := storage.getTableInfo(id, alterJob.BinlogInfo.FinishedTS)
						require.NoError(t, err)
						require.True(t, tableFilter.IsEligibleTable(info))
						tableEvents, err := storage.fetchTableDDLEvents(common.NewDispatcherID(), id, tableFilter,
							alterJob.BinlogInfo.FinishedTS, likeJob.BinlogInfo.FinishedTS)
						require.NoError(t, err)
						require.Len(t, tableEvents, 1)
						require.Equal(t, events[0].BlockedTables, tableEvents[0].BlockedTables)
						require.NoError(t, storage.unregisterTable(id))
					}
				}
			}
		})
	}
}
