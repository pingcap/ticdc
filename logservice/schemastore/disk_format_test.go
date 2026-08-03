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
	"context"
	"testing"

	"github.com/cockroachdb/pebble"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	parser_model "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/stretchr/testify/require"
)

func TestPersistSchemaSnapshotReturnsWhenListDatabasesSnapshotLost(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer db.Close()

	snapshotLostErr := snapshotLostByGCError{}
	_, _, _, err = persistSchemaSnapshot(db, &snapshotLostStorage{err: snapshotLostErr}, 100, true)
	require.ErrorIs(t, err, snapshotLostErr)
}

func TestGetAllPhysicalTablesSkipsViews(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	const snapshotTs = uint64(100)
	dbInfo := &model.DBInfo{ID: 100, Name: parser_model.NewCIStr("test")}
	tableInfo := newEligibleTableInfoForTest(200, "t1")
	viewInfo := &model.TableInfo{
		ID:   201,
		Name: parser_model.NewCIStr("v1"),
		View: &model.ViewInfo{},
	}
	mockWriteKVSnapOnDisk(db, snapshotTs, []mockDBInfo{
		{
			dbInfo: dbInfo,
			tables: []*model.TableInfo{tableInfo, viewInfo},
		},
	})

	snapshot := db.NewSnapshot()
	defer func() {
		require.NoError(t, snapshot.Close())
	}()

	tables, err := loadAllPhysicalTablesAtTs(snapshot, snapshotTs, snapshotTs, nil)
	require.NoError(t, err)
	require.Equal(t, []commonEvent.Table{
		{
			SchemaID:  100,
			TableID:   200,
			Splitable: true,
			SchemaTableName: &commonEvent.SchemaTableName{
				SchemaName: "test",
				TableName:  "t1",
			},
		},
	}, tables)
}

type snapshotLostByGCError struct{}

func (snapshotLostByGCError) Error() string {
	return "GC life time is shorter than transaction duration"
}

type snapshotLostStorage struct {
	kv.Storage
	err error
}

func (s *snapshotLostStorage) GetSnapshot(kv.Version) kv.Snapshot {
	return &snapshotLostSnapshot{err: s.err}
}

type snapshotLostSnapshot struct {
	err error
}

func (s *snapshotLostSnapshot) Get(context.Context, kv.Key, ...kv.GetOption) (kv.ValueEntry, error) {
	return kv.ValueEntry{}, s.err
}

func (s *snapshotLostSnapshot) Iter(kv.Key, kv.Key) (kv.Iterator, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) IterReverse(kv.Key, kv.Key) (kv.Iterator, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) BatchGet(context.Context, []kv.Key, ...kv.BatchGetOption) (map[string]kv.ValueEntry, error) {
	return nil, s.err
}

func (s *snapshotLostSnapshot) SetOption(int, any) {}
