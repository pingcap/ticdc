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
	"github.com/pingcap/tidb/pkg/kv"
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
