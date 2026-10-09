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

package eventservice

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestLargeTxnInsertSpillReadOrder(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir(), 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()

	entries := []*common.RawKVEntry{
		newTestSpillRawKVEntry(1),
		newTestSpillRawKVEntry(2),
		newTestSpillRawKVEntry(3),
	}
	for _, entry := range entries {
		require.NoError(t, spill.Append(context.Background(), entry))
	}

	reader, err := spill.NewReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	for _, expected := range entries {
		actual, err := reader.Next(context.Background())
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	actual, err := reader.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, actual)
}

func TestLargeTxnInsertSpillCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data-dir", largeTxnInsertSpillDirName)

	spill, err := newLargeTxnInsertSpill(dir, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()

	require.DirExists(t, dir)
	require.Equal(t, dir, filepath.Dir(spill.file.Path()))
}

func TestLargeTxnInsertSpillCleanup(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir(), 0)
	require.NoError(t, err)
	require.NoError(t, spill.Append(context.Background(), newTestSpillRawKVEntry(1)))

	path := spill.file.Path()
	require.NoError(t, spill.Cleanup())
	require.NoError(t, spill.Cleanup())

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))

	reader, err := spill.NewReader()
	require.Error(t, err)
	require.Nil(t, reader)
}

func TestLargeTxnInsertSpillEmpty(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir(), 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()

	reader, err := spill.NewReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	entry, err := reader.Next(context.Background())
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, entry)
}

func TestLargeTxnInsertSpillValidationErrors(t *testing.T) {
	spill, err := newLargeTxnInsertSpill("", 0)
	require.True(t, errors.ErrSpillFileOp.Equal(err))
	require.Nil(t, spill)

	spill, err = newLargeTxnInsertSpill(t.TempDir(), 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()
	require.True(t, errors.ErrSpillFileOp.Equal(spill.Append(context.Background(), nil)))
}

func TestLargeTxnStateValidationErrors(t *testing.T) {
	cleanedState := &largeTxnScanState{cleaned: true}
	err := cleanedState.appendInsert(context.Background(), newTestSpillRawKVEntry(1))
	require.True(t, errors.ErrSpillFileOp.Equal(err))
	_, err = cleanedState.nextInsert(context.Background())
	require.True(t, errors.ErrSpillFileOp.Equal(err))

	drainingState := &largeTxnScanState{phase: largeTxnScanPhaseDrainInserts}
	err = drainingState.appendInsert(context.Background(), newTestSpillRawKVEntry(1))
	require.True(t, errors.ErrSpillFileOp.Equal(err))

	processor := &dmlProcessor{}
	_, err = processor.getOrCreateLargeTxnState()
	require.True(t, errors.ErrSpillFileOp.Equal(err))
}

func TestCleanupLargeTxnInsertSpillFiles(t *testing.T) {
	dir := t.TempDir()
	orphanPaths := []string{
		filepath.Join(dir, "eventservice-large-txn-insert-1.spill"),
		filepath.Join(dir, "eventservice-large-txn-insert-2.spill"),
	}
	for _, path := range orphanPaths {
		require.NoError(t, os.WriteFile(path, []byte("orphan"), 0o600))
	}
	keepPath := filepath.Join(dir, "unrelated.spill")
	require.NoError(t, os.WriteFile(keepPath, []byte("keep"), 0o600))

	removed, err := cleanupLargeTxnInsertSpillFiles(dir)
	require.NoError(t, err)
	require.Equal(t, len(orphanPaths), removed)
	for _, path := range orphanPaths {
		require.NoFileExists(t, path)
	}
	require.FileExists(t, keepPath)
}

func TestLargeTxnStateUsesOperationContext(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir(), 0)
	require.NoError(t, err)
	state := &largeTxnScanState{spill: spill}
	t.Cleanup(func() {
		require.NoError(t, state.cleanup())
	})

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, state.appendInsert(canceledCtx, newTestSpillRawKVEntry(1)), context.Canceled)

	require.NoError(t, state.appendInsert(context.Background(), newTestSpillRawKVEntry(2)))
	_, err = state.nextInsert(canceledCtx)
	require.ErrorIs(t, err, context.Canceled)
}

func newTestSpillRawKVEntry(index int) *common.RawKVEntry {
	return &common.RawKVEntry{
		OpType:   common.OpTypePut,
		CRTs:     100,
		StartTs:  90,
		RegionID: uint64(index),
		Key:      fmt.Appendf(nil, "key-%02d", index),
		Value:    fmt.Appendf(nil, "value-%02d", index),
	}
}
