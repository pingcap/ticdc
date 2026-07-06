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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestLargeTxnInsertSpillReadOrder(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir())
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
		require.NoError(t, spill.Append(entry))
	}

	reader, err := spill.NewReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	for _, expected := range entries {
		actual, err := reader.Next()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
	actual, err := reader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, actual)
}

func TestLargeTxnInsertSpillCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data-dir", largeTxnInsertSpillDirName)

	spill, err := newLargeTxnInsertSpill(dir)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()

	require.DirExists(t, dir)
	require.Equal(t, dir, filepath.Dir(spill.path))
}

func TestLargeTxnInsertSpillCleanup(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, spill.Append(newTestSpillRawKVEntry(1)))

	path := spill.path
	require.NoError(t, spill.Cleanup())
	require.NoError(t, spill.Cleanup())

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))

	reader, err := spill.NewReader()
	require.Error(t, err)
	require.Nil(t, reader)
}

func TestLargeTxnInsertSpillEmpty(t *testing.T) {
	spill, err := newLargeTxnInsertSpill(t.TempDir())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, spill.Cleanup())
	}()

	reader, err := spill.NewReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	entry, err := reader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, entry)
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
