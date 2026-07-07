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

package spill

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRecordFileReadByHandle(t *testing.T) {
	store, err := NewRecordFile(t.TempDir(), "test-*.spill")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Cleanup())
	}()

	first, err := store.Append([]byte("first"))
	require.NoError(t, err)
	second, err := store.AppendChunks([]byte("sec"), []byte("ond"))
	require.NoError(t, err)

	data, err := store.Read(second)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), data)
	data, err = store.Read(first)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), data)
}

func TestRecordFileSequentialReader(t *testing.T) {
	store, err := NewRecordFile(t.TempDir(), "test-*.spill")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, store.Cleanup())
	}()

	_, err = store.Append([]byte("first"))
	require.NoError(t, err)
	_, err = store.Append([]byte("second"))
	require.NoError(t, err)
	require.NoError(t, store.Close())

	reader, err := store.NewReader()
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()

	data, err := reader.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("first"), data)
	data, err = reader.Next()
	require.NoError(t, err)
	require.Equal(t, []byte("second"), data)
	data, err = reader.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Nil(t, data)
}

func TestRecordFileCreatesDirAndCleansUp(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "spill")
	store, err := NewRecordFile(dir, "test-*.spill")
	require.NoError(t, err)

	require.DirExists(t, dir)
	path := store.Path()
	require.NoError(t, store.Cleanup())
	require.NoError(t, store.Cleanup())

	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
}
