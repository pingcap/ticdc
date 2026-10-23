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

package applier

import (
	"context"
	"os"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func newStorageTestInsertEvent(commitTs uint64, id int64) *commonEvent.RedoDMLEvent {
	return &commonEvent.RedoDMLEvent{
		Row: &commonEvent.DMLEventInRedoLog{
			StartTs: commitTs - 1, CommitTs: commitTs,
			Table:   &common.TableName{Schema: "test", Table: "t", TableID: 1},
			Columns: []*commonEvent.RedoColumn{{Name: "id", Type: mysql.TypeLong}},
		},
		Columns: []commonEvent.RedoColumnValue{{Value: id}},
	}
}

func TestTxnInsertStorageClosesFilesAfterDrain(t *testing.T) {
	storage := newTempTxnInsertEventStorage(2, t.TempDir())
	t.Cleanup(func() {
		if storage.writingFile != nil {
			_ = storage.writingFile.Close()
		}
		if storage.readingFile != nil {
			_ = storage.readingFile.Close()
		}
	})
	for txn := uint64(1); txn <= 3; txn++ {
		for i := range int64(3) {
			require.NoError(t, storage.addEvent(newStorageTestInsertEvent(txn, i)))
		}
		writingFile := storage.writingFile
		require.NotNil(t, writingFile)
		var readingFile *os.File
		for i := range int64(3) {
			event, err := storage.readNextEvent()
			require.NoError(t, err)
			require.EqualValues(t, i, event.Columns[0].Value)
			if i == 0 {
				_, err = writingFile.Stat()
				require.ErrorIs(t, err, os.ErrClosed)
				readingFile = storage.readingFile
				require.NotNil(t, readingFile)
			}
		}
		_, err := readingFile.Stat()
		require.ErrorIs(t, err, os.ErrClosed)
		require.Nil(t, storage.writingFile)
		require.Nil(t, storage.readingFile)
		require.False(t, storage.hasEvent())
	}
}

func TestConsumeLogsClosesTxnStorageOnCancel(t *testing.T) {
	for _, reading := range []bool{false, true} {
		name := "writing"
		if reading {
			name = "reading"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			ap := NewRedoApplier(&RedoApplierConfig{Dir: dir})
			ap.rd = NewMockReader(100, 130, nil, nil)
			ap.updateSplitter = newUpdateEventSplitter(ap.rd, dir)
			storage := ap.updateSplitter.tempStorage
			storage.flushThreshold = 2
			for i := range int64(3) {
				require.NoError(t, storage.addEvent(newStorageTestInsertEvent(120, i)))
			}
			file := storage.writingFile
			if reading {
				_, err := storage.readNextEvent()
				require.NoError(t, err)
				file = storage.readingFile
			}
			require.NotNil(t, file)
			t.Cleanup(func() { _ = file.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.ErrorIs(t, ap.consumeLogs(ctx), context.Canceled)
			_, err := file.Stat()
			require.ErrorIs(t, err, os.ErrClosed)
			require.Nil(t, storage.writingFile)
			require.Nil(t, storage.readingFile)
		})
	}
}
