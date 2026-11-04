//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package reader

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/log"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	misc "github.com/pingcap/ticdc/pkg/redo/common"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestReadTruncatedRedoFile(t *testing.T) {
	remote := t.TempDir()
	genLogFile(t.Context(), t, remote, redo.RedoRowLogFileType, 11, 12)
	paths, err := filepath.Glob(filepath.Join(remote, "*.log"))
	require.NoError(t, err)
	require.Len(t, paths, 1)
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	recBytes, padBytes := decodeFrameSize(int64(binary.LittleEndian.Uint64(data[:8])))
	lastFrame := frameSizeBytes + int(recBytes+padBytes)

	for _, temporary := range []bool{false, true} {
		for _, damage := range []string{"empty", "partial header", "missing body", "partial body", "torn body"} {
			t.Run(fmt.Sprintf("temporary=%t/%s", temporary, damage), func(t *testing.T) {
				damaged := append([]byte(nil), data...)
				switch damage {
				case "empty":
					damaged = nil
				case "partial header":
					damaged = damaged[:lastFrame+4]
				case "missing body":
					damaged = damaged[:lastFrame+8]
				case "partial body":
					damaged = damaged[:len(damaged)-1]
				case "torn body":
					clear(damaged[lastFrame+8:])
				}
				name := filepath.Base(paths[0])
				if temporary {
					name += redo.TmpEXT
				}
				storageDir := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(storageDir, name), damaged, 0o600))
				uri, err := url.Parse("file://" + storageDir)
				require.NoError(t, err)
				rd := &LogReader{
					cfg:   &LogReaderConfig{Dir: t.TempDir(), URI: *uri, UseExternalStorage: true},
					meta:  &misc.LogMeta{CheckpointTs: 10, ResolvedTs: 12},
					rowCh: make(chan *pevent.RedoDMLEvent, 4),
				}
				err = rd.runRowReader(t.Context())
				if !temporary {
					if damage == "torn body" {
						require.ErrorIs(t, err, errors.ErrUnmarshalFailed)
					} else {
						require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					}
					require.NotErrorIs(t, err, io.EOF)
					return
				}
				require.NoError(t, err)
				var commits []uint64
				for row := range rd.rowCh {
					commits = append(commits, row.Row.CommitTs)
				}
				if damage == "empty" {
					require.Empty(t, commits)
				} else {
					require.Equal(t, []uint64{12}, commits)
				}
			})
		}
	}
}

func TestReaderNewReader(t *testing.T) {
	_, err := newReaders(context.Background(), nil)
	require.NotNil(t, err)

	dir := t.TempDir()
	require.Panics(t, func() {
		_, err = newReaders(context.Background(), &readerConfig{dir: dir})
	})
}

func TestFileReaderRead(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	cfg := &readerConfig{
		dir:                t.TempDir(),
		startTs:            10,
		endTs:              12,
		fileType:           redo.RedoRowLogFileType,
		uri:                *uri,
		useExternalStorage: true,
	}
	// log file with maxCommitTs<=startTs, fileter when download file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 1, cfg.startTs)
	// normal log file, include [10, 11, 12] and [11, 12, ... 20]
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.startTs, cfg.endTs+2)
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, cfg.endTs-1, 20)
	// log file with minCommitTs>endTs, filtered when sort file
	genLogFile(ctx, t, dir, redo.RedoRowLogFileType, 2000, 2023)

	log.Info("start to read redo log files")
	readers, err := newReaders(ctx, cfg)
	require.NoError(t, err)
	require.Equal(t, 2, len(readers))
	defer readers[0].Close() //nolint:errcheck

	for _, r := range readers {
		log, err := r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 11, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.NoError(t, err)
		require.EqualValues(t, 12, log.RedoRow.Row.CommitTs)
		log, err = r.Read()
		require.Nil(t, log)
		require.ErrorIs(t, err, io.EOF)
		require.NoError(t, r.Close())
	}
}

func TestDownLoadAndSortFilesClosesExternalStorage(t *testing.T) {
	controller := gomock.NewController(t)
	mockStorage := mockstorage.NewMockExternalStorage(controller)
	mockStorage.EXPECT().WalkDir(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, opt *storage.WalkOption, fn func(string, int64) error) error {
			return nil
		})
	mockStorage.EXPECT().Close().Times(1)

	oldInitExternalStorage := redo.InitExternalStorage
	defer func() {
		redo.InitExternalStorage = oldInitExternalStorage
	}()
	redo.InitExternalStorage = func(context.Context, url.URL) (storage.ExternalStorage, error) {
		return mockStorage, nil
	}

	uri, err := url.Parse("file:///tmp/redo-test")
	require.NoError(t, err)

	files, err := downLoadAndSortFiles(context.Background(), &readerConfig{
		dir:        t.TempDir(),
		startTs:    10,
		fileType:   redo.RedoRowLogFileType,
		uri:        *uri,
		workerNums: 1,
	})
	require.NoError(t, err)
	require.Empty(t, files)
}
