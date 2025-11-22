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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/codec"
	misc "github.com/pingcap/ticdc/pkg/redo/common"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/file"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func genLogFile(
	ctx context.Context, t *testing.T,
	dir string, logType string,
	minCommitTs, maxCommitTs uint64,
) {
	cfg := &writer.LogWriterConfig{
		MaxLogSizeInBytes: 100000,
		Dir:               dir,
	}
	fileName := fmt.Sprintf(redo.RedoLogFileFormatV2, "capture", "default",
		"changefeed", logType, maxCommitTs, uuid.NewString(), redo.LogEXT)
	w, err := file.NewFileWriter(ctx, cfg, logType, writer.WithLogFileName(func() string {
		return fileName
	}))
	require.Nil(t, err)
	if logType == redo.RedoRowLogFileType {
		// generate unsorted logs
		for ts := maxCommitTs; ts >= minCommitTs; ts-- {
			event := &pevent.RedoRowEvent{
				CommitTs: ts,
				TableInfo: common.NewTableInfo4Decoder("test", &model.TableInfo{
					Name: ast.NewCIStr("t"),
				}),
			}
			log := event.ToRedoLog()
			rawData, err := codec.MarshalRedoLog(log, nil)
			require.Nil(t, err)
			_, err = w.Write(rawData)
			require.Nil(t, err)
		}
	} else if logType == redo.RedoDDLLogFileType {
		event := &pevent.DDLEvent{
			FinishedTs: maxCommitTs,
			TableInfo:  &common.TableInfo{},
		}
		log := event.ToRedoLog()
		rawData, err := codec.MarshalRedoLog(log, nil)
		require.Nil(t, err)
		_, err = w.Write(rawData)
		require.Nil(t, err)
	}
	err = w.Close()
	require.Nil(t, err)
}

func TestReadLogs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	meta := &misc.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   100,
	}
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, 12, 12)
		genLogFile(ctx, t, dir, logType, meta.ResolvedTs, meta.ResolvedTs)
	}
	expectedRows := []uint64{12, meta.ResolvedTs}
	expectedDDLs := []uint64{meta.CheckpointTs, meta.CheckpointTs, 12, meta.ResolvedTs}

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	r := &LogReader{
		cfg: &LogReaderConfig{
			Dir:                t.TempDir(),
			URI:                *uri,
			UseExternalStorage: true,
		},
		meta:  meta,
		rowCh: make(chan *pevent.RedoDMLEvent, defaultReaderChanSize),
		ddlCh: make(chan *pevent.RedoDDLEvent, defaultReaderChanSize),
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return r.Run(egCtx)
	})

	for _, ts := range expectedRows {
		row, err := r.ReadNextRow(egCtx)
		require.NoError(t, err)
		require.Equal(t, ts, row.Row.CommitTs)
	}
	for _, ts := range expectedDDLs {
		ddl, err := r.ReadNextDDL(egCtx)
		require.NoError(t, err)
		require.Equal(t, ts, ddl.DDL.CommitTs)
	}

	cancel()
	require.ErrorIs(t, eg.Wait(), nil)
}

func TestLogReaderClose(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())

	meta := &misc.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   100,
	}
	for _, logType := range []string{redo.RedoRowLogFileType, redo.RedoDDLLogFileType} {
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, meta.CheckpointTs, meta.CheckpointTs)
		genLogFile(ctx, t, dir, logType, 12, 12)
		genLogFile(ctx, t, dir, logType, meta.ResolvedTs, meta.CheckpointTs)
	}

	uri, err := url.Parse(fmt.Sprintf("file://%s", dir))
	require.NoError(t, err)
	r := &LogReader{
		cfg: &LogReaderConfig{
			Dir:                t.TempDir(),
			URI:                *uri,
			UseExternalStorage: true,
		},
		meta:  meta,
		rowCh: make(chan *pevent.RedoDMLEvent, 1),
		ddlCh: make(chan *pevent.RedoDDLEvent, 1),
	}
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return r.Run(egCtx)
	})

	time.Sleep(2 * time.Second)
	cancel()
	require.ErrorIs(t, eg.Wait(), context.Canceled)
}

func TestNewLogReaderAndReadMeta(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	genMetaFile(t, dir, &misc.LogMeta{
		CheckpointTs: 11,
		ResolvedTs:   22,
	})
	genMetaFile(t, dir, &misc.LogMeta{
		CheckpointTs: 12,
		ResolvedTs:   21,
	})

	tests := []struct {
		name                             string
		dir                              string
		wantCheckpointTs, wantResolvedTs uint64
		wantErr                          string
	}{
		{
			name:             "happy",
			dir:              dir,
			wantCheckpointTs: 12,
			wantResolvedTs:   22,
		},
		{
			name:    "no meta file",
			dir:     t.TempDir(),
			wantErr: ".*no redo meta file found in dir*.",
		},
		{
			name:    "wrong dir",
			dir:     "xxx",
			wantErr: ".*fail to open storage for redo log*.",
		},
		{
			name:             "context cancel",
			dir:              dir,
			wantCheckpointTs: 12,
			wantResolvedTs:   22,
			wantErr:          context.Canceled.Error(),
		},
	}
	for _, tt := range tests {
		ctx := context.Background()
		if tt.name == "context cancel" {
			ctx1, cancel := context.WithCancel(context.Background())
			cancel()
			ctx = ctx1
		}
		uriStr := fmt.Sprintf("file://%s", tt.dir)
		uri, err := url.Parse(uriStr)
		require.Nil(t, err)
		l, err := newLogReader(ctx, &LogReaderConfig{
			Dir:                t.TempDir(),
			URI:                *uri,
			UseExternalStorage: redo.IsExternalStorage(uri.Scheme),
		})
		if tt.wantErr != "" {
			require.Regexp(t, tt.wantErr, err, tt.name)
		} else {
			require.Nil(t, err, tt.name)
			cts, rts, _, err := l.ReadMeta(ctx)
			require.Nil(t, err, tt.name)
			require.Equal(t, tt.wantCheckpointTs, cts, tt.name)
			require.Equal(t, tt.wantResolvedTs, rts, tt.name)
		}
	}
}

func genMetaFile(t *testing.T, dir string, meta *misc.LogMeta) {
	fileName := fmt.Sprintf(redo.RedoMetaFileFormat, "capture", "default",
		"changefeed", redo.RedoMetaFileType, uuid.NewString(), redo.MetaEXT)
	path := filepath.Join(dir, fileName)
	f, err := os.Create(path)
	require.Nil(t, err)
	data, err := meta.MarshalMsg(nil)
	require.Nil(t, err)
	_, err = f.Write(data)
	require.Nil(t, err)
}

func TestLogHeapLess(t *testing.T) {
	tests := []struct {
		name   string
		h      logHeap
		i      int
		j      int
		expect bool
	}{
		{
			name: "Delete before Update",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Update before Insert",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Update before Delete",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: false,
		},
		{
			name: "Update before Update",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Delete before Insert",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Same type of operations, different commit ts",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 200,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Same type of operations, same commit ts, different startTs",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								StartTs:  80,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								StartTs:  90,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
		{
			name: "Same type of operations, same commit ts",
			h: logHeap{
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
				{
					data: &pevent.RedoLog{
						Type: pevent.RedoLogTypeRow,
						RedoRow: &pevent.RedoDMLEvent{
							Row: &pevent.DMLEventInRedoLog{
								CommitTs: 100,
								Table: &common.TableName{
									Schema:      "test",
									Table:       "table",
									TableID:     1,
									IsPartition: false,
								},
								PreColumns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
								Columns: []*pevent.RedoColumn{
									{
										Name: "col-1",
									}, {
										Name: "col-2",
									},
								},
							},
						},
					},
				},
			},
			i:      0,
			j:      1,
			expect: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.h.Less(tt.i, tt.j); got != tt.expect {
				t.Errorf("logHeap.Less() = %v, want %v", got, tt.expect)
			}
		})
	}
}
