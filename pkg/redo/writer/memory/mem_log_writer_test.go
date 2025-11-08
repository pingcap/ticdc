//  Copyright 2023 PingCAP, Inc.
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

package memory

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestWriteDDL(t *testing.T) {
	t.Parallel()

	rows := []writer.RedoEvent{
		nil,
		&pevent.RedoRowEvent{
			CommitTs:  11,
			TableInfo: common.NewTableInfo4Decoder("test", &model.TableInfo{Name: ast.NewCIStr("t1")}),
		},
		&pevent.RedoRowEvent{
			CommitTs:  15,
			TableInfo: common.NewTableInfo4Decoder("test", &model.TableInfo{Name: ast.NewCIStr("t2")}),
		},
		&pevent.RedoRowEvent{
			CommitTs:  8,
			TableInfo: common.NewTableInfo4Decoder("test", &model.TableInfo{Name: ast.NewCIStr("t2")}),
		},
	}
	testWriteEvents(t, rows)
}

func TestWriteDML(t *testing.T) {
	t.Parallel()

	ddls := []writer.RedoEvent{
		nil,
		&pevent.DDLEvent{FinishedTs: 1},
		&pevent.DDLEvent{FinishedTs: 10},
		&pevent.DDLEvent{FinishedTs: 8},
	}
	testWriteEvents(t, ddls)
}

func testWriteEvents(t *testing.T, events []writer.RedoEvent) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	lwcfg := &writer.LogWriterConfig{
		CaptureID:          "test-capture",
		ChangeFeedID:       common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceNamme),
		URI:                uri,
		UseExternalStorage: true,
		MaxLogSizeInBytes:  10 * redo.Megabyte,
	}
	filename := t.Name()
	lw, err := NewLogWriter(ctx, lwcfg, redo.RedoDDLLogFileType, writer.WithLogFileName(func() string {
		return filename
	}))
	require.NoError(t, err)

	require.NoError(t, lw.WriteEvents(ctx, events...))

	// test flush
	err = extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		require.Equal(t, filename, path)
		return nil
	})
	require.NoError(t, err)

	require.ErrorIs(t, lw.Close(), nil)
	// duplicate close should return the same error
	require.ErrorIs(t, lw.Close(), nil)
}
