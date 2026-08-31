// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package writer

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/mockobjstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestDDLWriterCloseStorageOnce(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockStorage := mockobjstore.NewMockStorage(ctrl)
	mockStorage.EXPECT().Close().Times(1)

	oldInitExternalStorage := redo.InitExternalStorage
	t.Cleanup(func() {
		redo.InitExternalStorage = oldInitExternalStorage
	})
	redo.InitExternalStorage = func(context.Context, url.URL) (storeapi.Storage, error) {
		return mockStorage, nil
	}

	cfg, err := NewConfig(
		common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName),
		testutil.NewConsistentConfig("file:///tmp/redo"),
	)
	require.NoError(t, err)
	lw, err := NewDDLWriter(t.Context(), cfg)
	require.NoError(t, err)
	require.NoError(t, lw.Close())
	require.NoError(t, lw.Close())
}

func TestWriteDDL(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extStorage, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	cfg, err := NewConfig(
		common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		testutil.NewConsistentConfig(uri.String()),
	)
	require.NoError(t, err)

	filename := t.Name()
	lw, err := NewDDLWriter(ctx, cfg, WithLogFileName(func() string {
		return filename
	}))
	require.NoError(t, err)

	ddls := []*pevent.DDLEvent{
		nil,
		{FinishedTs: 1},
		{FinishedTs: 10},
		{FinishedTs: 8},
	}
	for _, ddl := range ddls {
		require.NoError(t, lw.WriteDDLEvent(ctx, ddl))
	}

	err = extStorage.WalkDir(ctx, nil, func(path string, size int64) error {
		require.Equal(t, filename, path)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, lw.Close())
	require.NoError(t, lw.Close())
}
