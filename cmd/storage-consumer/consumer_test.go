// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestGetNewFilesSkipsUnsupportedLegacyIndexPath(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	storage, err := putil.GetExternalStorageWithDefaultTimeout(ctx, fmt.Sprintf("file:///%s", t.TempDir()))
	require.NoError(t, err)
	require.NoError(t, storage.WriteFile(ctx, "test/binary_columns_dummy/2026-06-23/meta/CDC.index", []byte("CDC000001.csv\n")))

	dateSeparator := config.DateSeparatorDay.String()
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.DateSeparator = &dateSeparator

	c := &consumer{
		replicationCfg:  replicaConfig,
		externalStorage: storage,
		tableDMLIdxMap:  make(map[cloudstorage.DMLPathKey]fileIndexKeyMap),
	}

	var got map[cloudstorage.DMLPathKey]fileIndexRange
	require.NotPanics(t, func() {
		got, err = c.getNewFiles(ctx)
	})
	require.NoError(t, err)
	require.Empty(t, got)
}
