// Copyright 2023 PingCAP, Inc.
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

package memory

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewDMLWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	cfg, err := writer.NewConfig(
		common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName),
		testutil.NewConsistentConfig(uri.String()),
	)
	require.NoError(t, err)

	lw, err := NewDMLWriter(ctx, cfg)
	require.NoError(t, err)
	require.NoError(t, lw.Close())
}
