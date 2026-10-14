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

package memory

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
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

// TestDMLWriterCancelReportsCanceled verifies that a writer stopped by context
// cancellation reports the cancellation from Run and AddDMLEvents instead of
// ErrRedoWriterStopped. The sink may still hand over buffered rows after the run
// loop exited, and those writes must not hide why the writer stopped.
func TestDMLWriterCancelReportsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	_, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	cfg, err := writer.NewConfig(changefeedID, testutil.NewConsistentConfig(uri.String()))
	require.NoError(t, err)

	lw, err := NewDMLWriter(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, lw.Close())
	})

	runDone := make(chan error, 1)
	go func() {
		runDone <- lw.Run(ctx)
	}()

	cancel()
	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "dml writer did not stop after context cancellation")
	}
	require.ErrorIs(t, lw.AddDMLEvents(ctx, &commonEvent.RedoRowEvent{}), context.Canceled)
}
