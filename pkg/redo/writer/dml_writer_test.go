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
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/sink/spool"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/stretchr/testify/require"
)

func TestNewDMLWriter(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, uri, err := util.GetTestExtStorage(ctx, t.TempDir())
	require.NoError(t, err)
	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)
	consistentCfg := testutil.NewConsistentConfig(uri.String())
	spoolBaseDir := t.TempDir()
	consistentCfg.SpoolBaseDir = util.AddressOf(spoolBaseDir)
	consistentCfg.SpoolDiskQuota = util.AddressOf(int64(1024))
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	lw, err := NewDMLWriter(ctx, cfg)
	require.NoError(t, err)
	spoolDir := filepath.Join(spoolBaseDir, changefeedID.Keyspace(), changefeedID.Name())
	require.DirExists(t, spoolDir)
	require.NoError(t, lw.Close())
	require.NoDirExists(t, spoolDir)
}

func TestRedoSpoolMemoryRatio(t *testing.T) {
	t.Parallel()

	require.Equal(t, 0.025, redoSpoolMemoryRatio(0))
	require.Equal(t, 0.025, redoSpoolMemoryRatio(-1))
	require.Equal(t, defaultRedoSpoolMemoryRatio, redoSpoolMemoryRatio(1024*1024*1024))
	require.Equal(t, 0.025, redoSpoolMemoryRatio(10*1024*1024*1024))
}

func TestDMLWriterSpoolsEncodedBytesBeforePostEnqueue(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	spoolBuffer, err := spool.New(
		changefeedID,
		spool.WithRootDir(t.TempDir()),
		spool.WithDiskQuotaBytes(1000),
		spool.WithSegmentBytes(1<<20),
		spool.WithMemoryRatio(0.2),
		spool.WithHighWatermarkRatio(0.6),
		spool.WithLowWatermarkRatio(0.3),
	)
	require.NoError(t, err)
	defer spoolBuffer.Close()

	encodedCh := make(chan *polymorphicRedoEvent, 2)
	dmlWriter := &dmlWriter{
		encodeWorkers: &encodingWorkerGroup{outputCh: encodedCh},
		spool:         spoolBuffer,
		spoolEntries:  chann.NewUnlimitedChannelDefault[*redoSpoolEntry](),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- dmlWriter.writeEncodedEventsToSpool(ctx)
	}()

	var firstEnqueued atomic.Int64
	var secondEnqueued atomic.Int64
	firstData := []byte(strings.Repeat("a", 350))
	secondData := []byte(strings.Repeat("b", 350))
	encodedCh <- &polymorphicRedoEvent{
		commitTs:    1,
		data:        firstData,
		postEnqueue: func() { firstEnqueued.Add(1) },
	}
	encodedCh <- &polymorphicRedoEvent{
		commitTs:    2,
		data:        secondData,
		postEnqueue: func() { secondEnqueued.Add(1) },
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	firstEntry, ok, err := dmlWriter.spoolEntries.GetWithContext(readCtx)
	require.NoError(t, err)
	require.True(t, ok)
	secondEntry, ok, err := dmlWriter.spoolEntries.GetWithContext(readCtx)
	require.NoError(t, err)
	require.True(t, ok)

	require.True(t, firstEntry.entry.IsSpilled())
	require.True(t, secondEntry.entry.IsSpilled())
	require.False(t, firstEntry.flushImmediately)
	require.False(t, secondEntry.flushImmediately)
	require.Equal(t, int64(1), firstEnqueued.Load())
	require.Equal(t, int64(0), secondEnqueued.Load())

	reader, err := spoolBuffer.NewMessageReader(firstEntry.entry)
	require.NoError(t, err)
	_, encodedData, _, ok, err := reader.Next()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, firstData, encodedData)

	spoolBuffer.Release(firstEntry.entry)
	require.Equal(t, int64(0), secondEnqueued.Load())
	spoolBuffer.Release(secondEntry.entry)
	require.Equal(t, int64(1), secondEnqueued.Load())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestDMLWriterMarksOversizedEncodedBytesForImmediateFlush(t *testing.T) {
	changefeedID := common.NewChangeFeedIDWithName(t.Name(), common.DefaultKeyspaceName)
	spoolBuffer, err := spool.New(
		changefeedID,
		spool.WithRootDir(t.TempDir()),
		spool.WithDiskQuotaBytes(100),
	)
	require.NoError(t, err)
	defer spoolBuffer.Close()

	encodedCh := make(chan *polymorphicRedoEvent, 1)
	dmlWriter := &dmlWriter{
		encodeWorkers: &encodingWorkerGroup{outputCh: encodedCh},
		spool:         spoolBuffer,
		spoolEntries:  chann.NewUnlimitedChannelDefault[*redoSpoolEntry](),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- dmlWriter.writeEncodedEventsToSpool(ctx)
	}()

	encodedCh <- &polymorphicRedoEvent{
		commitTs: 1,
		data:     []byte(strings.Repeat("a", 200)),
	}
	readCtx, readCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readCancel()
	entry, ok, err := dmlWriter.spoolEntries.GetWithContext(readCtx)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, entry.entry.InMemory())
	require.True(t, entry.flushImmediately)
	spoolBuffer.Release(entry.entry)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
