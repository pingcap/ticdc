// Copyright 2025 PingCAP, Inc.
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

package spool

import (
	"sync/atomic"
	"testing"

	commonType "github.com/pingcap/ticdc/pkg/common"
	codeccommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func newTestMessage(value string, rows int) *codeccommon.Message {
	msg := codeccommon.NewMsg(nil, []byte(value))
	msg.SetRowsCount(rows)
	return msg
}

func TestSuppressAndResumeWake(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	options := &Options{
		RootDir:            t.TempDir(),
		SegmentBytes:       1 << 20,
		MemoryRatio:        0.99,
		HighWatermarkRatio: 0.6,
		LowWatermarkRatio:  0.3,
	}
	manager, err := New(changefeedID, 1000, options)
	require.NoError(t, err)
	defer manager.Close()

	var wakeCount int64
	incWake := func() {
		atomic.AddInt64(&wakeCount, 1)
	}

	entry1, err := manager.Enqueue([]*codeccommon.Message{
		newTestMessage(string(make([]byte, 350)), 1),
	}, incWake)
	require.NoError(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	entry2, err := manager.Enqueue([]*codeccommon.Message{
		newTestMessage(string(make([]byte, 450)), 1),
	}, incWake)
	require.NoError(t, err)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	manager.Release(entry1)
	require.Equal(t, int64(1), atomic.LoadInt64(&wakeCount))

	manager.Release(entry2)
	require.Equal(t, int64(2), atomic.LoadInt64(&wakeCount))
}

func TestSpillAndReadBack(t *testing.T) {
	t.Parallel()

	changefeedID := commonType.NewChangefeedID4Test("test", "spool")
	options := &Options{
		RootDir:            t.TempDir(),
		SegmentBytes:       1 << 20,
		MemoryRatio:        0.01,
		HighWatermarkRatio: 0.95,
		LowWatermarkRatio:  0.7,
	}
	manager, err := New(changefeedID, 64, options)
	require.NoError(t, err)
	defer manager.Close()

	msg := newTestMessage("hello-spool", 3)
	entry, err := manager.Enqueue([]*codeccommon.Message{msg}, nil)
	require.NoError(t, err)
	require.True(t, entry.IsSpilled())
	require.False(t, entry.InMemory())
	require.Equal(t, uint64(len(msg.Value)), entry.FileBytes())

	msgs, callbacks, err := manager.Load(entry)
	require.NoError(t, err)
	require.Len(t, callbacks, 1)
	require.Len(t, msgs, 1)
	require.Equal(t, []byte("hello-spool"), msgs[0].Value)
	require.Equal(t, 3, msgs[0].GetRowsCount())

	manager.Release(entry)
}
