//  Copyright 2026 PingCAP, Inc.
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

package writer

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func encodeRedoEventWithFinalizer(
	t *testing.T, finalized chan<- struct{}, postEnqueue, postFlush func(),
) *polymorphicRedoEvent {
	t.Helper()
	event := &commonEvent.RedoRowEvent{
		CommitTs:        1,
		EnqueueCallback: postEnqueue,
		Callback:        postFlush,
	}
	runtime.SetFinalizer(event, func(*commonEvent.RedoRowEvent) {
		finalized <- struct{}{}
	})
	encoded, err := toPolymorphicDMLEvent(event)
	require.NoError(t, err)
	return encoded
}

func TestEncodedEventDoesNotRetainSourceRowEvent(t *testing.T) {
	finalized := make(chan struct{}, 1)
	var postEnqueueCount atomic.Int64
	var postFlushCount atomic.Int64
	encoded := encodeRedoEventWithFinalizer(
		t,
		finalized,
		func() { postEnqueueCount.Add(1) },
		func() { postFlushCount.Add(1) },
	)

	require.Eventually(t, func() bool {
		runtime.GC()
		select {
		case <-finalized:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)

	encoded.postEnqueue()
	encoded.PostFlush()
	require.Equal(t, int64(1), postEnqueueCount.Load())
	require.Equal(t, int64(1), postFlushCount.Load())
	runtime.KeepAlive(encoded)
}

func TestNewEncodingWorkerGroup(t *testing.T) {
	t.Parallel()

	changefeed := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	cfg := testutil.NewConsistentConfig("nfs:///tmp/redo")
	cfg.EncodingWorkerNum = util.AddressOf(3)
	writerCfg, err := NewConfig(changefeed, cfg)
	require.NoError(t, err)
	g := newEncodingWorkerGroup(writerCfg)
	require.Equal(t, 3, g.workerNum)
	require.Len(t, g.inputChs, 3)

	defaultCfg, err := NewConfig(changefeed, testutil.NewConsistentConfig("nfs:///tmp/redo"))
	require.NoError(t, err)
	g = newEncodingWorkerGroup(defaultCfg)
	require.Equal(t, redo.DefaultEncodingWorkerNum, g.workerNum)
	require.Len(t, g.inputChs, redo.DefaultEncodingWorkerNum)
}
