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

package cloudstorage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestFlushMarkerWaitCanBeCalledMultipleTimes(t *testing.T) {
	t.Parallel()

	marker := newFlushMarker(100)
	marker.finish()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, marker.wait(ctx))

	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.NoError(t, marker.wait(ctx))
}

func TestFlushMarkerWaitReturnsContextCause(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	cause := errors.New("flush marker canceled")
	cancel(cause)

	marker := newFlushMarker(100)
	err := marker.wait(ctx)
	require.ErrorIs(t, err, cause)
}

func TestReplacePostFlushCallbacksDetachesOriginalMessageCallbacks(t *testing.T) {
	t.Parallel()

	var originalCallbackCount atomic.Int32
	var flushCallbackCount atomic.Int32
	task := &task{
		callbacks: &txnCallbacks{
			flushed: []func(){
				func() {
					flushCallbackCount.Add(1)
				},
			},
		},
		encodedMsgs: []*common.Message{
			{Callback: func() { originalCallbackCount.Add(1) }},
			{Callback: func() { originalCallbackCount.Add(1) }},
			{},
		},
	}

	task.replacePostFlushCallbacks()

	require.Nil(t, task.encodedMsgs[0].Callback)
	require.Nil(t, task.encodedMsgs[1].Callback)
	require.NotNil(t, task.encodedMsgs[2].Callback)

	task.encodedMsgs[2].Callback()
	task.encodedMsgs[2].Callback()
	require.Equal(t, int32(0), originalCallbackCount.Load())
	require.Equal(t, int32(1), flushCallbackCount.Load())
}
