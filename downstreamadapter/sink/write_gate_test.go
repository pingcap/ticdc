// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sink_test

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/writelease"
	"github.com/stretchr/testify/require"
)

func TestWriteGatedSinkBlocksAndResumesDML(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := mock.NewMockSink(ctrl)
	gate := writelease.NewGate()
	inner.EXPECT().SetWriteGate(gate)
	gated := sink.WithWriteGate(t.Context(), inner, gate)

	written := make(chan struct{})
	inner.EXPECT().AddDMLEvent(nil).Do(func(_ any) { close(written) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		gated.AddDMLEvent(nil)
	}()

	select {
	case <-written:
		t.Fatal("DML passed through a closed capture write gate")
	case <-time.After(50 * time.Millisecond):
	}

	now := time.Now()
	require.True(t, gate.RenewP2P(now, writelease.P2PLeaseDuration))
	require.True(t, gate.RenewEtcd(now, writelease.EtcdProofDuration))
	require.Eventually(t, func() bool {
		select {
		case <-written:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
	<-done
}

func TestWriteGatedSinkStopsWaitingWhenContextIsCanceled(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := mock.NewMockSink(ctrl)
	gate := writelease.NewGate()
	ctx, cancel := context.WithCancel(context.Background())
	inner.EXPECT().SetWriteGate(gate)
	gated := sink.WithWriteGate(ctx, inner, gate)

	done := make(chan error, 1)
	go func() {
		done <- gated.WriteBlockEvent(nil)
	}()
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestWriteGatedSinkCoversEveryWriteEntry(t *testing.T) {
	ctrl := gomock.NewController(t)
	inner := mock.NewMockSink(ctrl)
	gate := writelease.NewGate()
	inner.EXPECT().SetWriteGate(gate)
	gated := sink.WithWriteGate(t.Context(), inner, gate)

	// A checkpoint is safe to drop while closed because later checkpoints
	// supersede it.
	gated.AddCheckpointTs(41)

	now := time.Now()
	require.True(t, gate.RenewP2P(now, writelease.P2PLeaseDuration))
	require.True(t, gate.RenewEtcd(now, writelease.EtcdProofDuration))

	inner.EXPECT().FlushDMLBeforeBlock(nil).Return(nil)
	inner.EXPECT().WriteBlockEvent(nil).Return(nil)
	inner.EXPECT().AddCheckpointTs(uint64(42))
	require.NoError(t, gated.FlushDMLBeforeBlock(nil))
	require.NoError(t, gated.WriteBlockEvent(nil))
	gated.AddCheckpointTs(42)
}
