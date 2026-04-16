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

package liveness

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStoreMonotonic(t *testing.T) {
	var l Liveness
	require.Equal(t, CaptureAlive, l.Load())
	require.Equal(t, "Alive", l.String())

	require.True(t, l.Store(CaptureDraining))
	require.Equal(t, CaptureDraining, l.Load())
	require.Equal(t, "Draining", l.String())

	require.False(t, l.Store(CaptureAlive))
	require.False(t, l.Store(CaptureDraining))

	require.True(t, l.Store(CaptureStopping))
	require.Equal(t, CaptureStopping, l.Load())
	require.Equal(t, "Stopping", l.String())

	require.False(t, l.Store(CaptureDraining))
	require.False(t, l.Store(CaptureStopping))
}

func TestStoreDisallowSkip(t *testing.T) {
	var l Liveness
	require.False(t, l.Store(CaptureStopping))
	require.Equal(t, CaptureAlive, l.Load())
}

func TestStoreRejectInvalidValue(t *testing.T) {
	var l Liveness
	require.False(t, l.Store(Liveness(3)))

	require.True(t, l.Store(CaptureDraining))
	require.True(t, l.Store(CaptureStopping))
	require.False(t, l.Store(Liveness(3)))
	require.Equal(t, CaptureStopping, l.Load())
}

func TestStringNil(t *testing.T) {
	var l *Liveness
	require.Equal(t, "Unknown", l.String())
}
