// Copyright 2025 PingCAP, Inc.
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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestChecksumStateCaptureMapUpdateIfNewerDropsOldSeq(t *testing.T) {
	m := newChecksumStateCaptureMap()
	capture := node.ID("capture-1")

	require.True(t, m.UpdateIfNewer(capture, heartbeatpb.ChecksumState_MISMATCH, 2))
	require.False(t, m.UpdateIfNewer(capture, heartbeatpb.ChecksumState_MATCH, 1))

	state, ok := m.Get(capture)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ChecksumState_MISMATCH, state)

	require.True(t, m.UpdateIfNewer(capture, heartbeatpb.ChecksumState_MATCH, 3))
	state, ok = m.Get(capture)
	require.True(t, ok)
	require.Equal(t, heartbeatpb.ChecksumState_MATCH, state)
}
