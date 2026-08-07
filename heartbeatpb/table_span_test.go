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

package heartbeatpb

import "testing"

import "github.com/stretchr/testify/require"

func TestTableSpanLess(t *testing.T) {
	t.Parallel()

	base := &TableSpan{
		KeyspaceID: 1,
		TableID:    10,
		StartKey:   []byte("a"),
		EndKey:     []byte("b"),
	}

	require.True(t, (&TableSpan{KeyspaceID: 0, TableID: 10, StartKey: []byte("a"), EndKey: []byte("b")}).Less(base))
	require.True(t, (&TableSpan{KeyspaceID: 1, TableID: 9, StartKey: []byte("a"), EndKey: []byte("b")}).Less(base))
	require.True(t, (&TableSpan{KeyspaceID: 1, TableID: 10, StartKey: []byte("a"), EndKey: []byte("a")}).Less(base))
	require.True(t, (&TableSpan{KeyspaceID: 1, TableID: 10, StartKey: []byte("0"), EndKey: []byte("z")}).Less(base))

	require.False(t, base.Less(&TableSpan{
		KeyspaceID: 1,
		TableID:    10,
		StartKey:   []byte("a"),
		EndKey:     []byte("b"),
	}))
	require.False(t, base.Less(&TableSpan{
		KeyspaceID: 0,
		TableID:    10,
		StartKey:   []byte("a"),
		EndKey:     []byte("b"),
	}))
	require.False(t, base.Less(&TableSpan{
		KeyspaceID: 1,
		TableID:    10,
		StartKey:   []byte("a"),
		EndKey:     []byte("a"),
	}))
}
