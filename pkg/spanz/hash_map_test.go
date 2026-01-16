// Copyright 2022 PingCAP, Inc.
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

package spanz

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/stretchr/testify/require"
)

func TestSyncMapBasic(t *testing.T) {
	t.Parallel()

	var m SyncMap

	span1 := heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	span2 := heartbeatpb.TableSpan{TableID: 2}

	m.Store(span1, 1)
	v, ok := m.Load(span1)
	require.True(t, ok)
	require.Equal(t, 1, v)

	actual, loaded := m.LoadOrStore(span1, 2)
	require.True(t, loaded)
	require.Equal(t, 1, actual)

	actual, loaded = m.LoadOrStore(span2, 3)
	require.False(t, loaded)
	require.Equal(t, 3, actual)

	actual, loaded = m.LoadAndDelete(span2)
	require.True(t, loaded)
	require.Equal(t, 3, actual)

	_, ok = m.Load(span2)
	require.False(t, ok)

	m.Delete(span1)
	_, ok = m.Load(span1)
	require.False(t, ok)
}

func TestSyncMapRangeStop(t *testing.T) {
	t.Parallel()

	var m SyncMap
	m.Store(heartbeatpb.TableSpan{TableID: 1}, 1)
	m.Store(heartbeatpb.TableSpan{TableID: 2}, 2)
	m.Store(heartbeatpb.TableSpan{TableID: 3}, 3)

	visited := 0
	m.Range(func(span heartbeatpb.TableSpan, value any) bool {
		visited++
		return false
	})
	require.Equal(t, 1, visited)
}
