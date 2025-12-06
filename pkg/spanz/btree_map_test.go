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

func TestSpanMap(t *testing.T) {
	t.Parallel()

	m := NewBtreeMap[int]()

	// Insert then get.
	m.ReplaceOrInsert(*heartbeatpb.NewTableSpan(1, nil, nil, 0), 1)
	v, ok := m.Get(*heartbeatpb.NewTableSpan(1, nil, nil, 0))
	require.Equal(t, v, 1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
	require.True(t, m.Has(*heartbeatpb.NewTableSpan(1, nil, nil, 0)))

	// Insert then get again.
	m.ReplaceOrInsert(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0), 2)
	require.Equal(t, 2, m.Len())
	v, ok = m.Get(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0))
	require.Equal(t, v, 2)
	require.True(t, ok)

	// Overwrite then get.
	old, ok := m.ReplaceOrInsert(
		*heartbeatpb.NewTableSpan(1, []byte{1}, []byte{1}, 0), 3)
	require.Equal(t, old, 2)
	require.True(t, ok)
	require.Equal(t, 2, m.Len())
	require.True(t, m.Has(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0)))
	v, ok = m.Get(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0))
	require.Equal(t, v, 3)
	require.True(t, ok)

	// get value
	v = m.GetV(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0))
	require.Equal(t, v, 3)

	// Delete than get value
	v, ok = m.Delete(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0))
	require.Equal(t, v, 3)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
	require.False(t, m.Has(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0)))
	v = m.GetV(*heartbeatpb.NewTableSpan(1, []byte{1}, nil, 0))
	require.Equal(t, v, 0)

	// Pointer value
	mp := NewBtreeMap[*int]()
	vp := &v
	mp.ReplaceOrInsert(*heartbeatpb.NewTableSpan(1, nil, nil, 0), vp)
	vp1, ok := mp.Get(*heartbeatpb.NewTableSpan(1, nil, nil, 0))
	require.Equal(t, vp, vp1)
	require.True(t, ok)
	require.Equal(t, 1, m.Len())
}

func TestMapAscend(t *testing.T) {
	t.Parallel()

	m := NewBtreeMap[int]()
	for i := 0; i < 4; i++ {
		m.ReplaceOrInsert(*heartbeatpb.NewTableSpan(int64(i), nil, nil, 0), i)
	}

	j := 0
	m.Ascend(func(span heartbeatpb.TableSpan, value int) bool {
		require.Equal(t, *heartbeatpb.NewTableSpan(int64(j), nil, nil, 0), span)
		j++
		return true
	})
	require.Equal(t, 4, j)

	j = 0
	m.AscendRange(*heartbeatpb.NewTableSpan(1, nil, nil, 0), *heartbeatpb.NewTableSpan(2, nil, nil, 0),
		func(span heartbeatpb.TableSpan, value int) bool {
			require.Equal(t, *heartbeatpb.NewTableSpan(1, nil, nil, 0), span)
			j++
			return true
		})
	require.Equal(t, 1, j)
}

func TestMapFindHole(t *testing.T) {
	t.Parallel()

	cases := []struct {
		spans         []heartbeatpb.TableSpan
		rang          [2]heartbeatpb.TableSpan
		expectedFound []heartbeatpb.TableSpan
		expectedHole  []heartbeatpb.TableSpan
	}{
		{ // 0. all found.
			spans: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t1_2"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_2"), []byte("t2_0"), 0),
			},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedFound: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t1_2"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_2"), []byte("t2_0"), 0),
			},
		},
		{ // 1. on hole in the middle.
			spans: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_3"), []byte("t1_4"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedFound: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_3"), []byte("t1_4"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			expectedHole: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t1_3"), 0),
			},
		},
		{ // 2. two holes in the middle.
			spans: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_2"), []byte("t1_3"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedFound: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_2"), []byte("t1_3"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			expectedHole: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t1_2"), 0),
				*heartbeatpb.NewTableSpan(0, []byte("t1_3"), []byte("t1_4"), 0),
			},
		},
		{ // 3. all missing.
			spans: []heartbeatpb.TableSpan{},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedHole: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t2_0"), 0),
			},
		},
		{ // 4. start not found
			spans: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedFound: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_4"), []byte("t2_0"), 0),
			},
			expectedHole: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_4"), 0),
			},
		},
		{ // 5. end not found
			spans: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
			},
			rang: [2]heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), nil, 0),
				*heartbeatpb.NewTableSpan(0, []byte("t2_0"), nil, 0),
			},
			expectedFound: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_0"), []byte("t1_1"), 0),
			},
			expectedHole: []heartbeatpb.TableSpan{
				*heartbeatpb.NewTableSpan(0, []byte("t1_1"), []byte("t2_0"), 0),
			},
		},
	}

	for i, cs := range cases {
		_, _ = i, cs
		m := NewBtreeMap[struct{}]()
		for _, span := range cs.spans {
			m.ReplaceOrInsert(span, struct{}{})
		}
		found, holes := m.FindHoles(cs.rang[0], cs.rang[1])
		require.Equalf(t, cs.expectedFound, found, "case %d, %#v", i, cs)
		require.Equalf(t, cs.expectedHole, holes, "case %d, %#v", i, cs)
	}
}
