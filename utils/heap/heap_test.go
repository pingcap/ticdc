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

package heap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock Item implementation for testing
type mockItem struct {
	value     int
	heapIndex int
}

func (m *mockItem) SetHeapIndex(index int)        { m.heapIndex = index }
func (m *mockItem) GetHeapIndex() int             { return m.heapIndex }
func (m *mockItem) LessThan(other *mockItem) bool { return m.value < other.value }

func TestHeap(t *testing.T) {
	h := NewHeap[*mockItem]()

	h.AddOrUpdate(&mockItem{value: 1})
	h.AddOrUpdate(&mockItem{value: 4})
	h.AddOrUpdate(&mockItem{value: 2})
	h.AddOrUpdate(&mockItem{value: 3})
	h.AddOrUpdate(&mockItem{value: 5})

	assert.Equal(t, 5, h.Len())
	for i := 1; i <= 5; i++ {
		top, ok := h.PeekTop()
		assert.True(t, ok)
		assert.Equal(t, i, top.value)
		h.PopTop()
	}
	assert.Equal(t, 0, h.Len())

	h.AddOrUpdate(&mockItem{value: 1})
	h.AddOrUpdate(&mockItem{value: 4})
	h.AddOrUpdate(&mockItem{value: 2})
	i3 := &mockItem{value: 3}
	h.AddOrUpdate(i3)
	h.AddOrUpdate(&mockItem{value: 5})

	ok := h.Remove(i3)
	assert.True(t, ok)
	ok = h.Remove(i3)
	assert.False(t, ok)

	items := []int{1, 2, 4, 5}

	for _, item := range items {
		top, ok := h.PopTop()
		assert.True(t, ok)
		assert.Equal(t, item, top.value)
	}

	_, ok = h.PopTop()
	assert.False(t, ok)
	assert.Equal(t, 0, h.Len())
}
