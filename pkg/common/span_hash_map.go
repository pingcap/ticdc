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

package common

import (
	"encoding/binary"
	"unsafe"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/util/seahash"
)

// HashMapSpan is a specialized hash map that map a TableSpan to a value.
type SpanHashMap[T any] struct {
	hashMap map[hashableSpan]T
}

// NewHashMap returns a new HashMap.
func NewSpanHashMap[T any]() *SpanHashMap[T] {
	return &SpanHashMap[T]{
		hashMap: make(map[hashableSpan]T),
	}
}

// GetV looks for the key item in the map, returning it.
// It returns zeroValue if unable to find that item.
func (m *SpanHashMap[T]) GetV(span heartbeatpb.TableSpan) T {
	item := m.hashMap[toHashableSpan(span)]
	return item
}

// ReplaceOrInsert adds the given item to the map.
func (m *SpanHashMap[T]) ReplaceOrInsert(span heartbeatpb.TableSpan, value T) {
	m.hashMap[toHashableSpan(span)] = value
}

// HashTableSpan hashes the given span to a slot offset.
func HashTableSpan(span *heartbeatpb.TableSpan, slots int) int {
	b := make([]byte, 8+len(span.StartKey))
	binary.LittleEndian.PutUint64(b[0:8], uint64(span.TableID))
	copy(b[8:], span.StartKey)
	return int(seahash.Sum64(b) % uint64(slots))
}

// hashableSpan is a hashable span, which can be used as a map key.
type hashableSpan struct {
	TableID  int64
	StartKey string
	EndKey   string
}

// toHashableSpan converts a Span to a hashable span.
func toHashableSpan(span heartbeatpb.TableSpan) hashableSpan {
	return hashableSpan{
		TableID:  span.TableID,
		StartKey: unsafeBytesToString(span.StartKey),
		EndKey:   unsafeBytesToString(span.EndKey),
	}
}

// unsafeBytesToString converts byte to string without memory allocation.
// See: https://cs.opensource.google/go/go/+/refs/tags/go1.19.4:src/strings/builder.go;l=48
func unsafeBytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
