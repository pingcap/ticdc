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

package causality

import (
	"fmt"
	"sort"
	"testing"
)

var (
	benchmarkDefaultSlotCount uint64 = 16 * 1024
	benchmarkSlotIndexResult  uint64
	benchmarkSlotHashesResult []uint64
)

func BenchmarkSlotIndex(b *testing.B) {
	hashes := makeBenchmarkSlotHashes(4096)

	b.Run("modulo_power_of_two", func(b *testing.B) {
		numSlots := benchmarkDefaultSlotCount
		var result uint64

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result += hashes[i&(len(hashes)-1)] % numSlots
		}
		benchmarkSlotIndexResult = result
	})

	b.Run("mask_power_of_two", func(b *testing.B) {
		mask := benchmarkDefaultSlotCount - 1
		var result uint64

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result += hashes[i&(len(hashes)-1)] & mask
		}
		benchmarkSlotIndexResult = result
	})

	b.Run("selected_mapper_power_of_two", func(b *testing.B) {
		getSlot := newGetSlotFunc(benchmarkDefaultSlotCount)
		var result uint64

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result += getSlot(hashes[i&(len(hashes)-1)])
		}
		benchmarkSlotIndexResult = result
	})

	b.Run("selected_mapper_non_power_of_two", func(b *testing.B) {
		getSlot := newGetSlotFunc(benchmarkDefaultSlotCount - 1)
		var result uint64

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			result += getSlot(hashes[i&(len(hashes)-1)])
		}
		benchmarkSlotIndexResult = result
	})
}

func BenchmarkSortHashesBySlot(b *testing.B) {
	for _, hashCount := range []int{8, 64, 1024} {
		b.Run(fmt.Sprintf("hashes_%d", hashCount), func(b *testing.B) {
			source := makeBenchmarkSlotHashes(hashCount)

			b.Run("old_modulo_dedup", func(b *testing.B) {
				benchmarkSortHashesByOldModuloAndDedup(b, source, benchmarkDefaultSlotCount)
			})

			b.Run("selected_mapper_dedup", func(b *testing.B) {
				benchmarkSortHashesByFuncAndDedup(b, source, newGetSlotFunc(benchmarkDefaultSlotCount))
			})

			sourceMap := makeBenchmarkSlotHashMap(source)
			b.Run("selected_mapper_map_input", func(b *testing.B) {
				benchmarkSortHashesByMap(b, len(source), sourceMap, newGetSlotFunc(benchmarkDefaultSlotCount))
			})
		})
	}
}

func benchmarkSortHashesByOldModuloAndDedup(b *testing.B, source []uint64, numSlots uint64) {
	b.Helper()
	sample := sortHashesByOldModuloAndDedup(append([]uint64(nil), source...), numSlots)
	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(len(source)), "input_hashes/op")
	b.ReportMetric(float64(len(sample)), "out_hashes/op")

	for i := 0; i < b.N; i++ {
		hashes := append([]uint64(nil), source...)
		benchmarkSlotHashesResult = sortHashesByOldModuloAndDedup(hashes, numSlots)
	}
}

func benchmarkSortHashesByMap(b *testing.B, inputCount int, source map[uint64]struct{}, getSlot getSlotFunc) {
	b.Helper()
	sample := sortHashes(source, getSlot)
	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(inputCount), "input_hashes/op")
	b.ReportMetric(float64(len(source)), "unique_hashes/op")
	b.ReportMetric(float64(len(sample)), "out_hashes/op")

	for i := 0; i < b.N; i++ {
		benchmarkSlotHashesResult = sortHashes(source, getSlot)
	}
}

func benchmarkSortHashesByFuncAndDedup(b *testing.B, source []uint64, getSlot getSlotFunc) {
	b.Helper()
	sample := sortHashesByFuncAndDedup(append([]uint64(nil), source...), getSlot)
	b.ResetTimer()
	b.ReportAllocs()
	b.ReportMetric(float64(len(source)), "input_hashes/op")
	b.ReportMetric(float64(len(sample)), "out_hashes/op")

	for i := 0; i < b.N; i++ {
		hashes := append([]uint64(nil), source...)
		benchmarkSlotHashesResult = sortHashesByFuncAndDedup(hashes, getSlot)
	}
}

func sortHashesByOldModuloAndDedup(hashes []uint64, numSlots uint64) []uint64 {
	if len(hashes) == 0 {
		return nil
	}

	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i]%numSlots < hashes[j]%numSlots
	})

	return dedupSortedHashes(hashes)
}

func sortHashesByFuncAndDedup(hashes []uint64, getSlot getSlotFunc) []uint64 {
	if len(hashes) == 0 {
		return nil
	}

	sort.Slice(hashes, func(i, j int) bool {
		return getSlot(hashes[i]) < getSlot(hashes[j])
	})

	return dedupSortedHashes(hashes)
}

func dedupSortedHashes(hashes []uint64) []uint64 {
	last := hashes[0]
	j := 1
	for i, hash := range hashes {
		if i == 0 {
			continue
		}
		if hash == last {
			continue
		}
		last = hash
		hashes[j] = hash
		j++
	}
	return hashes[:j]
}

func makeBenchmarkSlotHashes(count int) []uint64 {
	hashes := make([]uint64, count)
	x := uint64(0x9e3779b97f4a7c15)
	for i := range hashes {
		x += 0x9e3779b97f4a7c15
		z := x
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		hashes[i] = z ^ (z >> 31)
	}
	return hashes
}

func makeBenchmarkSlotHashMap(source []uint64) map[uint64]struct{} {
	hashes := make(map[uint64]struct{}, len(source))
	for _, hash := range source {
		hashes[hash] = struct{}{}
	}
	return hashes
}
