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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"strconv"
	"testing"
)

func BenchmarkConductorWatermark(b *testing.B) {
	for _, count := range []int{1024, 32768, 262144} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			c := &conductor{watermark: 20}
			w := &writer{}
			for i := range count {
				c.queueDML(&decodeTask{commitTs: 10, tableID: int64(i%100 + 1), stream: int32(i % 4)})
			}
			b.ReportAllocs()
			for b.Loop() {
				c.updateWatermark(w)
			}
		})
	}
}
