// Copyright 2024 PingCAP, Inc.
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

package eventstore

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
)

func BenchmarkEventStoreWrite(b *testing.B) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		log.Panic("Failed to create zstd encoder", zap.Error(err))
	}
	store := &eventStore{
		encoder: encoder, // only this field is needed
	}
	dbPath := fmt.Sprintf("/tmp/testdb-%s", b.Name())
	db, err := pebble.Open(dbPath, newPebbleOptions())
	if err != nil {
		log.Fatal("open db failed", zap.Error(err))
	}
	ch := chann.NewUnlimitedChannel[eventWithCallback, uint64](nil, eventWithCallbackSizer)
	taskpool := newWriteTaskPool(store, db, ch, writeWorkerNumPerDB)
	taskpool.run(context.Background())
	b.ResetTimer()
	for range b.N {
		ch.Push(eventWithCallback{
			subID:    1,
			tableID:  100,
			kvs:      kvs,
			callback: finishCallback,
		})
	}
}
