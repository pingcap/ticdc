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
	"crypto/rand"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
)

// Helper function to generate random data
func generateRandomData() []byte {
	data := make([]byte, 10240)
	rand.Read(data)
	return data
}

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
		log.Fatal("open db failed", zap.String("path", dbPath), zap.Error(err))
	}
	ch := chann.NewUnlimitedChannel[eventWithCallback, uint64](nil, eventWithCallbackSizer)
	taskpool := newWriteTaskPool(store, db, ch, writeWorkerNumPerDB)
	var writeNum atomic.Int32
	finishCallback := func() {
		writeNum.Add(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	taskpool.run(ctx)
	defer cancel()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		kvs := make([]common.RawKVEntry, 1000)
		for j := range kvs {
			kvs[j].OpType = common.OpTypePut
			// TODO: This is totally ordered
			kvs[j].CRTs = uint64(i) + 100
			kvs[j].StartTs = uint64(i) + 99
			kvs[j].Key = []byte(fmt.Sprintf("key-%d-%d", i, j))
			kvs[j].KeyLen = uint32(len(kvs[j].Key))
			kvs[j].Value = generateRandomData()
			kvs[j].ValueLen = uint32(len(kvs[j].Value))
		}

		ch.Push(eventWithCallback{
			subID:    1,
			tableID:  100,
			kvs:      kvs,
			callback: finishCallback,
		})
	}

	// Wait for all writes to complete
	for writeNum.Load() < int32(b.N) {
		time.Sleep(10 * time.Millisecond)
	}

	b.StopTimer()

	db.Close()
	if err := os.RemoveAll(dbPath); err != nil {
		log.Fatal("Failed to clean up test database", zap.Error(err))
	}
}
