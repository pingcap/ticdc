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
	"context"
	"net/url"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/pingcap/ticdc/pkg/config"
)

// Read and Confirm may run concurrently. Close runs after both have stopped.
type reader interface {
	Read(ctx context.Context) (*readData, error)
	Confirm(ctx context.Context) error
	Close() error
}

// readData retains source contents and metadata until all derived writes finish.
type readData struct {
	key       []byte
	value     []byte
	partition int32
	record    *ack
	storage   *storageInput
	schema    *cloudstorage.SchemaFile
}

// The assembler reports decoded progress so Kafka can prioritize slow partitions.
// Both stages access this state on the same goroutine.
type readProgress struct {
	watermark      uint64
	hasWatermark   bool
	needsMoreInput bool
	ddlTs          uint64
}

// A fixed broker boundary proves that earlier controls or related DML have
// been read. Readers own its positions and mark it reached after decoding input.
type readBoundary struct {
	reached  bool
	commitTs uint64 // A broadcast DDL can close its partition before the broker tail.
}

func newReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, memory *memoryUsage) (reader, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	switch source {
	case sourceKafka:
		return newKafkaReader(ctx, upstreamURI, consumerID, memory)
	case sourcePulsar:
		return newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, memory)
	default:
		return newStorageReader(ctx, upstreamURI, replicaConfig, memory)
	}
}
