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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
)

// Read and Advance have one caller. Confirm may run concurrently.
// Close runs after all callers have stopped.
type reader interface {
	Read(ctx context.Context) (*readData, error)
	Advance(ctx context.Context, feedback readFeedback) (readProgress, error)
	Confirm(ctx context.Context) error
	Close() error
}

type readOrder uint8

const (
	inputOrder readOrder = iota
	commitOrder
)

type readFormat uint8

const (
	messageFormat readFormat = iota
	rowFormat                // Rows decoded with externally supplied table metadata.
)

// readData carries encoded contents, logical decoding context and input proofs.
// Physical offsets, message IDs and file paths remain private to each reader.
type readData struct {
	format        readFormat
	key           []byte
	value         []byte
	stream        int32
	record        *ack
	retainedBytes int64
	table         *common.TableInfo
	ddl           *event.DDLEvent
	control       *readControl
	group         *readGroup
	groupEnd      bool
	dmlBoundary   *readBoundary
	ddlBoundary   *readBoundary
	ddlOrder      readOrder
}

// Group inputs are contiguous. An unordered group must be completely decoded
// before its rows are sorted.
type readGroup struct {
	tableID  int64
	order    readOrder
	boundary *readBoundary
}

// Decoded facts drive reading progress. They never confirm downstream writes.
// A boundary request covers only the candidate events already decoded.
type readFeedback struct {
	data           *readData
	decoded        bool
	dml            *event.DMLEvent
	ddl            *event.DDLEvent
	watermark      uint64
	hasWatermark   bool
	needsMoreInput bool
	boundaryDDL    *event.DDLEvent
	pendingDML     int
}

type readProgress struct {
	watermark      uint64
	hasWatermark   bool
	needsMoreInput bool
	boundary       *readBoundary
	control        *readControl
	skip           bool
}

// Controls retain their input references until the downstream watermark passes.
type readControl struct {
	watermark uint64
	records   []*ack
}

// Readers own the positions behind a proof and publish when it is reached.
type readBoundary struct {
	reached bool
}

func newReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, progress *inputProgress, schemas *schemaUsage, confirmable chan<- struct{}) (reader, error) {
	source, err := sourceTypeFromURI(upstreamURI)
	if err != nil {
		return nil, err
	}
	switch source {
	case sourceKafka:
		return newKafkaReader(ctx, upstreamURI, consumerID, progress, replicaConfig, confirmable)
	case sourcePulsar:
		return newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, progress, confirmable)
	default:
		return newStorageReader(ctx, upstreamURI, replicaConfig, progress, schemas, confirmable)
	}
}
