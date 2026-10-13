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
	"maps"
	"net/url"
	"slices"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Read and Advance have one caller. Confirm may run concurrently.
// Close runs after all callers have stopped.
type readClient interface {
	Read(ctx context.Context) (*readData, error)
	Advance(ctx context.Context, feedback readFeedback) (readProgress, error)
	Confirm(ctx context.Context) error
	Close()
}

// reader owns transport access, reading admission and upstream confirmation.
// Feedback and reads share one goroutine so positions never pass undecoded inputs.
type reader struct {
	g           *errgroup.Group
	client      readClient
	progress    *inputProgress
	confirmable <-chan struct{}
}

// Progress exchanges complete before reading resumes. This also publishes
// boundary updates to the assembler without concurrently mutating its proofs.
type readRequest struct {
	feedback       readFeedback
	resume         bool
	needsMoreInput bool
}

type readResult struct {
	data     *readData
	progress readProgress
}

// Return logical stream IDs with the reader so decoder initialization does not
// depend on transport-specific reader fields. An empty list uses one decoder.
func newReader(ctx context.Context, upstreamURI *url.URL, consumerID string, replicaConfig *config.ReplicaConfig, progress *inputProgress, schemas *schemaUsage, confirmable chan struct{}) (*reader, []int32) {
	var (
		client  readClient
		streams []int32
	)
	switch sourceTypeFromURI(upstreamURI) {
	case sourceKafka:
		reader := newKafkaReader(ctx, upstreamURI, consumerID, progress, replicaConfig, confirmable)
		client = reader
		streams = slices.Collect(maps.Keys(reader.partitions))
	case sourcePulsar:
		client = newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, progress, confirmable)
	default:
		client = newStorageReader(ctx, upstreamURI, replicaConfig, progress, schemas, confirmable)
	}
	return &reader{
		client:      client,
		progress:    progress,
		confirmable: confirmable,
	}, streams
}

func (r *reader) start(ctx context.Context, requests <-chan readRequest, results chan<- readResult) error {
	r.g, ctx = errgroup.WithContext(ctx)
	r.g.Go(func() error {
		return r.confirm(ctx)
	})
	var needsMoreInput bool
	for {
		if !needsMoreInput {
			if err := r.waitMemory(ctx); err != nil {
				return err
			}
		}
		data, err := r.client.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Error("consumer read failed", zap.Error(err))
			}
			return err
		}
		select {
		case results <- readResult{
			data: data,
		}:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		for {
			var request readRequest
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case request = <-requests:
			}
			if request.resume {
				needsMoreInput = request.needsMoreInput
				break
			}
			progress, err := r.client.Advance(ctx, request.feedback)
			if err != nil {
				if ctx.Err() == nil {
					log.Error("consumer read progress failed", zap.Error(err))
				}
				return err
			}
			select {
			case results <- readResult{
				progress: progress,
			}:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	}
}

func (r *reader) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-r.confirmable:
			if err := r.client.Confirm(ctx); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer input confirmation failed", zap.Error(err))
				}
				return err
			}
		}
	}
}

// Backpressure applies to new independent inputs. Retained caches alone must
// not prevent reading, and unfinished input boundaries bypass this wait.
func (r *reader) waitMemory(ctx context.Context) error {
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if r.progress.memory.used() < maxMemoryBytes || r.progress.received.Load() == r.progress.confirmed.Load() {
			return nil
		}
		changed := r.progress.memory.changes()
		// Subscribe before rechecking: a concurrent release either satisfies the
		// admission condition or closes changed.
		if r.progress.memory.used() < maxMemoryBytes || r.progress.received.Load() == r.progress.confirmed.Load() {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (r *reader) close() {
	if r.g != nil {
		_ = r.g.Wait()
	}
	r.client.Close()
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
	compare  func(first, second *event.DMLEvent) int
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
