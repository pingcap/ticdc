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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Read, Advance and Confirm may run concurrently. Boundary is assembler-owned.
// Close runs after all callers have stopped.
// Read returns a nonempty batch without waiting to fill it.
// ContinueReading lets an unfinished dependency advance past the soft budget.
type Reader interface {
	Read(ctx context.Context) ([]*readData, error)
	Advance(updates []readUpdate)
	// Boundary requests event progress through commitTs. Nil covers every input;
	// an empty, nonnil selection covers the control stream. Zero clears the request.
	Boundary(tables []event.SchemaTableName, commitTs uint64) *readBoundary
	ContinueReading(bool)
	Confirm(ctx context.Context) error
	Close()
}

// reader owns transport access, reading admission and upstream confirmation.
type reader struct {
	g           *errgroup.Group
	client      Reader
	confirmable <-chan struct{}
}

// readLimit admits new input against the shared soft budget. An unfinished
// dependency can request more input without exposing its protocol to the reader.
type readLimit struct {
	memory     *memoryUsage
	continuing atomic.Bool
	changed    chan struct{}
}

func newReadLimit(memory *memoryUsage) *readLimit {
	return &readLimit{
		memory:  memory,
		changed: make(chan struct{}, 1),
	}
}

func (l *readLimit) ContinueReading(enabled bool) {
	if l.continuing.Swap(enabled) == enabled {
		return
	}
	select {
	case l.changed <- struct{}{}:
	default:
	}
}

func (l *readLimit) wait(ctx context.Context) error {
	for l.memory.used() >= l.memory.limit && !l.continuing.Load() {
		changed := l.memory.changes()
		if l.memory.used() < l.memory.limit || l.continuing.Load() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		case <-l.changed:
		}
	}
	return nil
}

func newReader(
	ctx context.Context,
	upstreamURI *url.URL,
	consumerID string,
	replicaConfig *config.ReplicaConfig,
	memory *memoryUsage,
	confirmable chan struct{},
) *reader {
	var client Reader
	switch sourceTypeFromURI(upstreamURI) {
	case sourceKafka:
		client = newKafkaReader(ctx, upstreamURI, consumerID, memory, replicaConfig, confirmable)
	case sourcePulsar:
		client = newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, memory, confirmable)
	default:
		client = newStorageReader(ctx, upstreamURI, replicaConfig, memory, confirmable)
	}
	return &reader{
		client:      client,
		confirmable: confirmable,
	}
}

func (r *reader) start(ctx context.Context, results chan<- []*readData) error {
	r.g, ctx = errgroup.WithContext(ctx)
	r.g.Go(func() error {
		return r.confirm(ctx)
	})
	defer close(results)
	for {
		data, err := r.client.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return context.Cause(ctx)
			}
			log.Error("consumer read failed", zap.Error(err))
			return err
		}
		select {
		case results <- data:
		case <-ctx.Done():
			return context.Cause(ctx)
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
	format  readFormat
	key     []byte
	value   []byte
	stream  int32
	ddlCopy bool // A replica awaits its canonical DDL before downstream execution.
	record  *ack

	schema      *readSchema
	ddl         *event.DDLEvent
	control     *readControl
	group       *readGroup
	groupEnd    bool
	dmlBoundary *readBoundary
	ddlBoundary *readBoundary
	ddlOrder    readOrder
}

// Group inputs are contiguous. An unordered group must be completely decoded
// before its rows are sorted.
type readGroup struct {
	tableID  int64
	compare  func(first, second *event.DMLEvent) int
	boundary *readBoundary
}

// Assembly returns decoded progress and matching scoped boundary crossings.
// These facts never confirm downstream writes or expose protocol state to readers.
type readUpdate struct {
	data     *readData
	decoded  bool
	progress uint64
	boundary *readBoundary
}

// Controls retain their input references until the downstream watermark passes.
type readControl struct {
	watermark uint64
	records   []*ack
}

// A boundary either completes a finite input scope or exposes a shared control
// watermark. Only assembly advances these facts; groups retain their own boundary.
type readBoundary struct {
	reached   bool
	watermark *uint64
}
