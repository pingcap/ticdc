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
	"iter"
	"net/url"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Run owns source reading and parsing. Advance and Confirm may run concurrently.
// Boundary is assembler-owned. Close runs after all callers have stopped.
// ContinueReading lets an unfinished dependency advance past the soft budget.
type Reader interface {
	Run(ctx context.Context, parser *inputParser, output chan<- []assemblyInput) error
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

func (r *reader) start(ctx context.Context, parser *inputParser, results chan<- []assemblyInput) error {
	r.g, ctx = errgroup.WithContext(ctx)
	r.g.Go(func() error { return r.confirm(ctx) })
	defer close(results)
	err := r.client.Run(ctx, parser, results)
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err != nil {
		log.Error("consumer read failed", zap.Error(err))
	}
	return err
}

// Each source keeps its native input type through reading and parsing. The
// bounded queue lets I/O run while the source parser publishes logical work.
func readAndParse[T any](ctx context.Context, read func(context.Context) ([]T, error), parse func(context.Context, T) iter.Seq2[assemblyInput, error], output chan<- []assemblyInput) error {
	g, ctx := errgroup.WithContext(ctx)
	inputs := make(chan []T, 1)
	g.Go(func() error {
		defer close(inputs)
		for {
			batch, err := read(ctx)
			if err != nil {
				return err
			}
			select {
			case inputs <- batch:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
	})
	g.Go(func() error {
		pending := make([]assemblyInput, 0, batchRows)
		flush := func() error {
			if len(pending) == 0 {
				return nil
			}
			select {
			case output <- pending:
				pending = make([]assemblyInput, 0, batchRows)
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		for {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case batch, ok := <-inputs:
				if !ok {
					return flush()
				}
				for _, data := range batch {
					for input, err := range parse(ctx, data) {
						if err != nil {
							return err
						}
						pending = append(pending, input)
						_, row := input.(*decodeTask)
						if len(pending) == batchRows || !row {
							if err := flush(); err != nil {
								return err
							}
						}
					}
				}
				if err := flush(); err != nil {
					return err
				}
			}
		}
	})
	return g.Wait()
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

// Source progress contains no input payload or decoder state.
type readUpdate struct {
	stream   int32
	progress uint64
	boundary *readBoundary
}

// Controls retain their input references until the downstream watermark passes.
type readControl struct {
	watermark uint64
	records   []*ack
}

// A boundary either completes a finite input scope or exposes a shared control
// watermark. Only assembly advances these facts.
type readBoundary struct {
	reached   bool
	watermark *uint64
}
