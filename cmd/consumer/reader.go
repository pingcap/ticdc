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
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/uber-go/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// streamSpec identifies one source input that needs a persistent decoder.
type streamSpec struct {
	id    int32
	topic string
}

// Run owns source reading and decoding. Advance and Confirm may run concurrently.
// Boundary is conductor-owned. Close runs after all callers have stopped.
// ContinueReading permits progress past the soft budget for missing input
// dependencies or an empty pipeline holding source buffers and positions.
type Reader interface {
	Run(ctx context.Context, decoder *inputDecoder, tasks chan<- []task) error
	Advance(updates []readUpdate)
	// Boundary requests event progress through commitTs. Nil covers every input;
	// an empty, nonnil selection covers the control stream. Zero clears the request.
	Boundary(tables []event.SchemaTableName, commitTs uint64) *readBoundary
	ContinueReading(bool)
	Confirm(ctx context.Context, checkpoint uint64) error
	// Streams describes the source inputs that need a persistent decoder.
	// Storage returns nil because each file is decoded transiently.
	Streams() []streamSpec
	Close()
}

// reader owns transport access, reading admission and upstream confirmation.
type reader struct {
	g           *errgroup.Group
	client      Reader
	confirmable <-chan struct{}
	checkpoint  *atomic.Uint64
}

// readLimit admits new input against the shared soft budget. An unfinished
// dependency can request more input without exposing its protocol to the reader.
type readLimit struct {
	memory     *memoryBudget
	continuing atomic.Bool
	changed    chan struct{}
}

func newReadLimit(memory *memoryBudget) *readLimit {
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
	codecConfig *codecCommon.Config,
	memory *memoryBudget,
	confirmable chan struct{},
	checkpoint *atomic.Uint64,
) *reader {
	var client Reader
	switch sourceTypeFromURI(upstreamURI) {
	case sourceKafka:
		client = newKafkaReader(ctx, upstreamURI, consumerID, codecConfig, memory, replicaConfig, confirmable)
	case sourcePulsar:
		client = newPulsarReader(ctx, upstreamURI, consumerID, replicaConfig, memory, confirmable)
	default:
		client = newStorageReader(ctx, upstreamURI, replicaConfig, codecConfig.Protocol, memory)
	}
	return &reader{
		client:      client,
		confirmable: confirmable,
		checkpoint:  checkpoint,
	}
}

func (r *reader) start(ctx context.Context, decoder *inputDecoder, tasks chan<- []task) error {
	r.g, ctx = errgroup.WithContext(ctx)
	r.g.Go(func() error { return r.confirm(ctx) })
	defer close(tasks)
	err := r.client.Run(ctx, decoder, tasks)
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	if err != nil {
		log.Error("consumer read failed", zap.Error(err))
	}
	return err
}

// Each source keeps its native input type through reading and decoding. The
// bounded queue lets I/O run while the source decoder publishes logical work.
func readAndDecode[T any](
	ctx context.Context,
	decoder *inputDecoder,
	read func(context.Context) ([]T, error),
	decode func(context.Context, *inputDecoder, T) iter.Seq2[task, error],
	tasks chan<- []task,
) error {
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
		batch := taskBatch{
			tasks:   tasks,
			pending: make([]task, 0, batchRows),
		}
		for {
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case items, ok := <-inputs:
				if !ok {
					return batch.flush(ctx)
				}
				for index := range items {
					data := items[index]
					var zero T
					items[index] = zero
					for work, err := range decode(ctx, decoder, data) {
						if err != nil {
							return err
						}
						batch.pending = append(batch.pending, work)
						_, row := work.(*decodeTask)
						if len(batch.pending) == batchRows || !row {
							if err := batch.flush(ctx); err != nil {
								return err
							}
						}
					}
				}
				if err := batch.flush(ctx); err != nil {
					return err
				}
			}
		}
	})
	return g.Wait()
}

type taskBatch struct {
	tasks   chan<- []task
	pending []task
}

func (b *taskBatch) flush(ctx context.Context) error {
	if len(b.pending) == 0 {
		return nil
	}
	select {
	case b.tasks <- b.pending:
		b.pending = make([]task, 0, batchRows)
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (r *reader) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-r.confirmable:
			if err := r.client.Confirm(ctx, r.checkpoint.Load()); err != nil {
				if ctx.Err() == nil {
					log.Error("consumer input confirmation failed", zap.Error(err))
				}
				return err
			}
		}
	}
}

func notifyConfirmation(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
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
	stream    int32
	watermark uint64
	boundary  *readBoundary
}

// A boundary either completes a finite input scope or exposes a shared control
// watermark. Only the conductor advances these facts.
type readBoundary struct {
	reached   bool
	watermark *uint64
}
