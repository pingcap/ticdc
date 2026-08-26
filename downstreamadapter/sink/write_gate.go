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

package sink

import (
	"context"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/writelease"
)

type writeGatedSink struct {
	Sink
	ctx  context.Context
	gate *writelease.Gate
}

// WithWriteGate prevents new downstream side effects from entering a sink
// while the capture cannot prove that it still owns the write lease.
func WithWriteGate(ctx context.Context, inner Sink, gate *writelease.Gate) Sink {
	if gate == nil {
		return inner
	}
	return &writeGatedSink{
		Sink: inner,
		ctx:  ctx,
		gate: gate,
	}
}

func (s *writeGatedSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	if s.waitUntilWritable() {
		s.Sink.AddDMLEvent(event)
	}
}

func (s *writeGatedSink) FlushDMLBeforeBlock(event commonEvent.BlockEvent) error {
	if err := s.gate.WaitUntilWritable(s.ctx); err != nil {
		return err
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	return s.Sink.FlushDMLBeforeBlock(event)
}

func (s *writeGatedSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	if err := s.gate.WaitUntilWritable(s.ctx); err != nil {
		return err
	}
	if err := s.ensureWritable(); err != nil {
		return err
	}
	return s.Sink.WriteBlockEvent(event)
}

func (s *writeGatedSink) AddCheckpointTs(ts uint64) {
	// Checkpoints are periodic and superseded by newer values, so dropping one
	// while blocked avoids stalling the checkpoint message stream.
	if s.gate.IsWritable() {
		s.Sink.AddCheckpointTs(ts)
	}
}

func (s *writeGatedSink) waitUntilWritable() bool {
	for {
		if err := s.gate.WaitUntilWritable(s.ctx); err != nil {
			return false
		}
		if s.gate.IsWritable() {
			return true
		}
	}
}

func (s *writeGatedSink) ensureWritable() error {
	for {
		if s.gate.IsWritable() {
			return nil
		}
		if err := s.gate.WaitUntilWritable(s.ctx); err != nil {
			return err
		}
	}
}
