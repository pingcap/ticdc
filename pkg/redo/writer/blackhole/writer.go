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

package blackhole

import (
	"context"
	"errors"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
)

var (
	_ writer.RedoDMLWriter = (*blackHoleDMLWriter)(nil)
	_ writer.RedoDDLWriter = (*blackHoleDDLWriter)(nil)
)

// blackHoleSink defines a blackHole storage, it receives events and persists
// without any latency
type blackHoleDMLWriter struct {
	invalid bool
}

type blackHoleDDLWriter struct {
	invalid bool
}

// NewDMLWriter creates a blackHole DML writer.
func NewDMLWriter(invalid bool) *blackHoleDMLWriter {
	return &blackHoleDMLWriter{
		invalid: invalid,
	}
}

// NewDDLWriter creates a blackHole DDL writer.
func NewDDLWriter(invalid bool) *blackHoleDDLWriter {
	return &blackHoleDDLWriter{
		invalid: invalid,
	}
}

func (bs *blackHoleDMLWriter) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bs *blackHoleDMLWriter) AddDMLEvents(_ context.Context, events ...*event.RedoRowEvent) (err error) {
	if bs.invalid {
		return errors.New("[WriteLog] invalid black hole writer")
	}
	if len(events) == 0 {
		return nil
	}
	fields := []zap.Field{zap.Int("count", len(events))}
	for i := len(events) - 1; i >= 0; i-- {
		if events[i] != nil {
			fields = append(fields, zap.Uint64("current", events[i].CommitTs))
			break
		}
	}
	log.Debug("write redo events", fields...)
	for _, e := range events {
		if e != nil {
			e.PostFlush()
		}
	}
	return
}

func (bs *blackHoleDMLWriter) Close() error {
	return nil
}

func (bs *blackHoleDDLWriter) WriteDDLEvent(_ context.Context, event *event.DDLEvent) error {
	if bs.invalid {
		return errors.New("[WriteLog] invalid black hole writer")
	}
	if event != nil {
		log.Debug("write redo ddl", zap.Uint64("commitTs", event.GetCommitTs()))
		event.PostFlush()
	}
	return nil
}

func (bs *blackHoleDDLWriter) SetTableSchemaStore(_ *event.TableSchemaStore) {
}

func (bs *blackHoleDDLWriter) Close() error {
	return nil
}
