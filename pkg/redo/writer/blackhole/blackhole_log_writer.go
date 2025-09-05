// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"go.uber.org/zap"
)

var _ writer.RedoLogWriter = (*blackHoleWriter)(nil)

// blackHoleSink defines a blackHole storage, it receives events and persists
// without any latency
type blackHoleWriter struct {
	invalid bool
}

// NewLogWriter creates a blackHole writer
func NewLogWriter(invalid bool) *blackHoleWriter {
	return &blackHoleWriter{
		invalid: invalid,
	}
}

func (bs *blackHoleWriter) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (bs *blackHoleWriter) WriteEvents(_ context.Context, events ...writer.RedoEvent) (err error) {
	if bs.invalid {
		return errors.New("[WriteLog] invalid black hole writer")
	}
	rl := events[len(events)-1].ToRedoLog()
	current := rl.GetCommitTs()
	log.Debug("write redo events", zap.Int("count", len(events)),
		zap.Uint64("current", current))
	for _, rl := range events {
		rl.PostFlush()
	}
	return
}

func (bs *blackHoleWriter) Close() error {
	return nil
}
