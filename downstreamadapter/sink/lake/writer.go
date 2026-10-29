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

package lake

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
)

type commitState uint8

const (
	commitUnknown commitState = iota // Missing evidence must never mean success.
	committed
	notCommitted
)

// mutationResult is produced at the client boundary, before any inner retry.
// observationErr is valid only after commit has independently been confirmed.
type mutationResult struct {
	state          commitState
	retryable      bool
	observationErr error
}

// executeMutation must read the server terminal state, not just the first result
// page. The HTTP adapter supplies it; transport errors alone must not
// be labelled notCommitted. Arguments and SQL must not be included in errors.
type executeMutation func(context.Context, string, []any) (mutationResult, error)

type applyResult struct {
	// applied remains true if only subsequent raw compaction fails. A worker
	// must not undo/redo already applied source completion to repair maintenance.
	applied bool
}

// tableWriter is a serial raw/merge lane. It owns no source callbacks; its worker
// completes fragments only when applyResult.applied is true.
type tableWriter struct {
	mu          sync.Mutex
	schema      tableSchema
	execute     executeMutation
	batchRows   int
	maxAttempts int
	epoch       uint64
	clock       addTimeClock
	written     uint64
	merged      uint64
	failed      error
}

func newTableWriter(schema tableSchema, execute executeMutation, batchRows, maxAttempts int, epoch uint64) (*tableWriter, error) {
	if err := schema.validate(); err != nil {
		return nil, err
	}
	if execute == nil || batchRows <= 0 || maxAttempts <= 0 || epoch > math.MaxInt64 {
		return nil, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid table writer configuration")
	}
	// Keep metadata owned even if the caller reuses its slices for a later DDL.
	schema.columns = append([]column(nil), schema.columns...)
	schema.primaryKey = append([]string(nil), schema.primaryKey...)
	return &tableWriter{schema: schema, execute: execute, batchRows: batchRows, maxAttempts: maxAttempts, epoch: epoch}, nil
}

// applyBatch requires the target/raw tables and the durable schema epoch to
// have been initialized by the table worker. It preserves source row order and
// never writes a row at/before the applied epoch into the new projection.
func (w *tableWriter) applyBatch(ctx context.Context, rows []normalizedRow, now time.Time) (applyResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return applyResult{}, w.failed
	}
	// Validate the complete batch before issuing any mutation.
	for _, row := range rows {
		if row.id == "" || !json.Valid([]byte(row.payload)) || row.commitTs == 0 || row.commitTs > math.MaxInt64 ||
			(row.action != insertAction && row.action != updateAction && row.action != deleteAction) {
			return applyResult{}, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid normalized row")
		}
	}
	pending := make([]normalizedRow, 0, len(rows))
	for _, row := range rows {
		if row.commitTs > w.epoch {
			pending = append(pending, row)
		}
	}
	if len(pending) == 0 {
		// Only a proven applied DDL fence permits this no-op, not schema bootstrap.
		return applyResult{applied: true}, nil
	}
	for start := 0; start < len(pending); start += w.batchRows {
		end := min(start+w.batchRows, len(pending))
		statement, args := w.schema.insertRawSQL(pending[start:end], &w.clock, now)
		if err := w.runMutation(ctx, statement, args); err != nil {
			w.failed = err
			return applyResult{}, err
		}
		w.written++
	}
	if err := w.runMutation(ctx, w.schema.mergeSQL(w.epoch), nil); err != nil {
		w.failed = err
		return applyResult{}, err
	}
	w.merged = w.written
	result := applyResult{applied: true}
	if err := w.runMutation(ctx, w.schema.compactRawSQL(), nil); err != nil {
		w.failed = err
		return result, err
	}
	return result, nil
}

func (w *tableWriter) runMutation(ctx context.Context, statement string, args []any) error {
	for attempt := 0; attempt < w.maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return errors.WrapError(errors.ErrLakeExecution, err)
		}
		result, err := w.execute(ctx, statement, args)
		switch result.state {
		case committed:
			if err != nil {
				// Contradictory execution evidence is not an observation error.
				return errors.WrapError(errors.ErrLakeCommitUnknown, err)
			}
			// A confirmed commit is not undone by result statistics parsing.
			return nil
		case notCommitted:
			if !result.retryable || attempt+1 == w.maxAttempts {
				if err != nil {
					return errors.WrapError(errors.ErrLakeExecution, err)
				}
				return errors.ErrLakeExecution.GenWithStackByArgs()
			}
		default:
			// Check before transient-error handling: no unknown request is resent.
			if err != nil {
				return errors.WrapError(errors.ErrLakeCommitUnknown, err)
			}
			return errors.ErrLakeCommitUnknown.GenWithStackByArgs()
		}
	}
	return errors.ErrLakeExecution.GenWithStackByArgs()
}
