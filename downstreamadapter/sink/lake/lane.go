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
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	codec "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/spool"
)

type spoolRow struct {
	ID       string    `json:"id"`
	Payload  string    `json:"payload"`
	CommitTs uint64    `json:"commit_ts"`
	Action   rowAction `json:"action"`
}
type pendingFragment struct {
	entry      *spool.Entry
	completion *fragmentCompletion
	count      int
	bytes      int64
}
type pendingTable struct {
	state     *tableState
	fragments []pendingFragment
	rows      int
	bytes     int64
}
type tableLane struct {
	sink    *Sink
	input   chan workItem
	schemas *schemaManager
	batches map[string]*pendingTable
	entries int
}

func (l *tableLane) run(ctx context.Context) error {
	timer := time.NewTicker(l.sink.opts.flushInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-timer.C:
			if err := l.flushAll(ctx); err != nil {
				return err
			}
		case item := <-l.input:
			l.sink.mu.Lock()
			l.sink.notifyAdmissionLocked()
			l.sink.mu.Unlock()
			var err error
			switch {
			case item.event != nil:
				err = l.encode(ctx, &item)
			case item.ddl != nil:
				err = l.sink.statistics.RecordDDLExecution(item.ddl.GetDDLType().String(), func() error { return l.schemas.applyDDL(ctx, item.ddl) })
			default:
				err = l.flushAll(ctx)
			}
			if err != nil {
				l.sink.stop(err)
				err = context.Cause(l.sink.ctx)
			}
			if item.done != nil {
				item.done <- err
			}
			if err != nil {
				return err
			}
		}
	}
}

func (l *tableLane) encode(ctx context.Context, item *workItem) (err error) {
	event := item.event
	item.event = nil
	released := false
	release := func() {
		if !released {
			released = true
			l.sink.releaseSource(item.charge)
		}
	}
	defer release()
	s, err := schemaFromNative(event.TableInfo, l.sink.opts.database, l.sink.opts.conflictKey)
	if err != nil {
		return err
	}
	state, skip, err := l.schemas.prepareDML(ctx, s, event.CommitTs)
	if err != nil {
		return err
	}
	enqueue, flush := event.DetachPostCallbacks()
	tracker := newEventCompletion(flush)
	defer func() {
		if err != nil {
			tracker.fail()
		}
	}()
	if skip {
		event = nil
		release()
		enqueue()
		tracker.seal()
		return nil
	}
	var blob []byte
	rows := 0
	publish := func(last bool) error {
		if rows == 0 {
			if last {
				enqueue()
			}
			return nil
		}
		blob = append(blob, ']')
		fragment, ok := tracker.register()
		if !ok {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("fragment registered after completion")
		}
		var callback func()
		if last {
			callback = enqueue
		}
		err := l.enqueue(ctx, state, blob, rows, fragment, callback)
		blob = nil
		rows = 0
		return err
	}
	count := int32(0)
	for {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		change, ok := event.GetNextRow()
		if !ok {
			break
		}
		row, err := normalizeChange(event.TableInfo, s, change, event.CommitTs, l.sink.opts.timezone)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(spoolRow{row.id, row.payload, row.commitTs, row.action})
		if err != nil {
			return errors.WrapError(errors.ErrLakeInvalidData, err, "cannot encode spool row")
		}
		if int64(len(encoded)+2) > l.sink.opts.batchBytes {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("single encoded row exceeds batch byte limit")
		}
		if rows > 0 && (int64(len(blob)+len(encoded)+2) > l.sink.opts.batchBytes || rows >= l.sink.opts.batchRows) {
			if err := publish(false); err != nil {
				return err
			}
		}
		if rows == 0 {
			blob = append(blob, '[')
		} else {
			blob = append(blob, ',')
		}
		blob = append(blob, encoded...)
		rows++
		count++
	}
	if count != event.Len() {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("incomplete native row iteration")
	}
	event = nil
	release() // Only callback closures and immutable bounded fragments remain.
	if err := publish(true); err != nil {
		return err
	}
	tracker.seal()
	return nil
}

func (l *tableLane) enqueue(ctx context.Context, state *tableState, blob []byte, count int, completion *fragmentCompletion, enqueue func()) error {
	msg := &codec.Message{Value: blob} // Never attach source success to spool.Callback.
	msgs := []*codec.Message{msg}
	if spool.AccountingBytes(msgs) > l.sink.opts.spoolBytes {
		return errors.ErrLakeInvalidData.GenWithStackByArgs("encoded fragment exceeds spool quota")
	}
	for {
		action, entry, err := l.sink.spool.TryEnqueue(msgs, enqueue)
		if err != nil {
			return err
		}
		if action == spool.EnqueueActionAcceptedOversized {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("spool size invariant violated")
		}
		if action == spool.EnqueueActionWaitDiskQuota {
			// Drain every local table, not just the large event. Other lanes keep
			// draining on their own timers; released segment files remain charged.
			if err := l.flushAll(ctx); err != nil {
				return err
			}
			if err := l.sink.spool.WaitForDiskQuota(ctx, msgs); err != nil {
				return err
			}
			continue
		}
		key := schemaKey(state.schema)
		batch := l.batches[key]
		if batch == nil {
			batch = &pendingTable{state: state}
			l.batches[key] = batch
		}
		batch.fragments = append(batch.fragments, pendingFragment{entry, completion, count, int64(len(blob))})
		batch.rows += count
		batch.bytes += int64(len(blob))
		l.entries++
		if batch.bytes >= l.sink.opts.batchBytes || batch.rows >= l.sink.opts.batchRows {
			return l.flush(ctx, key)
		}
		if l.entries >= l.sink.opts.maxEntries {
			return l.flushAll(ctx)
		}
		return nil
	}
}

func (l *tableLane) flushAll(ctx context.Context) error {
	for key := range l.batches {
		if err := l.flush(ctx, key); err != nil {
			return err
		}
	}
	return nil
}

func (l *tableLane) flush(ctx context.Context, key string) error {
	batch := l.batches[key]
	if batch == nil {
		return nil
	}
	// Bound remote retained-raw admission by monitoring only. The measurement is
	// neither a cross-capture reservation nor an absolute physical-cost ceiling.
	if l.sink.opts.remoteLimit > 0 {
		rows, err := l.sink.client.selectRows(ctx, "SELECT COALESCE(SUM(data_compressed_size), 0)::VARCHAR FROM system.tables WHERE database = ? AND ends_with(name, '_cdc_raw')", []any{batch.state.schema.database})
		if err != nil {
			return err
		}
		n, err := parseRemoteBytes(rows)
		if err != nil {
			return err
		}
		if n >= l.sink.opts.remoteLimit {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("remote raw monitoring threshold reached; expand budget or reinitialize manually")
		}
	}
	var rows []normalizedRow
	for _, f := range batch.fragments {
		reader, err := l.sink.spool.NewMessageReader(f.entry)
		if err != nil {
			return err
		}
		_, data, _, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("empty spool fragment")
		}
		var decoded []spoolRow
		if err := json.Unmarshal(data, &decoded); err != nil {
			return errors.WrapError(errors.ErrLakeInvalidData, err, "cannot decode spool fragment")
		}
		if len(decoded) != f.count {
			return errors.ErrLakeInvalidData.GenWithStackByArgs("incomplete spool fragment")
		}
		for _, r := range decoded {
			rows = append(rows, normalizedRow{r.ID, r.Payload, r.CommitTs, r.Action})
		}
	}
	result, err := batch.state.writer.applyBatch(ctx, rows, time.Now())
	if err != nil {
		l.sink.statistics.RecordDMLError()
	} else {
		l.sink.statistics.RecordDMLBatch(len(rows), batch.bytes)
	}
	if result.applied {
		delete(l.batches, key)
		l.entries -= len(batch.fragments)
		// Reclaim space separately from source completion; OPEN events keep only
		// counters, never already-applied payloads. Release never invokes success.
		for _, f := range batch.fragments {
			l.sink.spool.Release(f.entry)
			f.completion.applied()
		}
	}
	return err
}

func (l *tableLane) cleanup() {
	for _, b := range l.batches {
		for _, f := range b.fragments {
			f.completion.parent.fail()
			l.sink.spool.Release(f.entry)
		}
	}
	clear(l.batches)
	for {
		select {
		case item := <-l.input:
			if item.event != nil {
				l.sink.releaseSource(item.charge)
			}
		default:
			return
		}
	}
}

// parseRemoteBytes rejects malformed measurements instead of treating them as 0.
func parseRemoteBytes(rows [][]*string) (int64, error) {
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] == nil {
		return 0, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid remote raw measurement")
	}
	var n int64
	if err := json.Unmarshal([]byte(*rows[0][0]), &n); err != nil || n < 0 {
		return 0, errors.ErrLakeInvalidData.GenWithStackByArgs("invalid remote raw byte count")
	}
	return n, nil
}
