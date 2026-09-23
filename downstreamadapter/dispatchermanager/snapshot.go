// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package dispatchermanager

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/snapshot/bootstrap"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"golang.org/x/sync/semaphore"
)

const (
	snapshotBatchRows  = 256
	snapshotBatchBytes = 1 << 20
	// Account for keys, row metadata and callback/queue overhead in addition to KV.
	snapshotRowOverhead = 512
)

type snapshotRuntime struct {
	spec   protocol.Spec
	tables map[int64]*common.TableInfo
	budget *semaphore.Weighted
	limit  int64
}

// The immutable schema bundle is shared by all range workers of this capture.
// Its digest and snapshot identity prevent using live or another generation's
// TableInfo while decoding the frozen backup.
func (e *DispatcherManager) getSnapshotRuntime(spec protocol.Spec) (*snapshotRuntime, error) {
	e.snapshotMu.Lock()
	defer e.snapshotMu.Unlock()
	if r := e.snapshotRuntime; r != nil {
		if r.spec.JobID != spec.JobID || r.spec.SnapshotID != spec.SnapshotID || r.spec.SnapshotTS != spec.SnapshotTS || r.spec.SchemaRef != spec.SchemaRef {
			return nil, protocol.Invalid("snapshot schema identity changed")
		}
		return r, nil
	}
	var cfg *protocol.Config
	if e.config != nil {
		cfg = e.config.Snapshot
	}
	if err := cfg.ValidateRuntime(); err != nil {
		return nil, err
	}
	var bundle bootstrap.SchemaBundle
	if err := protocol.Read(spec.SchemaRef, &bundle); err != nil {
		return nil, err
	}
	if bundle.SnapshotTS != spec.SnapshotTS {
		return nil, protocol.Invalid("schema timestamp mismatch")
	}
	tables := make(map[int64]*common.TableInfo, len(bundle.Tables))
	for _, s := range bundle.Tables {
		table, err := common.UnmarshalJSONToTableInfo(s.Data)
		if err != nil {
			return nil, protocol.Wrap(err)
		}
		if tables[s.TableID] != nil {
			return nil, protocol.Invalid("invalid frozen table identity")
		}
		tables[s.TableID] = table
	}
	r := &snapshotRuntime{spec: spec, tables: tables, limit: cfg.InflightLimit(), budget: semaphore.NewWeighted(cfg.InflightLimit())}
	e.snapshotRuntime = r
	return r, nil
}

// snapshotAcks observes existing sink PostFlush callbacks, not enqueue events.
// Cancellation never leaves a goroutine waiting forever for abandoned callbacks.
type snapshotAcks struct {
	pending atomic.Int64
	changed chan struct{}
}

func newSnapshotAcks() *snapshotAcks { return &snapshotAcks{changed: make(chan struct{}, 1)} }
func (a *snapshotAcks) add(release func()) func() {
	a.pending.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() {
			release()
			if a.pending.Add(-1) == 0 {
				select {
				case a.changed <- struct{}{}:
				default:
				}
			}
		})
	}
}

func (a *snapshotAcks) wait(ctx context.Context) error {
	for a.pending.Load() != 0 {
		select {
		case <-ctx.Done():
			return protocol.Wrap(ctx.Err())
		case <-a.changed:
		}
	}
	if err := ctx.Err(); err != nil {
		return protocol.Wrap(err)
	}
	return nil
}

// ApplySnapshot mounts bounded batches into the existing capture-owned sink.
// Workers share a byte budget released by broker ACKs. Only range completion
// drains all its callbacks; ordinary submission keeps the sink pipeline fed.
func (e *DispatcherManager) ApplySnapshot(ctx context.Context, epoch uint64, spec protocol.Spec, ref protocol.ObjectRef, ex protocol.Export) (uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if e.sinkStopped != nil {
		go func() {
			select {
			case <-e.sinkStopped:
				cancel()
			case <-ctx.Done():
			}
		}()
	}
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	authorized := func() error {
		select {
		case <-e.sinkStopped:
			return protocol.Invalid("snapshot sink stopped")
		default:
		}
		if err := ctx.Err(); err != nil {
			return protocol.Wrap(err)
		}
		if e.GetMaintainerEpoch() != epoch || e.writePathClosed.Load() {
			return protocol.Invalid("snapshot writer fenced")
		}
		return nil
	}
	if err := authorized(); err != nil {
		return 0, err
	}
	runtime, err := e.getSnapshotRuntime(spec)
	if err != nil {
		return 0, err
	}
	table := runtime.tables[ex.Range.TableID]
	if table == nil {
		return 0, protocol.Invalid("missing frozen table schema")
	}
	mounter := commonEvent.NewMounter(time.UTC, &integrity.Config{})
	acks := newSnapshotAcks()
	dispatcherID := common.NewDispatcherID()
	var rows uint64
	var batch *commonEvent.DMLEvent
	submit := func() error {
		if batch == nil || batch.Length == 0 {
			return nil
		}
		charge := batch.ApproximateSize + int64(batch.Length)*snapshotRowOverhead
		if charge > runtime.limit {
			return protocol.Invalid("snapshot batch exceeds in-flight byte budget")
		}
		if err := runtime.budget.Acquire(ctx, charge); err != nil {
			return protocol.Wrap(err)
		}
		if err := authorized(); err != nil {
			runtime.budget.Release(charge)
			return err
		}
		batch.AddPostFlushFunc(acks.add(func() { runtime.budget.Release(charge) }))
		e.writeSink.AddDMLEvent(batch)
		batch = nil
		return nil
	}
	for seq, c := range ex.Chunks {
		expected := protocol.Header{Version: 1, Compression: "none", JobID: spec.JobID, SnapshotID: spec.SnapshotID, SnapshotTS: spec.SnapshotTS, PlanDigest: ref.Digest, RangeID: ex.Range.RangeID, AttemptID: ex.AttemptID, TableID: ex.Range.TableID, Seq: uint64(seq)}
		_, err := protocol.DecodeChunk(c, expected, ex.Range, func(key, value []byte) error {
			if err := authorized(); err != nil {
				return err
			}
			if batch == nil {
				batch = commonEvent.NewDMLEvent(dispatcherID, ex.Range.TableID, 0, spec.SnapshotTS, table)
				batch.Rows = chunk.NewChunkWithCapacity(table.GetFieldSlice(), snapshotBatchRows)
				batch.RowTypes = make([]common.RowType, 0, snapshotBatchRows)
				batch.RowKeys = make([][]byte, 0, snapshotBatchRows)
				batch.Snapshot = &commonEvent.SnapshotRow{SnapshotID: spec.SnapshotID, Timestamp: spec.SnapshotTS}
			}
			raw := &common.RawKVEntry{Key: key, Value: value, CRTs: spec.SnapshotTS}
			n, checksum, err := mounter.DecodeToChunk(raw, table, batch.Rows)
			if err != nil {
				return protocol.Wrap(err)
			}
			if n != 1 {
				return protocol.Invalid("snapshot KV did not mount to one row")
			}
			batch.Length++
			batch.RowTypes = append(batch.RowTypes, common.RowTypeInsert)
			// Do not retain a whole verified input file through a small row key.
			batch.RowKeys = append(batch.RowKeys, bytes.Clone(key))
			batch.ApproximateSize += int64(len(key) + len(value))
			batch.Checksum = append(batch.Checksum, checksum)
			rows++
			if batch.Length >= snapshotBatchRows || batch.ApproximateSize >= snapshotBatchBytes {
				return submit()
			}
			return nil
		})
		if err != nil {
			return rows, err
		}
	}
	if err := submit(); err != nil {
		return rows, err
	}
	if err := acks.wait(ctx); err != nil {
		return rows, err
	}
	if err := authorized(); err != nil {
		return rows, err
	}
	if rows != ex.KVCount {
		return rows, protocol.Invalid("apply count mismatch")
	}
	return rows, nil
}

func (e *DispatcherManager) DrainSnapshot(ctx context.Context, epoch uint64) error {
	if err := ctx.Err(); err != nil {
		return protocol.Wrap(err)
	}
	if e.GetMaintainerEpoch() != epoch || e.writePathClosed.Load() {
		return protocol.Invalid("snapshot drain fenced")
	}
	return nil
}
