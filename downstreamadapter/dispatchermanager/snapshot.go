// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package dispatchermanager

import (
	"context"
	"encoding/base64"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/integrity"
	"github.com/pingcap/ticdc/pkg/snapshot/bootstrap"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

// ApplySnapshot is the second data entry to the same capture-owned sink. It
// mounts frozen schema, preserves ordinary row routing, and waits for broker
// callbacks. It does not start or own the CSE process.
func (e *DispatcherManager) ApplySnapshot(ctx context.Context, epoch uint64, spec protocol.Spec, ref protocol.ObjectRef, ex protocol.Export) (uint64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(e.ctx, cancel)
	defer stop()
	if e.GetMaintainerEpoch() != epoch || e.writePathClosed.Load() {
		return 0, protocol.Invalid("capture write authority lost")
	}
	var bundle bootstrap.SchemaBundle
	if err := protocol.Read(spec.SchemaRef, &bundle); err != nil {
		return 0, err
	}
	if bundle.SnapshotTS != spec.SnapshotTS {
		return 0, protocol.Invalid("schema timestamp mismatch")
	}
	var table *common.TableInfo
	var schema string
	for _, s := range bundle.Tables {
		if s.TableID == ex.Range.TableID {
			var err error
			table, err = common.UnmarshalJSONToTableInfo(s.Data)
			if err != nil {
				return 0, protocol.Wrap(err)
			}
			schema = base64.StdEncoding.EncodeToString(s.Data)
			break
		}
	}
	if table == nil {
		return 0, protocol.Invalid("missing frozen table schema")
	}
	mounter := commonEvent.NewMounter(time.UTC, &integrity.Config{})
	var rows uint64
	done := make(chan struct{}, 128)
	queued := 0
	wait := func() error {
		for queued > 0 {
			select {
			case <-done:
				queued--
			case <-ctx.Done():
				return protocol.Wrap(ctx.Err())
			}
		}
		return nil
	}
	for seq, c := range ex.Chunks {
		expected := protocol.Header{Version: 1, Compression: "none", JobID: spec.JobID, SnapshotID: spec.SnapshotID, SnapshotTS: spec.SnapshotTS, PlanDigest: ref.Digest, RangeID: ex.Range.RangeID, AttemptID: ex.AttemptID, TableID: ex.Range.TableID, Seq: uint64(seq)}
		_, err := protocol.DecodeChunk(c, expected, ex.Range, func(key, value []byte) error {
			if err := ctx.Err(); err != nil {
				return protocol.Wrap(err)
			}
			if e.GetMaintainerEpoch() != epoch || e.writePathClosed.Load() {
				return protocol.Invalid("snapshot writer fenced")
			}
			chk := chunk.NewChunkWithCapacity(table.GetFieldSlice(), 1)
			raw := &common.RawKVEntry{Key: key, Value: value, CRTs: spec.SnapshotTS}
			n, checksum, err := mounter.DecodeToChunk(raw, table, chk)
			if err != nil {
				return protocol.Wrap(err)
			}
			if n != 1 {
				return protocol.Invalid("snapshot KV did not mount to one row")
			}
			ev := commonEvent.NewDMLEvent(common.NewDispatcherID(), ex.Range.TableID, 0, spec.SnapshotTS, table)
			ev.Rows = chk
			ev.Length = 1
			ev.RowTypes = []common.RowType{common.RowTypeInsert}
			ev.RowKeys = [][]byte{key}
			ev.ApproximateSize = int64(len(key) + len(value))
			ev.Snapshot = &commonEvent.SnapshotRow{ID: protocol.RecordID(spec.SnapshotID, ex.Range.TableID, key), SnapshotID: spec.SnapshotID, Timestamp: spec.SnapshotTS, Schema: schema}
			if checksum != nil {
				ev.Checksum = append(ev.Checksum, checksum)
			}
			var once sync.Once
			ev.AddPostFlushFunc(func() { once.Do(func() { done <- struct{}{} }) })
			e.writeSink.AddDMLEvent(ev)
			queued++
			rows++
			if queued >= 128 {
				return wait()
			}
			return nil
		})
		if err != nil {
			return rows, err
		}
	}
	if err := wait(); err != nil {
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
