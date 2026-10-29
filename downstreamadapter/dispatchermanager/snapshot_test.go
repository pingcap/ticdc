// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package dispatchermanager

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/snapshot/bootstrap"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/ticdc/pkg/snapshot/store"
	"github.com/stretchr/testify/require"
)

func TestSnapshotBatchAcksBudgetAndFence(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	job := helper.DDL2Job("create table test.snap_batch (id int primary key, v varchar(50))")
	table := helper.GetTableInfo(job)
	raw := helper.DML2RawKv(table.TableName.TableID, table.GetUpdateTS(),
		"insert into test.snap_batch values(1,'first')", "insert into test.snap_batch values(2,'second')", "insert into test.snap_batch values(3,'third')")
	objects := store.Local{Root: t.TempDir()}
	schema, err := table.Marshal()
	require.NoError(t, err)
	schemaRef, err := objects.Put("schema.json", bootstrap.SchemaBundle{SnapshotTS: 42, Tables: []bootstrap.Schema{{TableID: table.TableName.TableID, Data: schema}}})
	require.NoError(t, err)
	spec := protocol.Spec{JobID: "j", SnapshotID: "s", SnapshotTS: 42, SchemaRef: schemaRef}
	plan := protocol.ObjectRef{Digest: "p"}
	span := protocol.Span{RangeID: "r", TableID: table.TableName.TableID, Start: raw[0].Key, End: append(bytes.Clone(raw[2].Key), 0)}
	header := protocol.Header{Version: 1, Compression: "none", JobID: "j", SnapshotID: "s", SnapshotTS: 42, PlanDigest: "p", RangeID: "r", AttemptID: "a", TableID: span.TableID}
	var file, block bytes.Buffer
	u32 := func(b *bytes.Buffer, n int) { require.NoError(t, binary.Write(b, binary.LittleEndian, uint32(n))) }
	metadata := func(v any) []byte { b, err := json.Marshal(v); require.NoError(t, err); return b }
	file.WriteString("SNAPKV01")
	h := metadata(header)
	u32(&file, len(h))
	file.Write(h)
	var size uint64
	for _, kv := range raw {
		u32(&block, len(kv.Key))
		u32(&block, len(kv.Value))
		block.Write(kv.Key)
		block.Write(kv.Value)
		size += uint64(len(kv.Key) + len(kv.Value))
	}
	u32(&file, block.Len())
	u32(&file, len(raw))
	file.Write(block.Bytes())
	footer := metadata(protocol.Footer{Blocks: 1, KVCount: 3, LogicalBytes: size, First: raw[0].Key, Last: raw[2].Key})
	u32(&file, 0)
	u32(&file, len(footer))
	file.Write(footer)
	file.WriteString("SNAPEND1")
	path := filepath.Join(objects.Root, "chunk")
	require.NoError(t, os.WriteFile(path, file.Bytes(), 0o600))
	object, err := protocol.Ref(path)
	require.NoError(t, err)
	ex := protocol.Export{Range: span, AttemptID: "a", KVCount: 3, Chunks: []protocol.Chunk{{ObjectRef: object, KVCount: 3, LogicalBytes: size, First: raw[0].Key, Last: raw[2].Key}}}
	sk := mock.NewMockSink(gomock.NewController(t))
	submitted := make(chan *commonEvent.DMLEvent, 4)
	sk.EXPECT().AddDMLEvent(gomock.Any()).Do(func(ev *commonEvent.DMLEvent) { submitted <- ev }).AnyTimes()
	manager := &DispatcherManager{ctx: context.Background(), writeSink: sk, sinkStopped: make(chan struct{})}
	manager.meta.maintainerEpoch = 7
	run := func(ctx context.Context) <-chan error {
		done := make(chan error, 1)
		go func() {
			n, err := manager.ApplySnapshot(ctx, 7, spec, plan, ex)
			if err == nil && n != 3 {
				done <- protocol.Invalid("unexpected count")
				return
			}
			done <- err
		}()
		return done
	}
	done := run(context.Background())
	var ev *commonEvent.DMLEvent
	select {
	case ev = <-submitted:
	case <-time.After(10 * time.Second):
		t.Fatal("no batch")
	}
	require.EqualValues(t, 3, ev.Length)
	require.Equal(t, 3, ev.Rows.NumRows())
	require.Equal(t, "first", ev.Rows.GetRow(0).GetString(1))
	require.Equal(t, "third", ev.Rows.GetRow(2).GetString(1))
	require.Equal(t, raw[0].Key, ev.RowKeys[0])
	require.Equal(t, raw[2].Key, ev.RowKeys[2])
	select {
	case <-done:
		t.Fatal("range completed before ACK")
	default:
	}
	ev.PostFlush()
	ev.PostFlush() // duplicate callback must not release budget twice
	require.NoError(t, <-done)
	runtime, err := manager.getSnapshotRuntime(spec)
	require.NoError(t, err)
	require.True(t, runtime.budget.TryAcquire(runtime.limit))
	runtime.budget.Release(runtime.limit)
	require.NoError(t, os.Remove(schemaRef.URI))
	cached, err := manager.getSnapshotRuntime(spec)
	require.NoError(t, err)
	require.Same(t, runtime, cached)
	changed := spec
	changed.SnapshotID = "other"
	_, err = manager.getSnapshotRuntime(changed)
	require.ErrorContains(t, err, "identity changed")

	// Backpressure can be canceled before submission, without leaking a waiter.
	require.NoError(t, runtime.budget.Acquire(context.Background(), runtime.limit))
	ctx, cancel := context.WithCancel(context.Background())
	done = run(ctx)
	cancel()
	require.Error(t, <-done)
	runtime.budget.Release(runtime.limit)
	select {
	case <-submitted:
		t.Fatal("submitted without budget")
	default:
	}

	done = run(context.Background())
	ev = <-submitted
	manager.writePathClosed.Store(true)
	ev.PostFlush()
	require.ErrorContains(t, <-done, "fenced")
	manager.writePathClosed.Store(false)
	done = run(context.Background())
	ev = <-submitted
	close(manager.sinkStopped)
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("sink failure stranded range ACK waiter")
	}
	ev.PostFlush()
}

func TestSnapshotAcksCancellation(t *testing.T) {
	acks := newSnapshotAcks()
	released := 0
	first := acks.add(func() { released++ })
	second := acks.add(func() { released++ })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.Error(t, acks.wait(ctx))
	first()
	first()
	second()
	require.NoError(t, acks.wait(context.Background()))
	require.Equal(t, 2, released)
}
