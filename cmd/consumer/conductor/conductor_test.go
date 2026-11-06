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

package conductor

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/pingcap/ticdc/cmd/consumer/budget"
	"github.com/pingcap/ticdc/cmd/consumer/decode"
	"github.com/pingcap/ticdc/cmd/consumer/source"
	"github.com/pingcap/ticdc/cmd/consumer/writer"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/atomic"
)

type fakeReader struct {
	streams    []source.Stream
	watermarks map[int32]uint64
	target     uint64
	scope      []event.SchemaTableName
	proven     []int32
	readCalls  int
}

func (r *fakeReader) Read(context.Context) ([]*source.Input, error) { return nil, nil }
func (r *fakeReader) Streams() []source.Stream                      { return r.streams }
func (r *fakeReader) Semantics() source.Semantics                   { return source.Semantics{} }
func (r *fakeReader) ContinueReading(bool)                          {}
func (r *fakeReader) Close()                                        {}
func (r *fakeReader) Advance(progress source.Progress) bool {
	if r.watermarks == nil {
		r.watermarks = make(map[int32]uint64)
	}
	for _, watermark := range progress.Watermarks {
		r.watermarks[watermark.Stream] = max(r.watermarks[watermark.Stream], watermark.Timestamp)
	}
	if progress.Retarget {
		r.target, r.scope = progress.Target, progress.Scope
	}
	r.proven = progress.Proven
	if r.target == 0 {
		return true
	}
	for _, stream := range r.streams {
		if slices.Contains(progress.Proven, stream.ID) {
			continue
		}
		if r.watermarks[stream.ID] < r.target {
			return false
		}
	}
	return true
}

func newTestWriter(t *testing.T, memory *budget.Budget) *writer.Writer {
	t.Helper()
	return writer.New(t.Context(), writer.Options{
		DownstreamURI: "blackhole://",
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		CodecConfig:   common.NewConfig(config.ProtocolOpen),
		Budget:        memory,
	})
}

func TestConfirmsRegisteredInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	memory := budget.New()
	reader := &fakeReader{}
	c := New(memory, reader)
	acknowledged := make(chan uint64, 2)
	first := source.Origin{Stream: 0, Ack: func(context.Context) error { acknowledged <- 100; return nil }}
	second := source.Origin{Stream: 0, Ack: func(context.Context) error { acknowledged <- 200; return nil }}
	// A decoded marker alone is insufficient: the rest of its input may still
	// contain events. Registration of the source-only task closes that prefix.
	require.NoError(t, c.accept(decode.Event{Origin: first, Event: &event.ResolvedEvent{ResolvedTs: 100}}))
	require.Empty(t, c.positions)
	require.NoError(t, c.accept(decode.Event{Origin: first}))
	pending := &event.DMLEvent{CommitTs: 150, PhysicalTableID: 42}
	require.NoError(t, c.accept(decode.Event{Origin: second, Event: pending}))
	require.NoError(t, c.accept(decode.Event{Origin: second, Event: &event.ResolvedEvent{ResolvedTs: 200}}))
	require.NoError(t, c.accept(decode.Event{Origin: second}))
	c.checkpoint.Store(149)
	stopped := make(chan error, 1)
	go func() { stopped <- c.confirm(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	}()
	notifyConfirmation(c.confirmable)
	select {
	case ts := <-acknowledged:
		require.Equal(t, uint64(100), ts)
	case <-time.After(10 * time.Second):
		t.Fatal("covered input was not acknowledged")
	}
	require.Eventually(t, func() bool { return memory.Used() == sourcePositionBytes }, 10*time.Second, time.Millisecond)
	select {
	case ts := <-acknowledged:
		t.Fatalf("acknowledged uncovered input %d", ts)
	default:
	}
	c.checkpoint.Store(200)
	notifyConfirmation(c.confirmable)
	select {
	case ts := <-acknowledged:
		require.Equal(t, uint64(200), ts)
	case <-time.After(10 * time.Second):
		t.Fatal("checkpoint advancement did not confirm input")
	}
	require.Eventually(t, func() bool { return memory.Used() == 0 }, 10*time.Second, time.Millisecond)
}

func TestReadingBoundaries(t *testing.T) {
	memory := budget.New()
	reader := &fakeReader{streams: []source.Stream{
		{ID: 0, Topic: "t", Control: true},
		{ID: 1, Topic: "t"},
	}}
	c := New(memory, reader)
	w := newTestWriter(t, memory)
	c.readThrough = 100
	c.advanceReading(w)
	require.NotNil(t, c.boundary)
	require.Equal(t, uint64(100), reader.target)
	require.False(t, c.boundary.reached)

	row := &dmlTask{stream: 1, dml: &event.DMLEvent{CommitTs: 90}}
	require.False(t, c.dmlReady(row, 0))
	require.NoError(t, c.accept(decode.Event{Origin: source.Origin{Stream: 0}, Event: &event.ResolvedEvent{ResolvedTs: 100}}))
	require.True(t, c.dmlReady(row, 0), "the control stream covers preceding DDLs")
	require.False(t, c.boundary.reached, "the second partition still has to catch up")

	require.NoError(t, c.accept(decode.Event{Origin: source.Origin{Stream: 1}, Event: &event.ResolvedEvent{ResolvedTs: 100}}))
	c.collectWatermarks()
	require.True(t, c.boundary.reached)
	require.Equal(t, uint64(100), c.watermark)

	tables := []event.SchemaTableName{{SchemaName: "test", TableName: "t"}}
	boundary := c.requestBoundary(tables, 200)
	require.False(t, boundary.reached)
	require.Equal(t, tables, reader.scope)
	require.NoError(t, c.accept(decode.Event{Origin: source.Origin{Stream: 1}, Event: &event.ResolvedEvent{ResolvedTs: 200}}))
	require.False(t, boundary.reached, "a new global scope must still cover every input")
	require.NoError(t, c.accept(decode.Event{Origin: source.Origin{Stream: 0}, Event: &event.ResolvedEvent{ResolvedTs: 200}}))
	require.True(t, boundary.reached)
}

func TestDDLOrdersEarlierAndLaterRows(t *testing.T) {
	memory := budget.New()
	c := New(memory, &fakeReader{})
	ddl := &event.DDLEvent{
		Type:       byte(model.ActionAddColumn),
		SchemaName: "test",
		TableName:  "t",
		FinishedTs: 100,
		BlockedTables: &event.InfluencedTables{
			InfluenceType: event.InfluenceTypeNormal,
			TableIDs:      []int64{1},
		},
	}
	c.queueDDL(ddl)
	earlier := &dmlTask{stream: 0, dml: &event.DMLEvent{CommitTs: 90, PhysicalTableID: 1}}
	later := &dmlTask{stream: 0, dml: &event.DMLEvent{CommitTs: 110, PhysicalTableID: 1}}
	c.queueDML(earlier)
	c.queueDML(later)

	// The row before the DDL must reach the writer first; the DDL itself is
	// selected before any row that follows it.
	selected, ok := c.nextReady(100).(*dmlTask)
	require.True(t, ok)
	require.Same(t, earlier, selected)
	c.popDML(earlier)
	selectedDDL, ok := c.nextReady(100).(*ddlTask)
	require.True(t, ok)
	require.Same(t, ddl, selectedDDL.ddl)
	require.False(t, c.rowReady(later, 100))

	c.popDDL().release()
	require.True(t, c.rowReady(later, 100))
}

func TestCheckpointNotifiesListener(t *testing.T) {
	var observed []uint64
	conductor := &Conductor{
		checkpoint:  atomic.NewUint64(0),
		confirmable: make(chan struct{}, 1),
		onCheckpoint: func(checkpoint uint64) {
			observed = append(observed, checkpoint)
		},
	}
	conductor.observeCheckpoint(100)
	conductor.observeCheckpoint(100)
	conductor.observeCheckpoint(150)
	require.Equal(t, []uint64{100, 150}, observed)
}

func TestConfirmsDMLOnlyInput(t *testing.T) {
	memory := budget.New()
	c := New(memory, &fakeReader{})
	acknowledged := make(chan struct{}, 1)
	origin := source.Origin{
		Stream: 0,
		Ack:    func(context.Context) error { acknowledged <- struct{}{}; return nil },
	}
	require.NoError(t, c.accept(decode.Event{Origin: origin, Event: &event.DMLEvent{CommitTs: 100, PhysicalTableID: 42}}))
	require.Empty(t, c.positions, "a position is confirmable only after its input ends")
	require.NoError(t, c.accept(decode.Event{Origin: origin}))
	require.Len(t, c.positions[0], 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- c.confirm(ctx) }()
	defer func() {
		cancel()
		require.ErrorIs(t, <-stopped, context.Canceled)
	}()
	c.checkpoint.Store(99)
	notifyConfirmation(c.confirmable)
	select {
	case <-acknowledged:
		t.Fatal("acknowledged an input below the checkpoint")
	case <-time.After(50 * time.Millisecond):
	}
	c.checkpoint.Store(100)
	notifyConfirmation(c.confirmable)
	select {
	case <-acknowledged:
	case <-time.After(10 * time.Second):
		t.Fatal("DML-only input was not acknowledged")
	}
	require.Eventually(t, func() bool { return memory.Used() == 0 }, 10*time.Second, time.Millisecond)
}

func TestQueueDMLSortsUnorderedRows(t *testing.T) {
	c := New(budget.New(), &fakeReader{})
	for _, ts := range []uint64{100, 90, 95} {
		c.queueDML(&dmlTask{
			dml:    &event.DMLEvent{CommitTs: ts, PhysicalTableID: 42},
			stream: 0,
		})
	}
	rows := c.pendingDML[42][0]
	require.Len(t, rows, 3)
	require.Equal(t, uint64(90), rows[0].dml.CommitTs)
	require.Equal(t, uint64(95), rows[1].dml.CommitTs)
	require.Equal(t, uint64(100), rows[2].dml.CommitTs)
}

func TestInputMarkerUsesMaximumTimestamp(t *testing.T) {
	c := New(budget.New(), &fakeReader{})
	origin := source.Origin{Stream: 0, Ack: func(context.Context) error { return nil }}
	require.NoError(t, c.accept(decode.Event{
		Origin: origin,
		Event:  &event.DMLEvent{CommitTs: 100, PhysicalTableID: 42},
	}))
	require.NoError(t, c.accept(decode.Event{
		Origin: origin,
		Event:  &event.ResolvedEvent{ResolvedTs: 90},
	}))
	require.NoError(t, c.accept(decode.Event{Origin: origin}))
	require.Len(t, c.positions[0], 1)
	require.Equal(t, uint64(100), c.positions[0][0].through)
}

func TestQueueDDLSortsUnorderedTimestamps(t *testing.T) {
	c := New(budget.New(), &fakeReader{})
	c.reorderDDL = true
	for _, ts := range []uint64{120, 100, 110} {
		c.queueDDL(&event.DDLEvent{
			FinishedTs: ts,
			SchemaName: "test",
			TableName:  "t",
			Query:      "alter table test.t add column c int",
		})
	}
	require.Len(t, c.pendingDDL, 3)
	for index, ts := range []uint64{100, 110, 120} {
		require.Equal(t, ts, c.pendingDDL[index].ddl.GetCommitTs())
	}
}

func TestSetDDLTableNames(t *testing.T) {
	testCases := []struct {
		name   string
		action model.ActionType
		query  string
		want   []event.SchemaTableName
	}{
		{
			name:   "rename tables",
			action: model.ActionRenameTable,
			query:  "rename table test.a to test.b, test.c to test.d",
			want: []event.SchemaTableName{
				{SchemaName: "test", TableName: "a"},
				{SchemaName: "test", TableName: "b"},
				{SchemaName: "test", TableName: "c"},
				{SchemaName: "test", TableName: "d"},
			},
		},
		{
			name:   "exchange partition",
			action: model.ActionExchangeTablePartition,
			query:  "alter table test.t exchange partition p with table test.u",
			want: []event.SchemaTableName{
				{SchemaName: "test", TableName: "t"},
				{SchemaName: "test", TableName: "u"},
			},
		},
		{
			name:   "create table like",
			action: model.ActionCreateTable,
			query:  "create table test.t like test.s",
			want: []event.SchemaTableName{
				{SchemaName: "test", TableName: "t"},
				{SchemaName: "test", TableName: "s"},
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ddl := &event.DDLEvent{
				Type:       byte(testCase.action),
				SchemaName: "test",
				Query:      testCase.query,
			}
			require.NoError(t, setDDLTableNames(ddl))
			require.Equal(t, testCase.want, ddl.BlockedTableNames)
		})
	}
}
