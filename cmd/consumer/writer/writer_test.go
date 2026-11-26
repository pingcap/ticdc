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

package writer

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/cmd/consumer/ddl"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

func TestWriterAdvanceCheckpointNotifiesTarget(t *testing.T) {
	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	gomock.InOrder(
		target.EXPECT().AddCheckpointTs(uint64(100)).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(149)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
	}

	w.Advance(Progress{Watermark: 100})
	w.advanceCheckpoint()
	// Calling again without progress must not re-emit the same checkpoint.
	w.advanceCheckpoint()

	w.Advance(Progress{Watermark: 200, Pending: 150})
	w.advanceCheckpoint()

	require.Equal(t, uint64(149), w.Checkpoint())
}

func TestWriterReplayDedupAcrossCheckpointAndOutOfOrder(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	dml200 := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml200.CommitTs = 200
	helper.ExecuteDeleteDml("test", "t", "delete from test.t where id = 1")
	dml200Replay := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml200Replay.CommitTs = 200
	dml150 := helper.DML2Event("test", "t", "insert into test.t values (2, 20)")
	dml150.CommitTs = 150
	dml100Replay := helper.DML2Event("test", "t", "insert into test.t values (3, 30)")
	dml100Replay.CommitTs = 100

	var replayed200Flushed, replayed100Flushed bool
	dml200Replay.AddPostFlushFunc(func() { replayed200Flushed = true })
	dml100Replay.AddPostFlushFunc(func() { replayed100Flushed = true })

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	var written []uint64
	gomock.InOrder(
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			written = append(written, dml.CommitTs)
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(100)).Times(1),
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			written = append(written, dml.CommitTs)
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(200)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	w.Advance(Progress{Watermark: 100})
	w.AddDML(dml200)
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(100), w.Checkpoint())

	// An earlier commit above checkpoint (> 100, < 200) must still be written.
	w.AddDML(dml150)
	require.NoError(t, w.Flush(t.Context()))

	// Same-handle replay in (checkpoint, ...] is dropped by mutations; <= checkpoint is dropped directly.
	w.AddDML(dml200Replay)
	w.AddDML(dml100Replay)
	require.NoError(t, w.Flush(t.Context()))
	require.True(t, replayed200Flushed)
	require.True(t, replayed100Flushed)
	require.Equal(t, []uint64{200, 150}, written)

	w.Advance(Progress{Watermark: 200})
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(200), w.Checkpoint())
	require.Empty(t, w.mutations)
}

func TestWriterFlushOrdersDMLBeforeDDLAndRetainsDDLWrites(t *testing.T) {
	helper := event.NewEventTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event("create table test.t(id int primary key, v int)")
	dml100 := helper.DML2Event("test", "t", "insert into test.t values (1, 10)")
	dml100.CommitTs = 100
	ddlEvent := helper.DDL2Event("alter table test.t add column c int")
	ddlEvent.FinishedTs = 150

	ctrl := gomock.NewController(t)
	target := sinkmock.NewMockSink(ctrl)
	gomock.InOrder(
		target.EXPECT().AddDMLEvent(gomock.Any()).Do(func(dml *event.DMLEvent) {
			dml.PostFlush()
		}).Times(1),
		target.EXPECT().FlushDMLBeforeBlock(ddlEvent).Return(nil).Times(1),
		target.EXPECT().WriteBlockEvent(ddlEvent).Return(nil).Times(1),
		target.EXPECT().AddCheckpointTs(uint64(200)).Times(1),
	)

	w := &Writer{
		target:    target,
		mutations: make(map[mutationKey]bool),
		ddlWrites: make(map[ddl.Key]bool),
	}

	w.AddDML(dml100)
	w.AddDDL(ddlEvent)
	require.NoError(t, w.Flush(t.Context()))

	w.Advance(Progress{Watermark: 200})
	require.NoError(t, w.Flush(t.Context()))
	require.Equal(t, uint64(200), w.Checkpoint())
	require.True(t, w.ddlWrites[ddl.KeyOf(ddlEvent)])

	// Historical DDL replay after checkpoint advancement is still deduplicated.
	w.AddDDL(ddlEvent)
	require.NoError(t, w.Flush(t.Context()))
}
