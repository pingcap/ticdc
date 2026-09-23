// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package helper

import (
	"testing"

	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
)

func TestSnapshotBatchPreservesPerRowIdentity(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job("create table test.snap_ids (id int primary key, v int)")
	ev := helper.DML2Event("test", "snap_ids", "insert into test.snap_ids values(1,10)", "insert into test.snap_ids values(2,20)")
	ev.Snapshot = &commonEvent.SnapshotRow{SnapshotID: "generation", Timestamp: 42}
	flushed := 0
	ev.AddPostFlushFunc(func() { flushed++ })
	rows := NewRowEvents(ev, nil, NewPostFlushRowCallback(ev, uint64(ev.Len())))
	require.Len(t, rows, 2)
	require.NotEqual(t, rows[0].Snapshot.ID, rows[1].Snapshot.ID)
	for i, row := range rows {
		require.Equal(t, protocol.RecordID("generation", ev.PhysicalTableID, ev.RowKeys[i]), row.Snapshot.ID)
		require.Equal(t, uint64(42), row.Snapshot.Timestamp)
	}
	rows[1].Callback()
	require.Zero(t, flushed)
	rows[0].Callback()
	require.Equal(t, 1, flushed)
	ev.Snapshot = nil
	require.Nil(t, snapshotRow(ev, rows[0].Event))
}
