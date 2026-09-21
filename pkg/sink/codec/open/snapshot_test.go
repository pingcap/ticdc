// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package open

import (
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestSnapshotEnvelopeKeepsIdentityAndInlineSchema(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job("create table test.snap (id int primary key, v int)")
	event := helper.DML2Event("test", "snap", "insert into test.snap values(1,10)")
	row, ok := event.GetNextRow()
	require.True(t, ok)
	e := &commonEvent.RowEvent{PhysicalTableID: event.PhysicalTableID, TableInfo: event.TableInfo, Event: row, CommitTs: 42, ColumnSelector: columnselector.NewDefaultColumnSelector(), Snapshot: &commonEvent.SnapshotRow{ID: "stable", SnapshotID: "generation", Timestamp: 42, Schema: "c2NoZW1h"}}
	key, _, _, err := encodeRowChangedEvent(e, initColumnFlags(e.TableInfo), common.NewConfig(config.ProtocolOpen), false, "")
	require.NoError(t, err)
	require.Contains(t, string(key), `"snapshot_record_id":"stable"`)
	require.Contains(t, string(key), `"snapshot_schema":"c2NoZW1h"`)
	e.Snapshot = nil
	key, _, _, err = encodeRowChangedEvent(e, initColumnFlags(e.TableInfo), common.NewConfig(config.ProtocolOpen), false, "")
	require.NoError(t, err)
	require.NotContains(t, string(key), "snapshot_record_id")
}
