// Copyright 2024 PingCAP, Inc.
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

package replica

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestUpdateStatus(t *testing.T) {
	t.Parallel()

	replicaSet := NewSpanReplication(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), common.NewDispatcherID(), 1, getTableSpanByID(4), 10, common.DefaultMode, false)
	replicaSet.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 9})
	require.Equal(t, uint64(10), replicaSet.status.Load().CheckpointTs)
	replicaSet.UpdateStatus(&heartbeatpb.TableSpanStatus{CheckpointTs: 11})
	require.Equal(t, uint64(11), replicaSet.status.Load().CheckpointTs)
}

func TestNewRemoveDispatcherMessage(t *testing.T) {
	t.Parallel()

	replicaSet := NewSpanReplication(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), common.NewDispatcherID(), 1, getTableSpanByID(4), 10, common.DefaultMode, false)
	msg := replicaSet.NewRemoveDispatcherMessage("node1", heartbeatpb.OperatorType_O_Remove)
	req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.Equal(t, heartbeatpb.ScheduleAction_Remove, req.ScheduleAction)
	require.Equal(t, replicaSet.ID.ToPB(), req.Config.DispatcherID)
	require.Equal(t, replicaSet.Span, req.Config.Span)
	require.Equal(t, "node1", msg.To.String())
}

func TestSpanReplication_NewAddDispatcherMessage(t *testing.T) {
	t.Parallel()

	replicaSet := NewSpanReplication(common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceNamme), common.NewDispatcherID(), 1, getTableSpanByID(4), 10, common.DefaultMode, false)

	msg := replicaSet.NewAddDispatcherMessage("node1", heartbeatpb.OperatorType_O_Add)
	require.Equal(t, "node1", msg.To.String())
	req := msg.Message[0].(*heartbeatpb.ScheduleDispatcherRequest)
	require.Equal(t, heartbeatpb.ScheduleAction_Create, req.ScheduleAction)
	require.Equal(t, replicaSet.ID.ToPB(), req.Config.DispatcherID)
	require.Equal(t, replicaSet.schemaID, req.Config.SchemaID)
}

// getTableSpanByID returns a mock TableSpan for testing
func getTableSpanByID(id common.TableID) *heartbeatpb.TableSpan {
	totalSpan := common.TableIDToComparableSpan(0, id)
	return &heartbeatpb.TableSpan{
		TableID:  totalSpan.TableID,
		StartKey: totalSpan.StartKey,
		EndKey:   totalSpan.EndKey,
	}
}
