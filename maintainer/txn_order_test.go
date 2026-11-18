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

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/eventservice"
	"github.com/stretchr/testify/require"
)

func TestFinishBootstrapTxnOrderUsesSpanCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name       string
		checkpoint uint64
		pending    bool
		whole      bool
		wantError  bool
	}{
		{name: "span before rename", checkpoint: 19},
		{name: "span at rename", checkpoint: 20, wantError: true},
		{name: "pending create after rename", checkpoint: 21, pending: true, wantError: true},
		{name: "whole span after rename", checkpoint: 21, whole: true},
		{name: "span after leaving ordered set", checkpoint: 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newMergeBootstrapTestEnv(t)
			env.controller.replicaConfig.Sink.MySQLConfig = &config.MySQLConfig{TxnOrderTables: []string{"test.ordered"}}
			store := eventservice.NewMockSchemaStore()
			store.SetTables([]commonEvent.Table{{
				TableID: 1, SchemaID: 1, Splitable: true,
				SchemaTableName: &commonEvent.SchemaTableName{SchemaName: "test", TableName: "t1"},
			}})
			store.AppendDDLEvent(1,
				commonEvent.DDLEvent{FinishedTs: 10, TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "test", Table: "t1", TableID: 1}, UpdateTS: 10,
				}},
				commonEvent.DDLEvent{FinishedTs: 20, TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "test", Table: "ordered", TableID: 1}, UpdateTS: 20,
				}},
				commonEvent.DDLEvent{FinishedTs: 30, TableInfo: &common.TableInfo{
					TableName: common.TableName{Schema: "test", Table: "ordinary", TableID: 1}, UpdateTS: 30,
				}},
			)
			appcontext.SetService(appcontext.SchemaStore, store)

			responses := env.bootstrapResponses(nil)
			if tc.pending {
				responses[env.nodeID].Operators = []*heartbeatpb.ScheduleDispatcherRequest{{
					ScheduleAction: heartbeatpb.ScheduleAction_Create,
					Config: &heartbeatpb.DispatcherConfig{
						DispatcherID: env.sourceDispatcherID1.ToPB(), Span: env.sourceSpan1,
						SchemaID: 1, StartTs: tc.checkpoint,
					},
				}}
			} else {
				tableSpan := env.sourceSpan1
				if tc.whole {
					tableSpan = env.mergedSpan
				}
				span := env.bootstrapSpan(env.sourceDispatcherID1, tableSpan, heartbeatpb.ComponentState_Working)
				span.CheckpointTs = tc.checkpoint
				responses[env.nodeID].Spans = []*heartbeatpb.BootstrapTableSpan{span}
			}
			_, err := env.controller.FinishBootstrap(t.Context(), responses, true)
			if tc.wantError {
				require.ErrorContains(t, err, "does not support split spans")
				require.True(t, errors.ShouldFailChangefeed(err))
				require.False(t, env.controller.bootstrapped)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, env.controller.spanController.GetTaskByID(env.sourceDispatcherID1))
		})
	}
}
