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
// See the License for the specific language governing permissions and
// limitations under the License.

package maintainer

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestRebuildTargetTableRegistry(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-changefeed", common.DefaultKeyspaceName)

	t.Run("disabled without dispatch rules", func(t *testing.T) {
		t.Parallel()

		controller := &Controller{
			changefeedID:  changefeedID,
			replicaConfig: &config.ReplicaConfig{},
		}

		require.NoError(t, controller.rebuildTargetTableRegistry([]commonEvent.Table{
			newRegistryTestTable(1, "db1", "orders"),
		}))
		require.Nil(t, controller.targetTableRegistry)
	})

	t.Run("builds current target owners", func(t *testing.T) {
		t.Parallel()

		controller := &Controller{
			changefeedID: changefeedID,
			replicaConfig: &config.ReplicaConfig{
				CaseSensitive: util.AddressOf(false),
				Sink: &config.SinkConfig{
					DispatchRules: []*config.DispatchRule{
						{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
					},
				},
			},
		}

		require.NoError(t, controller.rebuildTargetTableRegistry([]commonEvent.Table{
			newRegistryTestTable(1, "db1", "orders"),
			newRegistryTestTable(2, "db2", "customers"),
		}))
		require.NotNil(t, controller.targetTableRegistry)
	})

	t.Run("fails on route conflict", func(t *testing.T) {
		t.Parallel()

		controller := &Controller{
			changefeedID: changefeedID,
			replicaConfig: &config.ReplicaConfig{
				CaseSensitive: util.AddressOf(false),
				Sink: &config.SinkConfig{
					DispatchRules: []*config.DispatchRule{
						{Matcher: []string{"db1.*"}, TargetSchema: "archive", TargetTable: "{table}"},
						{Matcher: []string{"db2.*"}, TargetSchema: "archive", TargetTable: "{table}"},
					},
				},
			},
		}

		err := controller.rebuildTargetTableRegistry([]commonEvent.Table{
			newRegistryTestTable(1, "db1", "orders"),
			newRegistryTestTable(2, "db2", "orders"),
		})
		require.Error(t, err)
		require.True(t, errors.ErrTableRouteConflict.Equal(err))
	})
}

func newRegistryTestTable(tableID int64, schema, table string) commonEvent.Table {
	return commonEvent.Table{
		TableID: tableID,
		SchemaTableName: &commonEvent.SchemaTableName{
			SchemaName: schema,
			TableName:  table,
		},
	}
}
