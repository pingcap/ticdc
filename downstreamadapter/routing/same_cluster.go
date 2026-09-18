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

package routing

import (
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
)

// ValidateSameClusterRouting verifies that a changefeed which is allowed to replicate into the
// same TiDB cluster as its upstream cannot capture the writes of its own sink.
//
// Let S be the tables the changefeed replicates and route(t) the target name of t under the
// dispatch rules. The changefeed is safe only when route(S) ∩ S = ∅:
//   - a table without any route rule keeps its own name, so route(t) = t and it is rejected;
//   - a route target which the filter replicates as well is rejected, even when the target table
//     does not exist yet, because the filter decides which tables the changefeed replicates.
func ValidateSameClusterRouting(
	changefeedID common.ChangeFeedID,
	caseSensitive bool,
	rules []*config.DispatchRule,
	capturedTables []common.TableName,
	f filter.Filter,
) error {
	router, err := NewRouter(changefeedID, caseSensitive, rules)
	if err != nil {
		return err
	}
	if !router.enabled() {
		return errors.ErrInvalidReplicaConfig.FastGenByArgs("allow-same-cluster requires table route to be enabled")
	}

	for _, tableName := range capturedTables {
		binding, err := router.route(tableName.Schema, tableName.Table)
		if err != nil {
			return err
		}
		if !binding.routed() {
			return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster requires every replicated table to be routed to another table, but table %s.%s is not routed", tableName.Schema, tableName.Table)
		}
		if !f.ShouldIgnoreTable(binding.Target.Schema, binding.Target.Table) {
			return errors.ErrInvalidReplicaConfig.FastGen("allow-same-cluster requires route targets to stay outside the changefeed, but table %s.%s is routed to %s.%s, which is replicated as well", tableName.Schema, tableName.Table, binding.Target.Schema, binding.Target.Table)
		}
	}
	return nil
}
