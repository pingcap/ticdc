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
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type targetKey = tableKey

// TargetTableRegistry tracks which upstream source table name owns each downstream
// target table name. Different source names mapping to the same target is a conflict;
// registering the same source-target mapping repeatedly is idempotent.
type TargetTableRegistry struct {
	changefeedID common.ChangeFeedID
	memo         map[targetKey]routeBinding
}

// NewTargetTableRegistry creates an empty registry.
func NewTargetTableRegistry(changefeedID common.ChangeFeedID) *TargetTableRegistry {
	return &TargetTableRegistry{
		changefeedID: changefeedID,
		memo:         make(map[targetKey]routeBinding),
	}
}

// Add validates and records a source-to-target table name mapping.
func (r *TargetTableRegistry) Add(binding routeBinding) error {
	existing, ok := r.memo[binding.Target]
	if !ok {
		r.memo[binding.Target] = binding
		return nil
	}
	if existing.Source.Equal(binding.Source) {
		return nil
	}
	log.Warn("table route conflict detected",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("targetSchema", binding.Target.Schema),
		zap.String("targetTable", binding.Target.Table),
		zap.String("existingSourceSchema", existing.Source.Schema),
		zap.String("existingSourceTable", existing.Source.Table),
		zap.String("incomingSourceSchema", binding.Source.Schema),
		zap.String("incomingSourceTable", binding.Source.Table))
	return errors.ErrTableRouteConflict.FastGenByArgs(
		binding.Target.Schema, binding.Target.Table,
		existing.Source.Schema, existing.Source.Table,
		binding.Source.Schema, binding.Source.Table)
}
