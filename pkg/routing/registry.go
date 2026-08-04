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

// TargetTableRegistry tracks which upstream source table name owns each downstream
// target table name. Different source names mapping to the same target is a conflict;
// registering the same source-target mapping repeatedly is idempotent.
type TargetTableRegistry struct {
	changefeedID  common.ChangeFeedID
	target2Source map[TableKey]TableKey
	source2Target map[TableKey]TableKey
}

// NewTargetTableRegistry creates an empty registry and preallocates the internal
// indexes for the expected source table count.
func NewTargetTableRegistry(changefeedID common.ChangeFeedID, capacity int) *TargetTableRegistry {
	return &TargetTableRegistry{
		changefeedID:  changefeedID,
		target2Source: make(map[TableKey]TableKey, capacity),
		source2Target: make(map[TableKey]TableKey, capacity),
	}
}

// remove releases a source table name from the registry. It is idempotent.
func (r *TargetTableRegistry) remove(source TableKey) {
	target, ok := r.source2Target[source]
	if !ok {
		return
	}
	delete(r.source2Target, source)
	delete(r.target2Source, target)
}

// ApplyTransition validates and applies source removals and source-to-target
// additions for one ordered table-info transition.
//
// Validation is side-effect free: when mutate=false, only conflict checks run.
// When mutate=true, both indexes are updated only after the full transition
// passes validation, so partial failures never leave the registry in an
// inconsistent state.
func (r *TargetTableRegistry) ApplyTransition(removes []TableKey, adds []RouteBinding, mutate bool) error {
	removeSet := make(map[TableKey]struct{}, len(removes))
	for _, source := range removes {
		removeSet[source] = struct{}{}
	}

	addedTargets := make(map[TableKey]TableKey, len(adds))
	for _, add := range adds {
		// A target that is already owned by another source can only be claimed if
		// that old owner is removed in the same transition. This is what makes
		// rename/drop-and-create style replacements atomic while still rejecting
		// two live source names that route to the same target.
		if existingSource, ok := r.target2Source[add.Target]; ok && !existingSource.Equal(add.Source) {
			if _, removed := removeSet[existingSource]; !removed {
				log.Warn("table route conflict detected",
					zap.String("keyspace", r.changefeedID.Keyspace()),
					zap.String("changefeed", r.changefeedID.Name()),
					zap.String("targetSchema", add.Target.Schema),
					zap.String("targetTable", add.Target.Table),
					zap.String("existingSourceSchema", existingSource.Schema),
					zap.String("existingSourceTable", existingSource.Table),
					zap.String("incomingSourceSchema", add.Source.Schema),
					zap.String("incomingSourceTable", add.Source.Table))
				return errors.ErrTableRouteConflict.GenWithStackByArgs(
					add.Target.Schema, add.Target.Table,
					existingSource.Schema, existingSource.Table,
					add.Source.Schema, add.Source.Table)
			}
		}
		// Likewise, two newly added live sources cannot claim the same target.
		if existingSource, ok := addedTargets[add.Target]; ok && !existingSource.Equal(add.Source) {
			log.Warn("table route conflict detected",
				zap.String("keyspace", r.changefeedID.Keyspace()),
				zap.String("changefeed", r.changefeedID.Name()),
				zap.String("targetSchema", add.Target.Schema),
				zap.String("targetTable", add.Target.Table),
				zap.String("existingSourceSchema", existingSource.Schema),
				zap.String("existingSourceTable", existingSource.Table),
				zap.String("incomingSourceSchema", add.Source.Schema),
				zap.String("incomingSourceTable", add.Source.Table))
			return errors.ErrTableRouteConflict.GenWithStackByArgs(
				add.Target.Schema, add.Target.Table,
				existingSource.Schema, existingSource.Table,
				add.Source.Schema, add.Source.Table)
		}
		addedTargets[add.Target] = add.Source
	}

	if !mutate {
		return nil
	}

	// All validation above is side-effect free. Only after the complete transition
	// is known to be valid do we update both indexes.
	for _, source := range removes {
		r.remove(source)
	}
	for _, add := range adds {
		r.target2Source[add.Target] = add.Source
		r.source2Target[add.Source] = add.Target
	}
	return nil
}
