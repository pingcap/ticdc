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

// Add validates and records a source-to-target table name mapping.
func (r *TargetTableRegistry) Add(binding RouteBinding) error {
	if target, ok := r.source2Target[binding.Source]; ok {
		if target.Equal(binding.Target) {
			return nil
		}
		return errors.ErrInternalCheckFailed.GenWithStack(
			"source `%s`.`%s` is already registered to target `%s`.`%s`, incoming target `%s`.`%s`",
			binding.Source.Schema, binding.Source.Table,
			target.Schema, target.Table,
			binding.Target.Schema, binding.Target.Table)
	}

	existingSource, ok := r.target2Source[binding.Target]
	if !ok {
		r.target2Source[binding.Target] = binding.Source
		r.source2Target[binding.Source] = binding.Target
		return nil
	}
	if existingSource.Equal(binding.Source) {
		return nil
	}
	log.Warn("table route conflict detected",
		zap.String("keyspace", r.changefeedID.Keyspace()),
		zap.String("changefeed", r.changefeedID.Name()),
		zap.String("targetSchema", binding.Target.Schema),
		zap.String("targetTable", binding.Target.Table),
		zap.String("existingSourceSchema", existingSource.Schema),
		zap.String("existingSourceTable", existingSource.Table),
		zap.String("incomingSourceSchema", binding.Source.Schema),
		zap.String("incomingSourceTable", binding.Source.Table))
	return errors.ErrTableRouteConflict.GenWithStackByArgs(
		binding.Target.Schema, binding.Target.Table,
		existingSource.Schema, existingSource.Table,
		binding.Source.Schema, binding.Source.Table)
}

// Remove releases a source table name from the registry. It is idempotent.
func (r *TargetTableRegistry) Remove(source TableKey) {
	target, ok := r.source2Target[source]
	if !ok {
		return
	}
	delete(r.source2Target, source)
	delete(r.target2Source, target)
}

// ApplyTransition atomically applies source removals and source-to-target additions.
//
// The caller must describe one ordered table-info transition: every source name that
// stops being replicated is listed in removes, and every source name that becomes
// replicated is listed in adds. When a source name changes, the old source must be
// removed before the new binding can claim its target.
//
// This method validates the whole transition before mutating the registry. If any
// check fails, both indexes remain unchanged. If mutate is false, this method
// only validates the transition and leaves both indexes unchanged.
func (r *TargetTableRegistry) ApplyTransition(removes []TableKey, adds []RouteBinding, mutate bool) error {
	removeSet := make(map[TableKey]struct{}, len(removes))
	for _, source := range removes {
		removeSet[source] = struct{}{}
	}

	addedTargets := make(map[TableKey]TableKey, len(adds))
	addedSources := make(map[TableKey]TableKey, len(adds))
	for _, add := range adds {
		// A source already in the registry can only be re-added with a different
		// target when the same transition also removes the old source binding.
		// Otherwise the caller is trying to retarget an existing source without
		// describing the corresponding table-info removal.
		if target, ok := r.source2Target[add.Source]; ok {
			if _, removed := removeSet[add.Source]; !removed && !target.Equal(add.Target) {
				return errors.ErrInternalCheckFailed.GenWithStack(
					"source `%s`.`%s` is already registered to target `%s`.`%s`, incoming target `%s`.`%s`",
					add.Source.Schema, add.Source.Table,
					target.Schema, target.Table,
					add.Target.Schema, add.Target.Table)
			}
		}

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

		// The adds list itself must be internally consistent before the registry
		// is touched. One source cannot point to two targets in one transition.
		if existingTarget, ok := addedSources[add.Source]; ok && !existingTarget.Equal(add.Target) {
			return errors.ErrInternalCheckFailed.GenWithStack(
				"source `%s`.`%s` is added to multiple targets `%s`.`%s` and `%s`.`%s` in one transition",
				add.Source.Schema, add.Source.Table,
				existingTarget.Schema, existingTarget.Table,
				add.Target.Schema, add.Target.Table)
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
		addedSources[add.Source] = add.Target
		addedTargets[add.Target] = add.Source
	}

	if !mutate {
		return nil
	}

	// All validation above is side-effect free. Only after the complete transition
	// is known to be valid do we update both indexes.
	for _, source := range removes {
		r.Remove(source)
	}
	for _, add := range adds {
		r.target2Source[add.Target] = add.Source
		r.source2Target[add.Source] = add.Target
	}
	return nil
}
