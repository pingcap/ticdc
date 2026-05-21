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
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

// TableKey identifies a table by schema and table name.
type TableKey = tableKey

// RouteBinding records one source-to-target route mapping.
type RouteBinding = routeBinding

type targetKey = tableKey

// TargetTableRegistry tracks which upstream source table name owns each downstream
// target table name. Different source names mapping to the same target is a conflict;
// registering the same source-target mapping repeatedly is idempotent.
type TargetTableRegistry struct {
	changefeedID   common.ChangeFeedID
	owners         map[targetKey]routeBinding
	sourceToTarget map[tableKey]targetKey
}

// NewTargetTableRegistry creates an empty registry. The optional capacity
// preallocates the internal indexes for large table sets.
func NewTargetTableRegistry(changefeedID common.ChangeFeedID, capacity ...int) *TargetTableRegistry {
	size := 0
	if len(capacity) > 0 {
		size = capacity[0]
	}
	return &TargetTableRegistry{
		changefeedID:   changefeedID,
		owners:         make(map[targetKey]routeBinding, size),
		sourceToTarget: make(map[tableKey]targetKey, size),
	}
}

// Add validates and records a source-to-target table name mapping.
func (r *TargetTableRegistry) Add(binding RouteBinding) error {
	if ownedTarget, ok := r.sourceToTarget[binding.Source]; ok {
		if ownedTarget.Equal(binding.Target) {
			return nil
		}
		return errors.ErrInternalCheckFailed.FastGenByArgs(fmt.Sprintf(
			"source `%s`.`%s` is already registered to target `%s`.`%s`, incoming target `%s`.`%s`",
			binding.Source.Schema, binding.Source.Table,
			ownedTarget.Schema, ownedTarget.Table,
			binding.Target.Schema, binding.Target.Table))
	}

	existing, ok := r.owners[binding.Target]
	if !ok {
		r.owners[binding.Target] = binding
		r.sourceToTarget[binding.Source] = binding.Target
		return nil
	}
	if existing.Source.Equal(binding.Source) {
		r.sourceToTarget[binding.Source] = binding.Target
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

// Remove releases a source table name from the registry. It is idempotent.
func (r *TargetTableRegistry) Remove(source TableKey) {
	target, ok := r.sourceToTarget[source]
	if !ok {
		return
	}
	delete(r.sourceToTarget, source)
	delete(r.owners, target)
}

// ApplyTransition atomically applies source removals and source-to-target additions.
// It validates the whole transition before mutating the registry, so a conflict leaves
// the registry unchanged.
func (r *TargetTableRegistry) ApplyTransition(removes []TableKey, adds []RouteBinding) error {
	removeSet := make(map[tableKey]struct{}, len(removes))
	for _, source := range removes {
		removeSet[source] = struct{}{}
	}

	addedTargets := make(map[targetKey]routeBinding, len(adds))
	addedSources := make(map[tableKey]routeBinding, len(adds))
	for _, add := range adds {
		if ownedTarget, ok := r.sourceToTarget[add.Source]; ok {
			if _, removed := removeSet[add.Source]; !removed && !ownedTarget.Equal(add.Target) {
				return errors.ErrInternalCheckFailed.FastGenByArgs(fmt.Sprintf(
					"source `%s`.`%s` is already registered to target `%s`.`%s`, incoming target `%s`.`%s`",
					add.Source.Schema, add.Source.Table,
					ownedTarget.Schema, ownedTarget.Table,
					add.Target.Schema, add.Target.Table))
			}
		}

		if existing, ok := r.owners[add.Target]; ok && !existing.Source.Equal(add.Source) {
			if _, removed := removeSet[existing.Source]; !removed {
				log.Warn("table route conflict detected",
					zap.String("keyspace", r.changefeedID.Keyspace()),
					zap.String("changefeed", r.changefeedID.Name()),
					zap.String("targetSchema", add.Target.Schema),
					zap.String("targetTable", add.Target.Table),
					zap.String("existingSourceSchema", existing.Source.Schema),
					zap.String("existingSourceTable", existing.Source.Table),
					zap.String("incomingSourceSchema", add.Source.Schema),
					zap.String("incomingSourceTable", add.Source.Table))
				return errors.ErrTableRouteConflict.FastGenByArgs(
					add.Target.Schema, add.Target.Table,
					existing.Source.Schema, existing.Source.Table,
					add.Source.Schema, add.Source.Table)
			}
		}

		if existing, ok := addedSources[add.Source]; ok && !existing.Target.Equal(add.Target) {
			return errors.ErrInternalCheckFailed.FastGenByArgs(fmt.Sprintf(
				"source `%s`.`%s` is added to multiple targets `%s`.`%s` and `%s`.`%s` in one transition",
				add.Source.Schema, add.Source.Table,
				existing.Target.Schema, existing.Target.Table,
				add.Target.Schema, add.Target.Table))
		}
		if existing, ok := addedTargets[add.Target]; ok && !existing.Source.Equal(add.Source) {
			log.Warn("table route conflict detected",
				zap.String("keyspace", r.changefeedID.Keyspace()),
				zap.String("changefeed", r.changefeedID.Name()),
				zap.String("targetSchema", add.Target.Schema),
				zap.String("targetTable", add.Target.Table),
				zap.String("existingSourceSchema", existing.Source.Schema),
				zap.String("existingSourceTable", existing.Source.Table),
				zap.String("incomingSourceSchema", add.Source.Schema),
				zap.String("incomingSourceTable", add.Source.Table))
			return errors.ErrTableRouteConflict.FastGenByArgs(
				add.Target.Schema, add.Target.Table,
				existing.Source.Schema, existing.Source.Table,
				add.Source.Schema, add.Source.Table)
		}
		addedSources[add.Source] = add
		addedTargets[add.Target] = add
	}

	for _, source := range removes {
		r.Remove(source)
	}
	for _, add := range adds {
		r.owners[add.Target] = add
		r.sourceToTarget[add.Source] = add.Target
	}
	return nil
}
