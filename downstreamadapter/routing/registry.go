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
	"sort"

	"github.com/pingcap/ticdc/pkg/errors"
)

// TargetKey identifies a downstream target table.
type TargetKey struct {
	Schema string
	Table  string
}

// SourceKey identifies an upstream logical table.
type SourceKey struct {
	LogicalTableID int64
	Schema         string
	Table          string
}

// RouteBinding records an upstream-to-downstream mapping for a single replica.
type RouteBinding struct {
	Source         SourceKey
	ReplicaTableID int64
	SourceSchemaID int64
	Target         TargetKey

	RuleIndex int
	Matcher   []string
}

// TargetTableRegistry tracks which upstream logical table owns each downstream
// target. It guarantees that at most one SourceKey maps to each TargetKey.
//
// Multiple replicas of the same logical source may share a target (e.g.
// partitioned tables or split spans). Different logical sources mapping to
// the same target is a conflict.
type TargetTableRegistry struct {
	// ownerSources tracks which SourceKey currently owns each target.
	// This is the constraint table for conflict detection.
	ownerSources map[TargetKey]SourceKey

	// bindings stores the full RouteBinding for each replica table ID.
	bindings map[int64]RouteBinding

	// bySourceID tracks all replica table IDs for a logical source.
	bySourceID map[int64]map[int64]struct{} // logicalTableID -> replicaTableIDs
	// bySchemaID tracks all replica table IDs for a source schema.
	bySchemaID map[int64]map[int64]struct{} // schemaID -> replicaTableIDs
}

// NewTargetTableRegistry creates a registry from a set of initial bindings.
func NewTargetTableRegistry(bindings []RouteBinding) (*TargetTableRegistry, error) {
	r := &TargetTableRegistry{
		ownerSources: make(map[TargetKey]SourceKey),
		bindings:     make(map[int64]RouteBinding, len(bindings)),
		bySourceID:   make(map[int64]map[int64]struct{}),
		bySchemaID:   make(map[int64]map[int64]struct{}),
	}
	for _, b := range bindings {
		if err := r.Upsert(b); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Snapshot returns a copy of all current bindings, sorted by target for
// deterministic output.
func (r *TargetTableRegistry) Snapshot() []RouteBinding {
	result := make([]RouteBinding, 0, len(r.bindings))
	for _, b := range r.bindings {
		result = append(result, b)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Target.Schema != result[j].Target.Schema {
			return result[i].Target.Schema < result[j].Target.Schema
		}
		if result[i].Target.Table != result[j].Target.Table {
			return result[i].Target.Table < result[j].Target.Table
		}
		return result[i].ReplicaTableID < result[j].ReplicaTableID
	})
	return result
}

// ValidateAdd checks whether binding can be added without conflict.
// Different logical sources mapping to the same target is a conflict.
func (r *TargetTableRegistry) ValidateAdd(binding RouteBinding) error {
	existingSource, ok := r.ownerSources[binding.Target]
	if !ok {
		return nil
	}
	if existingSource.LogicalTableID == binding.Source.LogicalTableID {
		return nil
	}
	// Find the existing binding for the error message.
	existingBinding := r.findBindingBySource(existingSource.LogicalTableID)
	return newConflictError(existingBinding, binding)
}

// ValidateReplace simulates removing replicaTableIDs then adding bindings.
// It does not mutate the registry.
func (r *TargetTableRegistry) ValidateReplace(removes []int64, adds []RouteBinding) error {
	tmp := r.clone()
	for _, id := range removes {
		tmp.RemoveByReplicaID(id)
	}
	for _, b := range adds {
		if err := tmp.ValidateAdd(b); err != nil {
			return err
		}
		tmp.unsafeInsert(b)
	}
	return nil
}

// Upsert inserts or replaces a binding. It validates for conflicts first.
// If the same ReplicaTableID already exists, its old binding is replaced.
func (r *TargetTableRegistry) Upsert(binding RouteBinding) error {
	existingSource, ok := r.ownerSources[binding.Target]
	if ok && existingSource.LogicalTableID != binding.Source.LogicalTableID {
		existingBinding := r.findBindingBySource(existingSource.LogicalTableID)
		return newConflictError(existingBinding, binding)
	}
	// Remove old binding for this replica if it exists.
	if oldBinding, exists := r.bindings[binding.ReplicaTableID]; exists {
		r.removeFromSourceIndex(binding.ReplicaTableID)
		r.removeFromSchemaIndex(binding.ReplicaTableID, oldBinding.SourceSchemaID)
		// If this was the last replica for this target, remove the owner.
		if r.countReplicasForTarget(binding.ReplicaTableID, oldBinding.Target) <= 1 {
			delete(r.ownerSources, oldBinding.Target)
		}
	}
	r.unsafeInsert(binding)
	return nil
}

// RemoveByReplicaID removes the binding for a given replica table ID.
func (r *TargetTableRegistry) RemoveByReplicaID(replicaTableID int64) {
	binding, ok := r.bindings[replicaTableID]
	if !ok {
		return
	}
	delete(r.bindings, replicaTableID)
	r.removeFromSourceIndex(replicaTableID)
	r.removeFromSchemaIndex(replicaTableID, binding.SourceSchemaID)
	// Only remove the owner if no other replica (from the same or different source)
	// maps to this target.
	remaining := 0
	for _, b := range r.bindings {
		if b.Target == binding.Target {
			remaining++
		}
	}
	if remaining == 0 {
		delete(r.ownerSources, binding.Target)
	}
}

// RemoveBySchemaID removes all bindings belonging to the given schema ID.
func (r *TargetTableRegistry) RemoveBySchemaID(schemaID int64) {
	replicas, ok := r.bySchemaID[schemaID]
	if !ok {
		return
	}
	for replicaID := range replicas {
		binding, exists := r.bindings[replicaID]
		if exists {
			delete(r.bindings, replicaID)
			r.removeFromSourceIndex(replicaID)
			// Check if the target still has other replicas.
			remaining := 0
			for _, b := range r.bindings {
				if b.Target == binding.Target {
					remaining++
				}
			}
			if remaining == 0 {
				delete(r.ownerSources, binding.Target)
			}
		}
	}
	delete(r.bySchemaID, schemaID)
}

// Replace atomically removes the given replica table IDs and adds the given
// bindings. It is all-or-nothing: on any conflict the registry is unchanged.
func (r *TargetTableRegistry) Replace(removes []int64, adds []RouteBinding) error {
	if err := r.ValidateReplace(removes, adds); err != nil {
		return err
	}
	for _, id := range removes {
		r.RemoveByReplicaID(id)
	}
	for _, b := range adds {
		_ = r.Upsert(b) // validated above, cannot fail
	}
	return nil
}

func (r *TargetTableRegistry) unsafeInsert(binding RouteBinding) {
	r.bindings[binding.ReplicaTableID] = binding
	r.ownerSources[binding.Target] = binding.Source
	if _, ok := r.bySourceID[binding.Source.LogicalTableID]; !ok {
		r.bySourceID[binding.Source.LogicalTableID] = make(map[int64]struct{})
	}
	r.bySourceID[binding.Source.LogicalTableID][binding.ReplicaTableID] = struct{}{}
	if _, ok := r.bySchemaID[binding.SourceSchemaID]; !ok {
		r.bySchemaID[binding.SourceSchemaID] = make(map[int64]struct{})
	}
	r.bySchemaID[binding.SourceSchemaID][binding.ReplicaTableID] = struct{}{}
}

func (r *TargetTableRegistry) removeFromSourceIndex(replicaTableID int64) {
	for logicalID, replicas := range r.bySourceID {
		if _, ok := replicas[replicaTableID]; ok {
			delete(replicas, replicaTableID)
			if len(replicas) == 0 {
				delete(r.bySourceID, logicalID)
			}
			return
		}
	}
}

func (r *TargetTableRegistry) removeFromSchemaIndex(replicaTableID, schemaID int64) {
	replicas, ok := r.bySchemaID[schemaID]
	if !ok {
		return
	}
	delete(replicas, replicaTableID)
	if len(replicas) == 0 {
		delete(r.bySchemaID, schemaID)
	}
}

func (r *TargetTableRegistry) countReplicasForTarget(excludeReplica int64, target TargetKey) int {
	count := 0
	for id, b := range r.bindings {
		if id != excludeReplica && b.Target == target {
			count++
		}
	}
	// Add 1 for the binding we haven't inserted yet (the one being upserted).
	return count
}

func (r *TargetTableRegistry) findBindingBySource(logicalTableID int64) RouteBinding {
	for _, b := range r.bindings {
		if b.Source.LogicalTableID == logicalTableID {
			return b
		}
	}
	return RouteBinding{}
}

func (r *TargetTableRegistry) clone() *TargetTableRegistry {
	return &TargetTableRegistry{
		ownerSources: r.cloneOwnerSources(),
		bindings:     r.cloneBindings(),
		bySourceID:   r.cloneSourceIndex(),
		bySchemaID:   r.cloneSchemaIndex(),
	}
}

func (r *TargetTableRegistry) cloneOwnerSources() map[TargetKey]SourceKey {
	m := make(map[TargetKey]SourceKey, len(r.ownerSources))
	for k, v := range r.ownerSources {
		m[k] = v
	}
	return m
}

func (r *TargetTableRegistry) cloneBindings() map[int64]RouteBinding {
	m := make(map[int64]RouteBinding, len(r.bindings))
	for k, v := range r.bindings {
		m[k] = v
	}
	return m
}

func (r *TargetTableRegistry) cloneSourceIndex() map[int64]map[int64]struct{} {
	m := make(map[int64]map[int64]struct{}, len(r.bySourceID))
	for k, v := range r.bySourceID {
		inner := make(map[int64]struct{}, len(v))
		for id := range v {
			inner[id] = struct{}{}
		}
		m[k] = inner
	}
	return m
}

func (r *TargetTableRegistry) cloneSchemaIndex() map[int64]map[int64]struct{} {
	m := make(map[int64]map[int64]struct{}, len(r.bySchemaID))
	for k, v := range r.bySchemaID {
		inner := make(map[int64]struct{}, len(v))
		for id := range v {
			inner[id] = struct{}{}
		}
		m[k] = inner
	}
	return m
}

// TableRouteConflictArgs carries structured conflict details.
type TableRouteConflictArgs struct {
	Target   TargetKey
	Existing RouteBinding
	Incoming RouteBinding
}

// Error implements the error interface with a human-readable conflict message.
func (a *TableRouteConflictArgs) Error() string {
	return fmt.Sprintf(
		"table route conflict: "+
			"target `%s`.`%s` is mapped by both "+
			"source `%s`.`%s` tableID=%d rule=%d matcher=%s "+
			"and source `%s`.`%s` tableID=%d rule=%d matcher=%s",
		a.Target.Schema, a.Target.Table,
		a.Existing.Source.Schema, a.Existing.Source.Table,
		a.Existing.Source.LogicalTableID, a.Existing.RuleIndex,
		formatMatcher(a.Existing.Matcher),
		a.Incoming.Source.Schema, a.Incoming.Source.Table,
		a.Incoming.Source.LogicalTableID, a.Incoming.RuleIndex,
		formatMatcher(a.Incoming.Matcher),
	)
}

func newConflictError(existing, incoming RouteBinding) error {
	return errors.ErrTableRouteConflict.GenWithStackByArgs(
		&TableRouteConflictArgs{
			Target:   existing.Target,
			Existing: existing,
			Incoming: incoming,
		},
	)
}

func formatMatcher(matcher []string) string {
	if len(matcher) == 0 {
		return "[]"
	}
	return fmt.Sprintf("%v", matcher)
}
