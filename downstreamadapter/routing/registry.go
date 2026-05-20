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

	"github.com/pingcap/ticdc/pkg/common"
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

// RouteBinding records one source-to-target route mapping.
type RouteBinding struct {
	Source         SourceKey
	ReplicaTableID int64
	SourceSchemaID int64
	Target         TargetKey

	RuleIndex int
	Matcher   []string
}

// TargetTableRegistry tracks which upstream logical table owns each downstream
// target. Different logical sources mapping to the same target is a conflict;
// multiple replicas of the same logical source may share a target.
type TargetTableRegistry struct {
	changefeedID common.ChangeFeedID

	ownerSources map[TargetKey]SourceKey
	bindings     map[int64]RouteBinding
	bySourceID   map[int64]map[int64]struct{} // logicalTableID -> replicaTableIDs
}

// SetChangefeedID annotates conflict errors emitted by this registry.
func (r *TargetTableRegistry) SetChangefeedID(changefeedID common.ChangeFeedID) {
	r.changefeedID = changefeedID
}

// NewTargetTableRegistry creates a registry from initial bindings.
func NewTargetTableRegistry(bindings []RouteBinding) (*TargetTableRegistry, error) {
	r := &TargetTableRegistry{
		ownerSources: make(map[TargetKey]SourceKey),
		bindings:     make(map[int64]RouteBinding, len(bindings)),
		bySourceID:   make(map[int64]map[int64]struct{}),
	}
	for _, b := range bindings {
		if err := r.ValidateAdd(b); err != nil {
			return nil, err
		}
		r.unsafeInsert(b)
	}
	return r, nil
}

// Snapshot returns a deterministic copy of all current bindings.
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
func (r *TargetTableRegistry) ValidateAdd(binding RouteBinding) error {
	existingSource, ok := r.ownerSources[binding.Target]
	if !ok {
		return nil
	}
	if existingSource.LogicalTableID == binding.Source.LogicalTableID {
		return nil
	}
	existingBinding := r.findBindingBySource(existingSource.LogicalTableID)
	return r.newConflictError(existingBinding, binding)
}

func (r *TargetTableRegistry) unsafeInsert(binding RouteBinding) {
	r.bindings[binding.ReplicaTableID] = binding
	r.ownerSources[binding.Target] = binding.Source
	if _, ok := r.bySourceID[binding.Source.LogicalTableID]; !ok {
		r.bySourceID[binding.Source.LogicalTableID] = make(map[int64]struct{})
	}
	r.bySourceID[binding.Source.LogicalTableID][binding.ReplicaTableID] = struct{}{}
}

func (r *TargetTableRegistry) findBindingBySource(logicalTableID int64) RouteBinding {
	replicas, ok := r.bySourceID[logicalTableID]
	if !ok {
		return RouteBinding{}
	}
	for id := range replicas {
		if b, exists := r.bindings[id]; exists {
			return b
		}
	}
	return RouteBinding{}
}

// NewRouteBinding constructs a RouteBinding from a table info and route result.
func NewRouteBinding(
	tableInfo *common.TableInfo,
	replicaTableID, sourceSchemaID int64,
	result RouteResult,
) RouteBinding {
	return RouteBinding{
		Source: SourceKey{
			LogicalTableID: tableInfo.TableName.TableID,
			Schema:         tableInfo.GetSchemaName(),
			Table:          tableInfo.GetTableName(),
		},
		ReplicaTableID: replicaTableID,
		SourceSchemaID: sourceSchemaID,
		Target: TargetKey{
			Schema: result.TargetSchema,
			Table:  result.TargetTable,
		},
		RuleIndex: result.RuleIndex,
		Matcher:   result.Matcher,
	}
}

// TableRouteConflictError carries structured conflict details.
type TableRouteConflictError struct {
	Changefeed string
	Target     TargetKey
	Existing   RouteBinding
	Incoming   RouteBinding
}

// Error implements the error interface with a human-readable conflict message.
func (a *TableRouteConflictError) Error() string {
	changefeed := ""
	if a.Changefeed != "" {
		changefeed = fmt.Sprintf(" in changefeed %s", a.Changefeed)
	}
	return fmt.Sprintf(
		"table route conflict%s: "+
			"target `%s`.`%s` is mapped by both "+
			"source `%s`.`%s` tableID=%d rule=%d matcher=%s "+
			"and source `%s`.`%s` tableID=%d rule=%d matcher=%s",
		changefeed,
		a.Target.Schema, a.Target.Table,
		a.Existing.Source.Schema, a.Existing.Source.Table,
		a.Existing.Source.LogicalTableID, a.Existing.RuleIndex,
		formatMatcher(a.Existing.Matcher),
		a.Incoming.Source.Schema, a.Incoming.Source.Table,
		a.Incoming.Source.LogicalTableID, a.Incoming.RuleIndex,
		formatMatcher(a.Incoming.Matcher),
	)
}

func (r *TargetTableRegistry) newConflictError(existing, incoming RouteBinding) error {
	return errors.ErrTableRouteConflict.GenWithStackByArgs(
		&TableRouteConflictError{
			Changefeed: r.changefeedID.Name(),
			Target:     existing.Target,
			Existing:   existing,
			Incoming:   incoming,
		},
	)
}

func formatMatcher(matcher []string) string {
	if len(matcher) == 0 {
		return "[]"
	}
	return fmt.Sprintf("%v", matcher)
}
