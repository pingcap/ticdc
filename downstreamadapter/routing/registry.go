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
	"github.com/pingcap/ticdc/pkg/errors"
)

// TargetTableRegistry tracks which upstream logical table owns each downstream
// target. Different logical sources mapping to the same target is a conflict;
// multiple replicas of the same logical source may share a target.
type TargetTableRegistry struct {
	memo map[targetKey]routeBinding
}

// NewTargetTableRegistry creates an empty registry.
func NewTargetTableRegistry() *TargetTableRegistry {
	return &TargetTableRegistry{
		memo: make(map[targetKey]routeBinding),
	}
}

// Add validates and records a source-to-target binding.
func (r *TargetTableRegistry) Add(binding routeBinding) error {
	existing, ok := r.memo[binding.Target]
	if !ok {
		r.memo[binding.Target] = binding
		return nil
	}

	if existing.Source.LogicalTableID == binding.Source.LogicalTableID {
		return nil
	}
	return errors.ErrTableRouteConflict.FastGenByArgs(
		binding.Target.Schema, binding.Target.Table,
		existing.Source.Schema, existing.Source.Table,
		binding.Source.Schema, binding.Source.Table)
}
