// Copyright 2025 PingCAP, Inc.
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

package dynstream

import (
	"sync"
	"sync/atomic"
)

type areaBatchPolicyStore[A Area] struct {
	defaultPolicy batchPolicy

	// The policies are stored as an immutable map in atomic.Value, so reads in the hot path
	// don't need locks. Updates are copy-on-write and guarded by mu.
	policies atomic.Value // map[A]batchPolicy

	mu           sync.Mutex
	areaRefCount map[A]int
}

func newAreaBatchPolicyStore[A Area](defaultPolicy batchPolicy) *areaBatchPolicyStore[A] {
	s := &areaBatchPolicyStore[A]{
		defaultPolicy: defaultPolicy,
		areaRefCount:  make(map[A]int),
	}
	s.policies.Store(make(map[A]batchPolicy))
	return s
}

func (s *areaBatchPolicyStore[A]) getPolicy(area A) batchPolicy {
	policies := s.policies.Load().(map[A]batchPolicy)
	if policy, ok := policies[area]; ok {
		return policy
	}
	return s.defaultPolicy
}

func (s *areaBatchPolicyStore[A]) onAddPath(area A) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.areaRefCount[area]++
}

func (s *areaBatchPolicyStore[A]) onRemovePath(area A) {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldCount := s.areaRefCount[area]
	if oldCount <= 1 {
		delete(s.areaRefCount, area)
		s.removeOverrideLocked(area)
		return
	}
	s.areaRefCount[area] = oldCount - 1
}

func (s *areaBatchPolicyStore[A]) setAreaSettings(area A, settings AreaSettings) {
	if settings.batchCount <= 0 && settings.batchBytes <= 0 {
		// Do not touch existing batching override when caller only wants to update memory control settings.
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.areaRefCount[area] == 0 {
		// Keep the same semantics as memory control: avoid leaking per-area overrides for areas
		// without any existing paths.
		return
	}

	policies := s.policies.Load().(map[A]batchPolicy)
	currentPolicy, hasExistingOverride := policies[area]
	if !hasExistingOverride {
		currentPolicy = s.defaultPolicy
	}

	newPolicy := currentPolicy
	if settings.batchCount > 0 {
		newPolicy.batchCount = max(1, settings.batchCount)
	}
	if settings.batchBytes > 0 {
		newPolicy.batchBytes = settings.batchBytes
	}

	if newPolicy == s.defaultPolicy {
		s.removeOverrideLocked(area)
		return
	}
	if hasExistingOverride && currentPolicy == newPolicy {
		return
	}
	s.setOverrideLocked(area, newPolicy)
}

func (s *areaBatchPolicyStore[A]) setOverrideLocked(area A, policy batchPolicy) {
	oldPolicies := s.policies.Load().(map[A]batchPolicy)
	newPolicies := make(map[A]batchPolicy, len(oldPolicies)+1)
	for k, v := range oldPolicies {
		newPolicies[k] = v
	}
	newPolicies[area] = policy
	s.policies.Store(newPolicies)
}

func (s *areaBatchPolicyStore[A]) removeOverrideLocked(area A) {
	oldPolicies := s.policies.Load().(map[A]batchPolicy)
	if _, ok := oldPolicies[area]; !ok {
		return
	}
	newPolicies := make(map[A]batchPolicy, len(oldPolicies)-1)
	for k, v := range oldPolicies {
		if k == area {
			continue
		}
		newPolicies[k] = v
	}
	s.policies.Store(newPolicies)
}
