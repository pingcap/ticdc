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

package logpuller

import (
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type regionEvent struct {
	// `states` is always non-empty.
	// Entry events: `states` has exactly one element and `entries` is set.
	// Region-error/stale notifications: `states` has exactly one element.
	// Resolved-ts events: `resolvedTs` is set and `states` contains all related regions.
	states []*regionFeedState

	entries    *cdcpb.Event_Entries_
	resolvedTs uint64
}

func (event regionEvent) mustFirstState() *regionFeedState {
	if len(event.states) == 0 || event.states[0] == nil {
		log.Panic("region event has empty states", zap.Any("event", event))
	}
	return event.states[0]
}
