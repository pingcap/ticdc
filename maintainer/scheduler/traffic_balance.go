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

package scheduler

import (
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/operator"
)

// TrafficBalanceState limits traffic migrations across one maintainer's normal
// and redo schedulers. Both run in the schedule controller's checker goroutine.
type TrafficBalanceState struct {
	pending             *operator.Controller
	notBefore           time.Time
	observationInterval time.Duration
}

func NewTrafficBalanceState(checkInterval time.Duration) *TrafficBalanceState {
	return &TrafficBalanceState{
		observationInterval: max(checkInterval, 3*heartbeatpb.DispatcherTrafficSampleInterval),
	}
}

func (s *TrafficBalanceState) ready(now time.Time) bool {
	if s.pending != nil {
		if s.pending.OperatorSize() > 0 {
			return false
		}
		// Either mode may observe completion. Start the shared window only
		// after the owner finishes, so a slow move cannot consume its cooldown.
		// Three report windows also let node smoothing reflect the new placement.
		s.pending = nil
		s.notBefore = now.Add(s.observationInterval)
	}
	return !now.Before(s.notBefore)
}
