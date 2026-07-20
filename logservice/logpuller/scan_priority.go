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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logpuller

import (
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/tikv/client-go/v2/oracle"
)

// scanPriorityPolicy owns the priority rules and sticky state for one subscribed span.
type scanPriorityPolicy struct {
	pdClock      pdutil.Clock
	lagThreshold time.Duration
	everCaughtUp atomic.Bool
}

func newScanPriorityPolicy(pdClock pdutil.Clock, lagThreshold time.Duration) scanPriorityPolicy {
	return scanPriorityPolicy{
		pdClock:      pdClock,
		lagThreshold: lagThreshold,
	}
}

// observeSpanResolved records when the whole span first catches up. The state is
// sticky so later recovery scans remain protected even if the span falls behind.
func (p *scanPriorityPolicy) observeSpanResolved(resolvedTs uint64) bool {
	if p.everCaughtUp.Load() || !p.isTsClose(resolvedTs, p.pdClock.CurrentTime()) {
		return false
	}
	return p.everCaughtUp.CompareAndSwap(false, true)
}

// resolve returns the effective priority after combining inherited, span, and
// region progress. A high priority decision is never downgraded.
func (p *scanPriorityPolicy) resolve(
	inherited TaskType,
	regionResolvedTs uint64,
	currentTime time.Time,
) TaskType {
	if inherited == TaskHighPrior || p.everCaughtUp.Load() || p.isTsClose(regionResolvedTs, currentTime) {
		return TaskHighPrior
	}
	return TaskLowPrior
}

func (p *scanPriorityPolicy) isTsClose(ts uint64, currentTime time.Time) bool {
	if ts == 0 {
		return false
	}
	return currentTime.Sub(oracle.GetTimeFromTS(ts)) <= p.lagThreshold
}
