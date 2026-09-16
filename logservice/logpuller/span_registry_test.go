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
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/logpuller/regionlock"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/pdutil"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestSpanRegistryUpdateMetrics(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	registry := newSpanRegistry(nil, pdutil.NewClockWithValue4Test(now))

	span1 := &subscribedSpan{
		subID:     SubscriptionID(1),
		rangeLock: regionlock.NewRangeLock(1, []byte("a"), []byte("m"), 100),
		span:      heartbeatpb.TableSpan{TableID: 1, StartKey: []byte("a"), EndKey: []byte("m")},
	}
	span1.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-5 * time.Second)))

	span2 := &subscribedSpan{
		subID:     SubscriptionID(2),
		rangeLock: regionlock.NewRangeLock(2, []byte("m"), []byte("z"), 100),
		span:      heartbeatpb.TableSpan{TableID: 2, StartKey: []byte("m"), EndKey: []byte("z")},
	}
	span2.resolvedTs.Store(oracle.GoTimeToTS(now.Add(-2 * time.Second)))

	lockRange := func(t *testing.T, span *subscribedSpan, start, end string, regionID uint64) {
		t.Helper()
		res := span.rangeLock.LockRange(context.Background(), []byte(start), []byte(end), regionID, 100)
		require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	}

	lockRange(t, span1, "a", "g", 11)
	lockRange(t, span1, "g", "m", 12)
	lockRange(t, span2, "m", "z", 21)

	registry.Add(span1)
	registry.Add(span2)

	registry.UpdateMetrics()

	require.Equal(t, float64(3), testutil.ToFloat64(metrics.SubscriptionClientSubscribedRegionCount))
	require.InDelta(t, 5.0, testutil.ToFloat64(metrics.LogPullerResolvedTsLag), 0.001)
}
