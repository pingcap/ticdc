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

package advancer

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/watcher"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
)

func TestNewTimeWindowAdvancer(t *testing.T) {
	t.Parallel()

	t.Run("create time window advancer", func(t *testing.T) {
		t.Parallel()
		checkpointWatchers := map[string]map[string]*watcher.CheckpointWatcher{
			"cluster1": {},
			"cluster2": {},
		}
		s3Watchers := map[string]*watcher.S3Watcher{
			"cluster1": nil,
			"cluster2": nil,
		}
		pdClients := map[string]pd.Client{
			"cluster1": nil,
			"cluster2": nil,
		}

		advancer, _, err := NewTimeWindowAdvancer(context.Background(), checkpointWatchers, s3Watchers, pdClients, nil)
		require.NoError(t, err)
		require.NotNil(t, advancer)
		require.Equal(t, uint64(0), advancer.round)
		require.Len(t, advancer.timeWindowTriplet, 2)
		require.Contains(t, advancer.timeWindowTriplet, "cluster1")
		require.Contains(t, advancer.timeWindowTriplet, "cluster2")
	})
}
