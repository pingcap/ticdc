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

package replica

import (
	"testing"

	"github.com/pingcap/ticdc/maintainer/testutil"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestNewRegionCountRefresher(t *testing.T) {
	testutil.SetUpTestServices()
	changefeedID := common.NewChangefeedID4Test("keyspace", "test")

	refresher := NewRegionCountRefresher(changefeedID, 0)
	require.Equal(t, refresher.interval, config.DefaultRegionRefreshInterval)

	refresher = NewRegionCountRefresher(changefeedID, 2*config.DefaultRegionRefreshInterval)
	require.Equal(t, refresher.interval, 2*config.DefaultRegionRefreshInterval)
}
