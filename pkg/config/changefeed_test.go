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

package config

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestChangeFeedInfoToChangefeedConfigBatchFields(t *testing.T) {
	assertBatchFields := func(batchCount *int, batchBytes *int) {
		replicaConfig := GetDefaultReplicaConfig()
		replicaConfig.EventCollectorBatchCount = batchCount
		replicaConfig.EventCollectorBatchBytes = batchBytes

		info := &ChangeFeedInfo{
			ChangefeedID: common.NewChangefeedID4Test("test", "test"),
			Config:       replicaConfig,
		}

		changefeedConfig := info.ToChangefeedConfig()
		require.Equal(t, batchCount, changefeedConfig.EventCollectorBatchCount)
		require.Equal(t, batchBytes, changefeedConfig.EventCollectorBatchBytes)
	}

	assertBatchFields(nil, nil)
	assertBatchFields(util.AddressOf(0), util.AddressOf(0))
	assertBatchFields(util.AddressOf(123), util.AddressOf(456))
}
