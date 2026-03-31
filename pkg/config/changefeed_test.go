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
	replicaConfig := GetDefaultReplicaConfig()
	replicaConfig.EventCollectorBatchCount = util.AddressOf(123)
	replicaConfig.EventCollectorBatchBytes = util.AddressOf(456)

	info := &ChangeFeedInfo{
		ChangefeedID: common.NewChangefeedID4Test("test", "test"),
		Config:       replicaConfig,
	}

	changefeedConfig := info.ToChangefeedConfig()
	require.Equal(t, 123, changefeedConfig.EventCollectorBatchCount)
	require.Equal(t, 456, changefeedConfig.EventCollectorBatchBytes)
}
