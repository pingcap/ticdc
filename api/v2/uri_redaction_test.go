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

package v2

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCfInfoToAPIModelOmitsNestedStorageCredentials(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Consistent.Storage = new("s3://bucket/redo?ENDPOINT=" +
		"https%3A%2F%2Fuser%3Aredo-endpoint-secret-sentinel%40storage.example")
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		LargeMessageHandle: &config.LargeMessageHandleConfig{
			ClaimCheckStorageURI: "s3://bucket/claim?endpoint=" +
				"https%3A%2F%2Fuser%3Aclaim-endpoint-secret-sentinel%40storage.example",
		},
	}
	info := &config.ChangeFeedInfo{
		ChangefeedID: common.NewChangeFeedIDWithName("test", common.DefaultKeyspaceName),
		Config:       replicaConfig,
	}
	status := &config.ChangeFeedStatus{CheckpointTs: 123}
	original, err := info.Marshal()
	require.NoError(t, err)

	apiInfo := CfInfoToAPIModel(info, status, nil)
	response, err := apiInfo.Marshal()
	require.NoError(t, err)
	require.NotContains(t, response, "endpoint-secret-sentinel")
	require.Nil(t, apiInfo.Config.Consistent.Storage)
	require.Nil(t, apiInfo.Config.Sink.KafkaConfig.LargeMessageHandle.ClaimCheckStorageURI)
	after, err := info.Marshal()
	require.NoError(t, err)
	require.Equal(t, original, after)
}
