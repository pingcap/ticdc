// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
package v2

import (
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func queryValue(t *testing.T, rawURL, key string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	require.NoError(t, err)
	return parsed.Query().Get(key)
}

func TestChangeFeedInfoCloneWithMaskedSensitiveData(t *testing.T) {
	info := &ChangeFeedInfo{
		ID:      "test",
		SinkURI: "kafka://user:sink-password-sentinel@127.0.0.1:9092/topic?secret=uri-secret-sentinel",
		Config: &ReplicaConfig{
			Sink: &SinkConfig{
				SchemaRegistry: util.AddressOf("https://registry.example.com?access-key=registry-secret-sentinel"),
				KafkaConfig: &KafkaConfig{
					KafkaClientID:         util.AddressOf("visible-client-id"),
					SASLPassword:          util.AddressOf("plain-password-sentinel"),
					SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
					SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
					SASLOAuthTokenURL:     util.AddressOf("https://oauth.example.com/token?client_secret=token-url-secret-sentinel"),
					LargeMessageHandle:    &LargeMessageHandleConfig{ClaimCheckStorageURI: "s3://bucket/prefix?access-key=claim-check-secret-sentinel"},
					GlueSchemaRegistryConfig: &GlueSchemaRegistryConfig{
						AccessKey:       "glue-access-sentinel",
						SecretAccessKey: "glue-secret-sentinel",
						Token:           "glue-token-sentinel",
					},
				},
				PulsarConfig: &PulsarConfig{
					AuthenticationToken: util.AddressOf("pulsar-token-sentinel"),
					BasicPassword:       util.AddressOf("pulsar-password-sentinel"),
					OAuth2:              &PulsarOAuth2{OAuth2PrivateKey: "pulsar-private-key-sentinel"},
				},
			},
			Consistent: &ConsistentConfig{Storage: util.AddressOf("s3://bucket/prefix?access-key=consistent-secret-sentinel")},
		},
	}
	original, err := info.Marshal()
	require.NoError(t, err)

	masked, err := info.CloneWithMaskedSensitiveData()
	require.NoError(t, err)
	output, err := masked.Marshal()
	require.NoError(t, err)
	require.NotContains(t, output, "sentinel")
	require.NotContains(t, output, "memory_quota")
	require.Contains(t, output, "visible-client-id")
	require.Nil(t, masked.Config.Sink.KafkaConfig.Key)
	after, err := info.Marshal()
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestChangefeedConfigRestoreMaskedSensitiveData(t *testing.T) {
	original := &ReplicaConfig{
		Sink: &SinkConfig{
			SchemaRegistry: util.AddressOf("https://registry.example.com?access-key=registry-secret"),
			KafkaConfig: &KafkaConfig{
				SASLPassword:          util.AddressOf("plain-password"),
				SASLGssAPIPassword:    util.AddressOf("gssapi-password"),
				SASLOAuthClientSecret: util.AddressOf("oauth-secret"),
				SASLOAuthTokenURL:     util.AddressOf("https://oauth.example.com/token?client_secret=token-secret"),
				Key:                   util.AddressOf("private-key"),
				LargeMessageHandle: &LargeMessageHandleConfig{
					ClaimCheckStorageURI: "s3://bucket/prefix?access-key=claim-check-secret",
				},
				GlueSchemaRegistryConfig: &GlueSchemaRegistryConfig{
					AccessKey:       "glue-access",
					SecretAccessKey: "glue-secret",
					Token:           "glue-token",
				},
			},
			PulsarConfig: &PulsarConfig{
				AuthenticationToken: util.AddressOf("pulsar-token"),
				BasicPassword:       util.AddressOf("pulsar-password"),
				OAuth2:              &PulsarOAuth2{OAuth2PrivateKey: "pulsar-private-key"},
			},
		},
		Consistent: &ConsistentConfig{
			Storage: util.AddressOf("s3://bucket/prefix?access-key=consistent-secret"),
		},
	}
	oldInfo := &config.ChangeFeedInfo{
		SinkURI: "kafka://user:sink-password@127.0.0.1:9092/topic?secret=uri-secret",
		Config:  original.ToInternalReplicaConfig(),
	}
	update := &ChangefeedConfig{
		SinkURI:       util.MaskSensitiveDataInURI(oldInfo.SinkURI),
		ReplicaConfig: ToAPIReplicaConfig(oldInfo.Config),
	}
	update.ReplicaConfig.maskSensitiveData()

	update.restoreMaskedSensitiveData(oldInfo)

	require.Equal(t, oldInfo.SinkURI, update.SinkURI)
	require.Equal(t, "registry-secret", queryValue(t, *update.ReplicaConfig.Sink.SchemaRegistry, "access-key"))
	require.Equal(t, "plain-password", *update.ReplicaConfig.Sink.KafkaConfig.SASLPassword)
	require.Equal(t, "gssapi-password", *update.ReplicaConfig.Sink.KafkaConfig.SASLGssAPIPassword)
	require.Equal(t, "oauth-secret", *update.ReplicaConfig.Sink.KafkaConfig.SASLOAuthClientSecret)
	require.Equal(t, "token-secret", queryValue(t, *update.ReplicaConfig.Sink.KafkaConfig.SASLOAuthTokenURL, "client_secret"))
	require.Equal(t, "private-key", *update.ReplicaConfig.Sink.KafkaConfig.Key)
	require.Equal(t, "claim-check-secret", queryValue(t,
		update.ReplicaConfig.Sink.KafkaConfig.LargeMessageHandle.ClaimCheckStorageURI, "access-key"))
	require.Equal(t, "glue-access", update.ReplicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig.AccessKey)
	require.Equal(t, "glue-secret", update.ReplicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig.SecretAccessKey)
	require.Equal(t, "glue-token", update.ReplicaConfig.Sink.KafkaConfig.GlueSchemaRegistryConfig.Token)
	require.Equal(t, "pulsar-token", *update.ReplicaConfig.Sink.PulsarConfig.AuthenticationToken)
	require.Equal(t, "pulsar-password", *update.ReplicaConfig.Sink.PulsarConfig.BasicPassword)
	require.Equal(t, "pulsar-private-key", update.ReplicaConfig.Sink.PulsarConfig.OAuth2.OAuth2PrivateKey)
	require.Equal(t, "consistent-secret", queryValue(t, *update.ReplicaConfig.Consistent.Storage, "access-key"))

	update.SinkURI = "kafka://user:new-password@127.0.0.1:9092/topic"
	update.ReplicaConfig.Sink.KafkaConfig.SASLPassword = util.AddressOf("new-password")
	update.restoreMaskedSensitiveData(oldInfo)
	require.Contains(t, update.SinkURI, "new-password")
	require.Equal(t, "new-password", *update.ReplicaConfig.Sink.KafkaConfig.SASLPassword)
}

// TestReplicaConfigConversion verifies API/internal replica config conversion,
// including round-tripping the optional event collector batch overrides.
func TestReplicaConfigConversion(t *testing.T) {
	t.Parallel()

	// Test case 1: All fields are set
	apiCfg := &ReplicaConfig{
		PerformanceMode:       util.AddressOf(config.PerformanceModeLowLatency),
		MemoryQuota:           util.AddressOf(uint64(1024)),
		CaseSensitive:         util.AddressOf(true),
		ForceReplicate:        util.AddressOf(true),
		IgnoreIneligibleTable: util.AddressOf(true),
		CheckGCSafePoint:      util.AddressOf(true),
		EnableSyncPoint:       util.AddressOf(true),
		EnableTableMonitor:    util.AddressOf(true),
		BDRMode:               util.AddressOf(true),
		Sink: &SinkConfig{
			CloudStorageConfig: &CloudStorageConfig{
				UseTableIDAsPath: util.AddressOf(true),
				SpoolDiskQuota:   util.AddressOf(int64(1024)),
				SpoolBaseDir:     util.AddressOf("/tmp/ticdc-spool"),
			},
		},
		Mounter: &MounterConfig{
			WorkerNum: util.AddressOf(16),
		},
		Scheduler: &ChangefeedSchedulerConfig{
			EnableTableAcrossNodes: util.AddressOf(true),
			RegionThreshold:        util.AddressOf(1000),
		},
		Integrity: &IntegrityConfig{
			IntegrityCheckLevel:   util.AddressOf("correctness"),
			CorruptionHandleLevel: util.AddressOf("warn"),
		},
		Consistent: &ConsistentConfig{
			Level:             util.AddressOf("eventual"),
			MaxLogSize:        util.AddressOf(int64(128)),
			FlushIntervalInMs: util.AddressOf(int64(2000)),
			Storage:           util.AddressOf("s3://test"),
		},
	}

	internalCfg := apiCfg.ToInternalReplicaConfig()
	require.Equal(t, config.PerformanceModeLowLatency, util.GetOrZero(internalCfg.PerformanceMode))
	require.Equal(t, uint64(1024), util.GetOrZero(internalCfg.MemoryQuota))
	require.True(t, util.GetOrZero(internalCfg.CaseSensitive))
	require.True(t, util.GetOrZero(internalCfg.ForceReplicate))
	require.True(t, util.GetOrZero(internalCfg.IgnoreIneligibleTable))
	require.True(t, util.GetOrZero(internalCfg.CheckGCSafePoint))
	require.True(t, util.GetOrZero(internalCfg.EnableSyncPoint))
	require.True(t, util.GetOrZero(internalCfg.EnableTableMonitor))
	require.True(t, util.GetOrZero(internalCfg.BDRMode))
	require.True(t, util.GetOrZero(internalCfg.Sink.CloudStorageConfig.UseTableIDAsPath))
	require.Equal(t, int64(1024), util.GetOrZero(internalCfg.Sink.CloudStorageConfig.SpoolDiskQuota))
	require.Equal(t, "/tmp/ticdc-spool", util.GetOrZero(internalCfg.Sink.CloudStorageConfig.SpoolBaseDir))
	require.Equal(t, internalCfg.Mounter.WorkerNum, *apiCfg.Mounter.WorkerNum)
	require.True(t, util.GetOrZero(internalCfg.Scheduler.EnableTableAcrossNodes))
	require.Equal(t, 1000, util.GetOrZero(internalCfg.Scheduler.RegionThreshold))
	require.Equal(t, "correctness", util.GetOrZero(internalCfg.Integrity.IntegrityCheckLevel))
	require.Equal(t, "warn", util.GetOrZero(internalCfg.Integrity.CorruptionHandleLevel))
	require.Equal(t, "eventual", util.GetOrZero(internalCfg.Consistent.Level))
	require.Equal(t, int64(128), util.GetOrZero(internalCfg.Consistent.MaxLogSize))
	require.Equal(t, int64(2000), util.GetOrZero(internalCfg.Consistent.FlushIntervalInMs))
	require.Equal(t, "s3://test", util.GetOrZero(internalCfg.Consistent.Storage))

	// Test case 2: Nil fields (should use defaults or be nil)
	apiCfgNil := &ReplicaConfig{}
	internalCfgNil := apiCfgNil.ToInternalReplicaConfig()
	// Check defaults from GetDefaultReplicaConfig which ToInternalReplicaConfig uses as base
	defaultCfg := config.GetDefaultReplicaConfig()
	require.Equal(t, util.GetOrZero(defaultCfg.MemoryQuota), util.GetOrZero(internalCfgNil.MemoryQuota))
	require.Equal(t, util.GetOrZero(defaultCfg.CaseSensitive), util.GetOrZero(internalCfgNil.CaseSensitive))

	// Test case 3: Conversion back to API config
	apiCfgBack := ToAPIReplicaConfig(internalCfg)
	require.Equal(t, config.PerformanceModeLowLatency, util.GetOrZero(apiCfgBack.PerformanceMode))
	require.Equal(t, uint64(1024), *apiCfgBack.MemoryQuota)
	require.True(t, *apiCfgBack.CaseSensitive)
	require.True(t, *apiCfgBack.ForceReplicate)
	require.True(t, *apiCfgBack.IgnoreIneligibleTable)
	require.True(t, *apiCfgBack.Sink.CloudStorageConfig.UseTableIDAsPath)
	require.Equal(t, int64(1024), *apiCfgBack.Sink.CloudStorageConfig.SpoolDiskQuota)
	require.Equal(t, "/tmp/ticdc-spool", *apiCfgBack.Sink.CloudStorageConfig.SpoolBaseDir)
	require.Equal(t, 16, *apiCfgBack.Mounter.WorkerNum)
	require.True(t, *apiCfgBack.Scheduler.EnableTableAcrossNodes)
	require.Equal(t, "correctness", *apiCfgBack.Integrity.IntegrityCheckLevel)
	require.Equal(t, "eventual", *apiCfgBack.Consistent.Level)

	// Test case 4: batch fields round trip and nil preservation
	apiBatchCfg := &ReplicaConfig{
		EventCollectorBatchCount: util.AddressOf(4096),
		EventCollectorBatchBytes: util.AddressOf(2048),
	}
	internalBatchCfg := apiBatchCfg.ToInternalReplicaConfig()
	require.NotNil(t, internalBatchCfg.EventCollectorBatchCount)
	require.NotNil(t, internalBatchCfg.EventCollectorBatchBytes)
	require.Equal(t, 4096, *internalBatchCfg.EventCollectorBatchCount)
	require.Equal(t, 2048, *internalBatchCfg.EventCollectorBatchBytes)

	apiBatchCfgBack := ToAPIReplicaConfig(internalBatchCfg)
	require.NotNil(t, apiBatchCfgBack.EventCollectorBatchCount)
	require.NotNil(t, apiBatchCfgBack.EventCollectorBatchBytes)
	require.Equal(t, 4096, *apiBatchCfgBack.EventCollectorBatchCount)
	require.Equal(t, 2048, *apiBatchCfgBack.EventCollectorBatchBytes)

	apiBatchZeroCfg := &ReplicaConfig{
		EventCollectorBatchCount: util.AddressOf(0),
		EventCollectorBatchBytes: util.AddressOf(0),
	}
	internalBatchZeroCfg := apiBatchZeroCfg.ToInternalReplicaConfig()
	require.NotNil(t, internalBatchZeroCfg.EventCollectorBatchCount)
	require.NotNil(t, internalBatchZeroCfg.EventCollectorBatchBytes)
	require.Equal(t, 0, *internalBatchZeroCfg.EventCollectorBatchCount)
	require.Equal(t, 0, *internalBatchZeroCfg.EventCollectorBatchBytes)

	apiBatchZeroCfgBack := ToAPIReplicaConfig(internalBatchZeroCfg)
	require.NotNil(t, apiBatchZeroCfgBack.EventCollectorBatchCount)
	require.NotNil(t, apiBatchZeroCfgBack.EventCollectorBatchBytes)
	require.Equal(t, 0, *apiBatchZeroCfgBack.EventCollectorBatchCount)
	require.Equal(t, 0, *apiBatchZeroCfgBack.EventCollectorBatchBytes)

	internalCfgNoBatch := (&ReplicaConfig{}).ToInternalReplicaConfig()
	require.Nil(t, internalCfgNoBatch.EventCollectorBatchCount)
	require.Nil(t, internalCfgNoBatch.EventCollectorBatchBytes)

	internalCfgNoBatchBack := config.GetDefaultReplicaConfig()
	internalCfgNoBatchBack.EventCollectorBatchCount = nil
	internalCfgNoBatchBack.EventCollectorBatchBytes = nil
	apiNoBatch := ToAPIReplicaConfig(internalCfgNoBatchBack)
	require.Nil(t, apiNoBatch.EventCollectorBatchCount)
	require.Nil(t, apiNoBatch.EventCollectorBatchBytes)
}

func TestReplicaConfigConversionBatchFields(t *testing.T) {
	t.Parallel()

	apiCfg := &ReplicaConfig{
		EventCollectorBatchCount: util.AddressOf(4096),
		EventCollectorBatchBytes: util.AddressOf(2048),
	}
	internalCfg := apiCfg.ToInternalReplicaConfig()
	require.Equal(t, 4096, util.GetOrZero(internalCfg.EventCollectorBatchCount))
	require.Equal(t, 2048, util.GetOrZero(internalCfg.EventCollectorBatchBytes))

	apiCfgBack := ToAPIReplicaConfig(internalCfg)
	require.NotNil(t, apiCfgBack.EventCollectorBatchCount)
	require.NotNil(t, apiCfgBack.EventCollectorBatchBytes)
	require.Equal(t, 4096, *apiCfgBack.EventCollectorBatchCount)
	require.Equal(t, 2048, *apiCfgBack.EventCollectorBatchBytes)

	apiCfgNil := &ReplicaConfig{}
	internalCfgNil := apiCfgNil.ToInternalReplicaConfig()
	defaultCfg := config.GetDefaultReplicaConfig()
	require.Equal(
		t,
		util.GetOrZero(defaultCfg.EventCollectorBatchCount),
		util.GetOrZero(internalCfgNil.EventCollectorBatchCount),
	)
	require.Equal(
		t,
		util.GetOrZero(defaultCfg.EventCollectorBatchBytes),
		util.GetOrZero(internalCfgNil.EventCollectorBatchBytes),
	)

	internalCfgNoBatch := config.GetDefaultReplicaConfig()
	internalCfgNoBatch.EventCollectorBatchCount = nil
	internalCfgNoBatch.EventCollectorBatchBytes = nil
	apiNoBatch := ToAPIReplicaConfig(internalCfgNoBatch)
	require.Nil(t, apiNoBatch.EventCollectorBatchCount)
	require.Nil(t, apiNoBatch.EventCollectorBatchBytes)
}

func TestReplicaConfigConversionRedoBatchField(t *testing.T) {
	t.Parallel()

	apiCfg := &ReplicaConfig{
		Consistent: &ConsistentConfig{
			EventCollectorBatchCount: util.AddressOf(4096),
		},
	}

	internalCfg := apiCfg.ToInternalReplicaConfig()
	require.NotNil(t, internalCfg.Consistent)
	require.Equal(t, 4096, util.GetOrZero(internalCfg.Consistent.EventCollectorBatchCount))

	apiCfgBack := ToAPIReplicaConfig(internalCfg)
	require.NotNil(t, apiCfgBack.Consistent)
	require.NotNil(t, apiCfgBack.Consistent.EventCollectorBatchCount)
	require.Equal(t, 4096, *apiCfgBack.Consistent.EventCollectorBatchCount)
}
