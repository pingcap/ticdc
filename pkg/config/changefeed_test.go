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

// TestFeedStateIsResumable verifies the shared state predicate used by CLI/API
// resume paths. Only stopped or terminal-but-resumable states should be accepted.
func TestFeedStateIsResumable(t *testing.T) {
	tests := []struct {
		state     FeedState
		resumable bool
	}{
		{state: StateStopped, resumable: true},
		{state: StateFailed, resumable: true},
		{state: StateFinished, resumable: true},
		{state: StateNormal, resumable: false},
		{state: StateWarning, resumable: false},
		{state: StatePending, resumable: false},
		{state: StateRemoved, resumable: false},
		{state: StateUnInitialized, resumable: false},
	}

	for _, tt := range tests {
		require.Equal(t, tt.resumable, tt.state.IsResumable())
	}
}

// TestChangeFeedInfoToChangefeedConfigBatchFields ensures the maintainer-facing
// changefeed config keeps the optional event collector batch overrides.
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

func TestChangeFeedInfoToChangefeedConfigPerformanceMode(t *testing.T) {
	replicaConfig := GetDefaultReplicaConfig()
	replicaConfig.PerformanceMode = util.AddressOf(PerformanceModeLowLatency)
	info := &ChangeFeedInfo{
		ChangefeedID: common.NewChangefeedID4Test("test", "test"),
		Config:       replicaConfig,
	}

	changefeedConfig := info.ToChangefeedConfig()
	require.Equal(t, PerformanceModeLowLatency, changefeedConfig.PerformanceMode)
	require.True(t, changefeedConfig.IsLowLatencyMode())
}

func TestChangeFeedInfoStringMasksSensitiveData(t *testing.T) {
	cfg := GetDefaultReplicaConfig()
	cfg.Sink.KafkaConfig = &KafkaConfig{
		SASLPassword:          util.AddressOf("plain-password-sentinel"),
		SASLGssAPIPassword:    util.AddressOf("gssapi-password-sentinel"),
		SASLOAuthClientSecret: util.AddressOf("oauth-secret-sentinel"),
		SASLOAuthTokenURL:     util.AddressOf("https://oauth.example.com/token?client_secret=token-url-secret-sentinel"),
		LargeMessageHandle:    &LargeMessageHandleConfig{ClaimCheckStorageURI: "s3://bucket/prefix?access-key=claim-check-secret-sentinel"},
	}
	info := &ChangeFeedInfo{
		SinkURI: "kafka://user:sink-password-sentinel@127.0.0.1:9092/topic?secret=uri-secret-sentinel",
		Config:  cfg,
	}
	original, err := info.Marshal()
	require.NoError(t, err)

	output := info.String()
	for _, secret := range []string{
		"sink-password-sentinel",
		"uri-secret-sentinel",
		"plain-password-sentinel",
		"gssapi-password-sentinel",
		"oauth-secret-sentinel",
		"token-url-secret-sentinel",
		"claim-check-secret-sentinel",
	} {
		require.NotContains(t, output, secret)
	}
	require.Contains(t, output, "xxxxx")
	require.Contains(t, output, "******")
	after, err := info.Marshal()
	require.NoError(t, err)
	require.Equal(t, original, after)
}

func TestChangeFeedInfoRmUnusedFieldsKeepsSchemaRegistryForAvroProtocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		protocol     Protocol
		keepRegistry bool
	}{
		{protocol: ProtocolAvro, keepRegistry: true},
		{protocol: ProtocolDebeziumAvro, keepRegistry: true},
		{protocol: ProtocolDebezium, keepRegistry: false},
	}

	for _, tt := range tests {
		t.Run(tt.protocol.String(), func(t *testing.T) {
			t.Parallel()

			cfg := GetDefaultReplicaConfig()
			cfg.Sink.Protocol = util.AddressOf(tt.protocol.String())
			cfg.Sink.SchemaRegistry = util.AddressOf("http://127.0.0.1:8088")
			info := &ChangeFeedInfo{
				SinkURI: "kafka://127.0.0.1:9092/topic",
				Config:  cfg,
			}

			info.RmUnusedFields()
			if tt.keepRegistry {
				require.NotNil(t, info.Config.Sink.SchemaRegistry)
				require.Equal(t, "http://127.0.0.1:8088", *info.Config.Sink.SchemaRegistry)
			} else {
				require.Nil(t, info.Config.Sink.SchemaRegistry)
			}
		})
	}
}
