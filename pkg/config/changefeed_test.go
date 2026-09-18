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

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestChangeFeedInfoToChangefeedConfigKeepsAllowSameCluster(t *testing.T) {
	t.Parallel()

	// The same upstream/downstream check runs on the ChangefeedConfig derived from the persisted
	// replica config, so `allow-same-cluster` must survive the conversion to skip the check.
	cfg := GetDefaultReplicaConfig()
	require.False(t, util.GetOrZero(cfg.AllowSameCluster))
	cfg.AllowSameCluster = util.AddressOf(true)
	info := &ChangeFeedInfo{Config: cfg}

	require.True(t, info.ToChangefeedConfig().AllowSameCluster)
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

func TestChangeFeedInfoRmUnusedFieldsKeepsTableRouting(t *testing.T) {
	for _, sinkURI := range []string{"mysql://127.0.0.1:3306", "file:///tmp/cdc"} {
		t.Run(sinkURI, func(t *testing.T) {
			cfg := GetDefaultReplicaConfig()
			cfg.Sink.DispatchRules = []*DispatchRule{
				nil,
				{
					Matcher:        []string{"sales.*", "!sales.tmp"},
					TargetSchema:   "archive",
					TargetTable:    "{schema}_{table}",
					DispatcherRule: "ts",
					PartitionRule:  "index-value",
					IndexName:      "primary",
					Columns:        []string{"id"},
					TopicRule:      "sales-events",
				},
			}
			info := &ChangeFeedInfo{SinkURI: sinkURI, Config: cfg}

			info.RmUnusedFields()

			require.Equal(t, []*DispatchRule{
				nil,
				{
					Matcher:      []string{"sales.*", "!sales.tmp"},
					TargetSchema: "archive",
					TargetTable:  "{schema}_{table}",
				},
			}, info.Config.Sink.DispatchRules)
		})
	}
}
