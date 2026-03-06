// Copyright 2025 PingCAP, Inc.
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

package iceberg_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestSinkConfigWithDualWrite(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		icebergCfg    *config.IcebergConfig
		wantDualWrite bool
		wantErr       bool
	}{
		{
			name: "dual-write enabled with broker",
			icebergCfg: &config.IcebergConfig{
				Warehouse: stringPtr("file:///tmp/test-iceberg-warehouse"),
				Catalog:   stringPtr("hadoop"),
				Broker:    stringPtr("kafka1:9092,kafka2:9092"),
			},
			wantDualWrite: true,
			wantErr:       false,
		},
		{
			name: "dual-write disabled without broker",
			icebergCfg: &config.IcebergConfig{
				Warehouse: stringPtr("file:///tmp/test-iceberg-warehouse"),
				Catalog:   stringPtr("hadoop"),
				Broker:    nil,
			},
			wantDualWrite: false,
			wantErr:       false,
		},
		{
			name: "dual-write disabled with empty broker",
			icebergCfg: &config.IcebergConfig{
				Warehouse: stringPtr("file:///tmp/test-iceberg-warehouse"),
				Catalog:   stringPtr("hadoop"),
				Broker:    stringPtr(""),
			},
			wantDualWrite: false,
			wantErr:       false,
		},
		{
			name: "dual-write with kafka config",
			icebergCfg: &config.IcebergConfig{
				Warehouse: stringPtr("file:///tmp/test-iceberg-warehouse"),
				Catalog:   stringPtr("hadoop"),
				Broker:    stringPtr("kafka1:9092"),
				KafkaConfig: &config.KafkaConfig{
					PartitionNum: int32Ptr(8),
				},
			},
			wantDualWrite: true,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinkURI, _ := url.Parse("iceberg://file:///tmp/test-iceberg-warehouse")
			sinkConfig := &config.SinkConfig{
				IcebergConfig: tt.icebergCfg,
			}

			changefeedID := common.NewChangeFeedIDWithName("test", "default")
			err := iceberg.Verify(ctx, changefeedID, sinkURI, sinkConfig)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSinkConfigValidation(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		broker     string
		wantErr    bool
		errContain string
	}{
		{
			name:    "valid broker",
			broker:  "kafka1:9092,kafka2:9092",
			wantErr: false,
		},
		{
			name:       "empty broker trimmed to empty",
			broker:     "   ",
			wantErr:    true,
			errContain: "empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinkURI, _ := url.Parse("iceberg://file:///tmp/test-iceberg-warehouse")
			sinkConfig := &config.SinkConfig{
				IcebergConfig: &config.IcebergConfig{
					Warehouse: stringPtr("file:///tmp/test-iceberg-warehouse"),
					Catalog:   stringPtr("hadoop"),
					Broker:    stringPtr(tt.broker),
				},
			}

			changefeedID := common.NewChangeFeedIDWithName("test", "default")
			err := iceberg.Verify(ctx, changefeedID, sinkURI, sinkConfig)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContain != "" {
					require.Contains(t, err.Error(), tt.errContain)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func stringPtr(s string) *string {
	return &s
}

func int32Ptr(i int32) *int32 {
	return &i
}

func TestDualWriteTopicNaming(t *testing.T) {
	tests := []struct {
		name           string
		keyspace       string
		changefeedName string
		wantPrefix     string
	}{
		{
			name:           "default keyspace",
			keyspace:       "default",
			changefeedName: "test-changefeed",
			wantPrefix:     "ticdc-iceberg",
		},
		{
			name:           "custom keyspace",
			keyspace:       "mykeyspace",
			changefeedName: "cf1",
			wantPrefix:     "ticdc-iceberg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantPrefix, "ticdc-iceberg")
		})
	}
}
