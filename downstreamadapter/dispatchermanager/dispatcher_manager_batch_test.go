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

package dispatchermanager

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/downstreamadapter/dispatcher"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

type mockBatchConfigSink struct {
	sinkType   common.SinkType
	batchCount int
	batchBytes int
}

func (s *mockBatchConfigSink) SinkType() common.SinkType { return s.sinkType }
func (s *mockBatchConfigSink) IsNormal() bool            { return true }
func (s *mockBatchConfigSink) AddDMLEvent(_ *commonEvent.DMLEvent) {
}
func (s *mockBatchConfigSink) WriteBlockEvent(_ commonEvent.BlockEvent) error { return nil }
func (s *mockBatchConfigSink) AddCheckpointTs(_ uint64)                       {}
func (s *mockBatchConfigSink) SetTableSchemaStore(_ *commonEvent.TableSchemaStore) {
}
func (s *mockBatchConfigSink) Close(_ bool)                {}
func (s *mockBatchConfigSink) Run(_ context.Context) error { return nil }
func (s *mockBatchConfigSink) BatchCount() int {
	if s.batchCount > 0 {
		return s.batchCount
	}
	return 4096
}
func (s *mockBatchConfigSink) BatchBytes() int {
	return s.batchBytes
}

func TestDispatcherManagerBatchConfig(t *testing.T) {
	cases := []struct {
		name           string
		sinkBatchCount int
		sinkBatchBytes int
		cfg            *config.ChangefeedConfig
		wantCount      int
		wantBytes      int
	}{
		{
			name:           "uses sink defaults",
			sinkBatchCount: 0,
			sinkBatchBytes: 0,
			cfg:            &config.ChangefeedConfig{},
			wantCount:      4096,
			wantBytes:      0,
		},
		{
			name:           "uses sink provided values",
			sinkBatchCount: 2048,
			sinkBatchBytes: 8192,
			cfg:            &config.ChangefeedConfig{},
			wantCount:      2048,
			wantBytes:      8192,
		},
		{
			name:           "overrides count only",
			sinkBatchCount: 2048,
			sinkBatchBytes: 8192,
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchCount: 123,
			},
			wantCount: 123,
			wantBytes: 8192,
		},
		{
			name:           "overrides bytes only",
			sinkBatchCount: 2048,
			sinkBatchBytes: 8192,
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchBytes: 456,
			},
			wantCount: 2048,
			wantBytes: 456,
		},
		{
			name:           "overrides both",
			sinkBatchCount: 2048,
			sinkBatchBytes: 8192,
			cfg: &config.ChangefeedConfig{
				EventCollectorBatchCount: 123,
				EventCollectorBatchBytes: 456,
			},
			wantCount: 123,
			wantBytes: 456,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &DispatcherManager{
				sink: &mockBatchConfigSink{
					sinkType:   common.MysqlSinkType,
					batchCount: tc.sinkBatchCount,
					batchBytes: tc.sinkBatchBytes,
				},
				config: tc.cfg,
			}
			gotCount, gotBytes := m.getEventCollectorBatchCountAndBytes()
			require.Equal(t, tc.wantCount, gotCount)
			require.Equal(t, tc.wantBytes, gotBytes)
		})
	}
}

func TestDispatcherManagerBatchConfigPassThroughDispatcher(t *testing.T) {
	sink := &mockBatchConfigSink{
		sinkType:   common.MysqlSinkType,
		batchCount: 2048,
		batchBytes: 8192,
	}
	manager := &DispatcherManager{
		sink:   sink,
		config: &config.ChangefeedConfig{},
	}

	batchCount, batchBytes := manager.getEventCollectorBatchCountAndBytes()
	require.Equal(t, 2048, batchCount)
	require.Equal(t, 8192, batchBytes)

	sharedInfo := dispatcher.NewSharedInfo(
		common.NewChangefeedID(common.DefaultKeyspaceName),
		"system",
		false,
		false,
		false,
		nil,
		&eventpb.FilterConfig{},
		nil,
		nil,
		false,
		batchCount,
		batchBytes,
		make(chan dispatcher.TableSpanStatusWithSeq, 1),
		make(chan *heartbeatpb.TableSpanBlockStatus, 1),
		make(chan error, 1),
	)
	d := dispatcher.NewBasicDispatcher(
		common.NewDispatcherID(),
		&heartbeatpb.TableSpan{TableID: 1},
		1,
		1,
		dispatcher.NewSchemaIDToDispatchers(),
		false,
		false,
		0,
		common.DefaultMode,
		sink,
		sharedInfo,
	)

	gotCount, gotBytes := d.GetEventCollectorBatchConfig()
	require.Equal(t, 2048, gotCount)
	require.Equal(t, 8192, gotBytes)
}
