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
	"testing"

	"github.com/golang/mock/gomock"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

func newBatchConfigSink(t *testing.T, sinkType common.SinkType, batchCount int, batchBytes int) *sinkmock.MockSink {
	t.Helper()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().SinkType().Return(sinkType).AnyTimes()
	s.EXPECT().BatchCount().Return(batchCount).AnyTimes()
	s.EXPECT().BatchBytes().Return(batchBytes).AnyTimes()
	return s
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
			sinkBatchCount: 4096,
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
			sink := newBatchConfigSink(t, common.MysqlSinkType, tc.sinkBatchCount, tc.sinkBatchBytes)
			m := &DispatcherManager{
				config: tc.cfg,
			}
			gotCount, gotBytes := m.getEventCollectorBatchCountAndBytes(sink)
			require.Equal(t, tc.wantCount, gotCount)
			require.Equal(t, tc.wantBytes, gotBytes)
		})
	}
}
