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

package dispatchermanager

import (
	"testing"

	"github.com/golang/mock/gomock"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	redosink "github.com/pingcap/ticdc/downstreamadapter/sink/redo"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func newBatchConfigSink(t *testing.T, batchCount int, batchBytes int) *sinkmock.MockSink {
	t.Helper()
	ctrl := gomock.NewController(t)
	s := sinkmock.NewMockSink(ctrl)
	s.EXPECT().SinkType().Return(common.MysqlSinkType).AnyTimes()
	s.EXPECT().BatchCount().Return(batchCount).AnyTimes()
	s.EXPECT().BatchBytes().Return(batchBytes).AnyTimes()
	return s
}

// TestDispatcherManagerBatchConfig ensures changefeed overrides take precedence
// over sink-derived defaults while preserving explicit zero values.
func TestDispatcherManagerBatchConfig(t *testing.T) {
	assertBatchConfig := func(
		sinkBatchCount int,
		sinkBatchBytes int,
		cfg *config.ChangefeedConfig,
		wantCount int,
		wantBytes int,
	) {
		sink := newBatchConfigSink(t, sinkBatchCount, sinkBatchBytes)
		m := &DispatcherManager{
			config: cfg,
		}
		gotCount, gotBytes := m.getEventCollectorBatchCountAndBytes(sink)
		require.Equal(t, wantCount, gotCount)
		require.Equal(t, wantBytes, gotBytes)
	}

	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{}, 2048, 8192)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchCount: util.AddressOf(0),
		EventCollectorBatchBytes: util.AddressOf(0),
	}, 0, 0)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchBytes: util.AddressOf(0),
	}, 2048, 0)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchCount: util.AddressOf(0),
	}, 0, 8192)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchCount: util.AddressOf(123),
	}, 123, 8192)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchBytes: util.AddressOf(456),
	}, 2048, 456)
	assertBatchConfig(2048, 8192, &config.ChangefeedConfig{
		EventCollectorBatchCount: util.AddressOf(123),
		EventCollectorBatchBytes: util.AddressOf(456),
	}, 123, 456)
}

func TestDispatcherManagerRedoBatchConfig(t *testing.T) {
	assertBatchConfig := func(cfg *config.ChangefeedConfig, wantCount int, wantBytes int) {
		m := &DispatcherManager{
			config: cfg,
		}
		gotCount, gotBytes := m.getRedoEventCollectorBatchCountAndBytes(&redosink.Sink{})
		require.Equal(t, wantCount, gotCount)
		require.Equal(t, wantBytes, gotBytes)
	}

	assertBatchConfig(&config.ChangefeedConfig{}, 4096, 0)
	assertBatchConfig(&config.ChangefeedConfig{
		EventCollectorBatchCount: util.AddressOf(123),
		EventCollectorBatchBytes: util.AddressOf(456),
	}, 4096, 0)
	assertBatchConfig(&config.ChangefeedConfig{
		Consistent: &config.ConsistentConfig{
			EventCollectorBatchCount: util.AddressOf(321),
		},
	}, 321, 0)
}
