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

package dispatcher

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	sinkmock "github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func newTestBasicDispatcherForBatchConfig(
	s sink.Sink,
	batchCount int,
	batchBytes int,
) *BasicDispatcher {
	sharedInfo := NewSharedInfo(
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
		make(chan TableSpanStatusWithSeq, 1),
		make(chan *heartbeatpb.TableSpanBlockStatus, 1),
		make(chan error, 1),
	)
	return NewBasicDispatcher(
		common.NewDispatcherID(),
		&heartbeatpb.TableSpan{TableID: 1},
		1,
		1,
		NewSchemaIDToDispatchers(),
		false,
		false,
		0,
		common.DefaultMode,
		s,
		sharedInfo,
	)
}

func TestBasicDispatcherBatchConfig(t *testing.T) {
	t.Run("returns values from shared info", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		d := newTestBasicDispatcherForBatchConfig(
			mockSink,
			123,
			777,
		)
		gotCount, gotBytes := d.GetEventCollectorBatchConfig()
		require.Equal(t, 123, gotCount)
		require.Equal(t, 777, gotBytes)
	})

	t.Run("zero values stay zero without sink fallback", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		d := newTestBasicDispatcherForBatchConfig(
			mockSink,
			0,
			0,
		)
		gotCount, gotBytes := d.GetEventCollectorBatchConfig()
		require.Equal(t, 0, gotCount)
		require.Equal(t, 0, gotBytes)
	})

	t.Run("redo mode uses the same shared config", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockSink := sinkmock.NewMockSink(ctrl)
		d := newTestBasicDispatcherForBatchConfig(
			mockSink,
			4096,
			8192,
		)
		d.mode = common.RedoMode
		gotCount, gotBytes := d.GetEventCollectorBatchConfig()
		require.Equal(t, 4096, gotCount)
		require.Equal(t, 8192, gotBytes)
	})
}
