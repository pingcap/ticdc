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
package dispatcher

import (
	"testing"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
)

func TestEnableIgnoreUpdateOnlyColumns(t *testing.T) {
	require.True(t, newTestBasicDispatcher(t, common.KafkaSinkType).EnableIgnoreUpdateOnlyColumns())
	require.False(t, newTestBasicDispatcher(t, common.MysqlSinkType).EnableIgnoreUpdateOnlyColumns())
	require.False(t, newTestBasicDispatcher(t, common.PulsarSinkType).EnableIgnoreUpdateOnlyColumns())
}

func newTestBasicDispatcher(t *testing.T, sinkType common.SinkType) *BasicDispatcher {
	t.Helper()
	sharedInfo := newTestSharedInfo(false, nil)
	dispatcherSink := newDispatcherTestSink(t, sinkType)
	tableSpan := &heartbeatpb.TableSpan{TableID: 1, StartKey: []byte{0}, EndKey: []byte{1}}
	return NewBasicDispatcher(
		common.NewDispatcherID(),
		tableSpan,
		100,
		1,
		NewSchemaIDToDispatchers(),
		false,
		false,
		4096,
		0,
		200,
		common.DefaultMode,
		dispatcherSink.Sink(),
		sharedInfo,
	)
}
