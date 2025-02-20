// Copyright 2024 PingCAP, Inc.
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

package sink

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

// BlackHoleSink is responsible for writing data to blackhole.
// Including DDL and DML.
type BlackHoleSink struct{}

func newBlackHoleSink() (*BlackHoleSink, error) {
	blackholeSink := BlackHoleSink{}
	return &blackholeSink, nil
}

func (s *BlackHoleSink) IsNormal() bool {
	return true
}

func (s *BlackHoleSink) SinkType() common.SinkType {
	return common.BlackHoleSinkType
}

func (s *BlackHoleSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
}

func (s *BlackHoleSink) AddDMLEvent(event *commonEvent.DMLEvent) error {
	// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
	// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L23
	log.Debug("BlackHoleSink: WriteEvents", zap.Any("dml", event))
	for _, callback := range event.PostTxnFlushed {
		callback()
	}
	return nil
}

func (s *BlackHoleSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *BlackHoleSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		e := event.(*commonEvent.DDLEvent)
		// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
		// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L17
		log.Debug("BlackHoleSink: DDL Event", zap.Any("ddl", e))
		for _, callback := range e.PostTxnFlushed {
			callback()
		}
	case commonEvent.TypeSyncPointEvent:
		e := event.(*commonEvent.SyncPointEvent)
		for _, callback := range e.PostTxnFlushed {
			callback()
		}
	default:
		log.Error("unknown event type",
			zap.Any("event", event))
	}
	return nil
}

func (s *BlackHoleSink) AddCheckpointTs(ts uint64) {
}

func (s *BlackHoleSink) GetStartTsList(tableIds []int64, startTsList []int64) ([]int64, error) {
	return []int64{}, nil
}

func (s *BlackHoleSink) Close(_ bool) {}

func (s *BlackHoleSink) Run(_ context.Context) error {
	return nil
}
