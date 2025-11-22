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

package blackhole

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"go.uber.org/zap"
)

// sink is responsible for writing data to blackhole.
// Including DDL and DML.
type sink struct {
	eventCh chan *commonEvent.DMLEvent
}

func New() (*sink, error) {
	return &sink{
		eventCh: make(chan *commonEvent.DMLEvent, 4096),
	}, nil
}

func (s *sink) IsNormal() bool {
	return true
}

func (s *sink) SinkType() common.SinkType {
	return common.BlackHoleSinkType
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *commonEvent.TableSchemaStore) {
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
	// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L23
	log.Debug("BlackHoleSink: WriteEvents", zap.Any("dml", event))
	s.eventCh <- event
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		e := event.(*commonEvent.DDLEvent)
		// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
		// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L17
		log.Debug("BlackHoleSink: DDL Event", zap.Any("ddl", e))
	case commonEvent.TypeSyncPointEvent:
	default:
		log.Error("unknown event type",
			zap.Any("event", event))
	}
	event.PostFlush()
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	log.Debug("BlackHoleSink: Checkpoint Ts Event", zap.Uint64("ts", ts))
}

func (s *sink) Close(_ bool) {}

func (s *sink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event := <-s.eventCh:
			event.PostFlush()
		}
	}
}
