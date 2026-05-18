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
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/zap"
)

// Sink is responsible for writing data to blackhole.
// Including DDL and DML.
type Sink struct {
	eventCh    *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]
	statistics *metrics.Statistics
}

func New(changefeedID common.ChangeFeedID) (*Sink, error) {
	return &Sink{
		eventCh:    chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		statistics: metrics.NewStatistics(changefeedID, "sink"),
	}, nil
}

func (s *Sink) IsNormal() bool {
	return true
}

func (s *Sink) SinkType() common.SinkType {
	return common.BlackHoleSinkType
}

func (s *Sink) SetTableSchemaStore(_ *commonEvent.TableSchemaStore) {
}

func (s *Sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
	// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L23
	// Use zap.Stringer to call String() method which applies log redaction
	log.Debug("BlackHoleSink: WriteEvents", zap.Stringer("dml", event))
	s.eventCh.Push(event)
}

func (s *Sink) FlushDMLBeforeBlock(_ commonEvent.BlockEvent) error {
	return nil
}

func (s *Sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		e := event.(*commonEvent.DDLEvent)
		// NOTE: don't change the log, integration test `lossy_ddl` depends on it.
		// ref: https://github.com/pingcap/ticdc/blob/da834db76e0662ff15ef12645d1f37bfa6506d83/tests/integration_tests/lossy_ddl/run.sh#L17
		log.Debug("BlackHoleSink: DDL Event", zap.Any("ddl", e))
		ddlType := e.GetDDLType().String()
		err := s.statistics.RecordDDLExecution(func() (string, error) {
			return ddlType, nil
		})
		if err != nil {
			return err
		}
	case commonEvent.TypeSyncPointEvent:
	default:
		log.Error("unknown event type",
			zap.Any("event", event))
	}
	event.PostFlush()
	return nil
}

func (s *Sink) AddCheckpointTs(ts uint64) {
	log.Debug("BlackHoleSink: Checkpoint Ts Event", zap.Uint64("ts", ts))
}

func (s *Sink) Close() {
	s.eventCh.Close()
	s.statistics.Close()
}

func (s *Sink) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			event, ok := s.eventCh.Get()
			if !ok {
				log.Info("blackhole sink event channel closed")
				return nil
			}
			err := s.statistics.RecordBatchExecution(func() (int, int64, error) {
				return int(event.Len()), event.GetSize(), nil
			})
			if err != nil {
				return err
			}
			event.PostFlush()
		}
	}
}

func (s *Sink) BatchCount() int {
	return s.eventCh.Len()
}

func (s *Sink) BatchBytes() int {
	return 0
}
