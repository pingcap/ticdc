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

package sink

import (
	"context"
	"sync"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	sinkutil "github.com/pingcap/ticdc/pkg/sink/util"
)

type mockSink struct {
	mu       sync.Mutex
	dmls     []*commonEvent.DMLEvent
	isNormal bool
	sinkType common.SinkType
}

func (s *mockSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dmls = append(s.dmls, event)
}

func (s *mockSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	event.PostFlush()
	return nil
}

func (s *mockSink) AddCheckpointTs(_ uint64) {
}

func (s *mockSink) SetTableSchemaStore(_ *sinkutil.TableSchemaStore) {
}

func (s *mockSink) Close(bool) {}

func (s *mockSink) Run(context.Context) error {
	return nil
}

func (s *mockSink) SinkType() common.SinkType {
	return s.sinkType
}

func (s *mockSink) IsNormal() bool {
	return s.isNormal
}

func (s *mockSink) SetIsNormal(isNormal bool) {
	s.isNormal = isNormal
}

func (s *mockSink) FlushDMLs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dml := range s.dmls {
		dml.PostFlush()
	}
	s.dmls = make([]*commonEvent.DMLEvent, 0)
}

func (s *mockSink) GetDMLs() []*commonEvent.DMLEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dmls
}

func NewMockSink(sinkType common.SinkType) *mockSink {
	return &mockSink{
		dmls:     make([]*commonEvent.DMLEvent, 0),
		isNormal: true,
		sinkType: sinkType,
	}
}
