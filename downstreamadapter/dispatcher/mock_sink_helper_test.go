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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/downstreamadapter/sink/mock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

// dispatcherTestSink wraps gomock sink and keeps the few stateful helpers that
// old tests used (captured DML events and normal/abnormal switch).
type dispatcherTestSink struct {
	sink *mock.MockSink

	mu       sync.Mutex
	dmls     []*commonEvent.DMLEvent
	isNormal atomic.Bool

	flushMu              sync.Mutex
	flushBeforeBlockHook func(commonEvent.BlockEvent) error
}

func newDispatcherTestSink(t *testing.T, sinkType common.SinkType) *dispatcherTestSink {
	t.Helper()

	ctrl := gomock.NewController(t)
	testSink := &dispatcherTestSink{
		sink: mock.NewMockSink(ctrl),
		dmls: make([]*commonEvent.DMLEvent, 0),
	}
	testSink.isNormal.Store(true)

	testSink.sink.EXPECT().SinkType().Return(sinkType).AnyTimes()
	testSink.sink.EXPECT().IsNormal().DoAndReturn(func() bool {
		return testSink.isNormal.Load()
	}).AnyTimes()
	testSink.sink.EXPECT().AddDMLEvent(gomock.Any()).Do(func(event *commonEvent.DMLEvent) {
		testSink.mu.Lock()
		defer testSink.mu.Unlock()
		testSink.dmls = append(testSink.dmls, event)
	}).AnyTimes()
	testSink.sink.EXPECT().WriteBlockEvent(gomock.Any()).DoAndReturn(func(event commonEvent.BlockEvent) error {
		event.PostFlush()
		return nil
	}).AnyTimes()
	testSink.sink.EXPECT().FlushDMLBeforeBlock(gomock.Any()).DoAndReturn(func(event commonEvent.BlockEvent) error {
		testSink.flushMu.Lock()
		hook := testSink.flushBeforeBlockHook
		testSink.flushMu.Unlock()
		if hook != nil {
			return hook(event)
		}
		return nil
	}).AnyTimes()
	testSink.sink.EXPECT().AddCheckpointTs(gomock.Any()).AnyTimes()
	testSink.sink.EXPECT().SetTableSchemaStore(gomock.Any()).AnyTimes()
	testSink.sink.EXPECT().Close(gomock.Any()).AnyTimes()
	testSink.sink.EXPECT().Run(gomock.Any()).Return(nil).AnyTimes()
	return testSink
}

func (s *dispatcherTestSink) Sink() sink.Sink {
	return s.sink
}

func (s *dispatcherTestSink) SetIsNormal(isNormal bool) {
	s.isNormal.Store(isNormal)
}

func (s *dispatcherTestSink) SetFlushBeforeBlockHook(hook func(commonEvent.BlockEvent) error) {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	s.flushBeforeBlockHook = hook
}

func (s *dispatcherTestSink) GetDMLs() []*commonEvent.DMLEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	dmls := make([]*commonEvent.DMLEvent, len(s.dmls))
	copy(dmls, s.dmls)
	return dmls
}

func (s *dispatcherTestSink) FlushDMLs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, dml := range s.dmls {
		dml.PostFlush()
	}
	s.dmls = s.dmls[:0]
}
