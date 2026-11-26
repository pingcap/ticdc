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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dispatchermanager

import (
	"sync"
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
)

// meteringSealer manages node-local traffic byte accumulation and monotonic
// round sealing for one changefeed DispatcherManager.
type meteringSealer struct {
	current     atomic.Uint64
	mu          sync.Mutex
	sealedEpoch uint64
	sealedSeq   uint64
	sealedBytes uint64
}

func (s *meteringSealer) Counter() *atomic.Uint64 {
	return &s.current
}

// HandleCommand advances or idempotently retains the sealed traffic metering
// snapshot for the given (metering_epoch, metering_seq).
func (s *meteringSealer) HandleCommand(targetEpoch, targetSeq uint64) {
	if targetEpoch == 0 || targetSeq == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if targetEpoch > s.sealedEpoch || (targetEpoch == s.sealedEpoch && targetSeq > s.sealedSeq) {
		s.sealedBytes = s.current.Swap(0)
		s.sealedEpoch = targetEpoch
		s.sealedSeq = targetSeq
	}
}

// Report returns the currently sealed (metering_epoch, metering_seq, metering_bytes) tuple.
func (s *meteringSealer) Report() (epoch, seq, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealedEpoch, s.sealedSeq, s.sealedBytes
}

func isColumnSelectorMeteredSink(sinkType common.SinkType) bool {
	switch sinkType {
	case common.KafkaSinkType, common.PulsarSinkType, common.CloudStorageSinkType:
		return true
	default:
		return false
	}
}
