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

package maintainer

import (
	"slices"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/node"
)

const (
	defaultMeteringRoundInterval  = time.Second
	meteringRoundStallThreshold   = 30 * time.Second
	meteringRoundStallLogInterval = 60 * time.Second
)

type meteringCommand struct {
	epoch uint64
	seq   uint64
}

type meteringRoundReport struct {
	epoch uint64
	seq   uint64
	bytes uint64
}

type meteringStall struct {
	targetSeq    uint64
	stalledFor   time.Duration
	missingNodes []node.ID
}

type meteringHeartbeatResult struct {
	cmd            *meteringCommand
	roundCompleted bool
	stall          *meteringStall
}

// meteringCollector coordinates sealed round collection across DispatcherManagers
// for one changefeed within a single Coordinator metering_epoch.
type meteringCollector struct {
	mu             sync.Mutex
	epoch          uint64
	ackedSeq       uint64
	targetSeq      uint64
	completedSeq   uint64
	completedBytes uint64
	supportedNodes map[node.ID]struct{}
	roundBytes     map[node.ID]uint64
	nextRoundAt    time.Time
	roundStartedAt time.Time
	lastStallLogAt time.Time
}

func newMeteringCollector() *meteringCollector {
	return &meteringCollector{
		supportedNodes: make(map[node.ID]struct{}),
		roundBytes:     make(map[node.ID]uint64),
	}
}

func (m *meteringCollector) SetInitialState(epoch, lastSeq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetLocked(epoch, lastSeq)
}

func (m *meteringCollector) SetEpoch(epoch uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Reject stale epochs to match DispatcherManager's monotonic sealing, while allowing 0 to disable metering.
	if epoch == m.epoch || (epoch > 0 && epoch < m.epoch) {
		return
	}
	m.resetLocked(epoch, 0)
}

func (m *meteringCollector) resetLocked(epoch, lastSeq uint64) {
	m.epoch = epoch
	m.ackedSeq = lastSeq
	if epoch > 0 {
		m.targetSeq = lastSeq + 1
	} else {
		m.targetSeq = 0
	}
	m.completedSeq = 0
	m.completedBytes = 0
	clear(m.roundBytes)
	m.nextRoundAt = time.Time{}
	m.roundStartedAt = time.Time{}
	m.lastStallLogAt = time.Time{}
}

// MarkNodeSupported records bootstrap-time metering capability before heartbeats
// arrive so Maintainer failover does not prematurely complete rounds without
// waiting for other surviving DispatcherManagers.
func (m *meteringCollector) MarkNodeSupported(id node.ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.supportedNodes[id] = struct{}{}
}

func (m *meteringCollector) RemoveNode(id node.ID, activeNodes []node.ID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.supportedNodes, id)
	if m.epoch == 0 || m.completedSeq > 0 {
		return false
	}
	return m.tryCompleteRoundLocked(activeNodes)
}

func (m *meteringCollector) OnHeartbeat(
	from node.ID,
	supported bool,
	msgEpoch, msgSeq, msgBytes uint64,
	getActiveNodes func() []node.ID,
	now time.Time,
) meteringHeartbeatResult {
	m.mu.Lock()
	if supported {
		m.supportedNodes[from] = struct{}{}
	}
	if m.epoch == 0 || m.completedSeq > 0 || (!m.nextRoundAt.IsZero() && now.Before(m.nextRoundAt)) {
		m.mu.Unlock()
		return meteringHeartbeatResult{}
	}
	if m.roundStartedAt.IsZero() {
		m.roundStartedAt = now
	}
	if msgEpoch == m.epoch && msgSeq == m.targetSeq {
		m.roundBytes[from] = msgBytes
	}
	m.mu.Unlock()

	var activeNodes []node.ID
	if getActiveNodes != nil {
		activeNodes = getActiveNodes()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch == 0 || m.completedSeq > 0 || (!m.nextRoundAt.IsZero() && now.Before(m.nextRoundAt)) {
		return meteringHeartbeatResult{}
	}
	if m.tryCompleteRoundLocked(activeNodes) {
		return meteringHeartbeatResult{roundCompleted: true}
	}

	var res meteringHeartbeatResult
	if stalledFor := now.Sub(m.roundStartedAt); stalledFor >= meteringRoundStallThreshold &&
		(m.lastStallLogAt.IsZero() || now.Sub(m.lastStallLogAt) >= meteringRoundStallLogInterval) {
		if missing := m.missingSupportedNodesLocked(activeNodes); len(missing) > 0 {
			m.lastStallLogAt = now
			res.stall = &meteringStall{
				targetSeq:    m.targetSeq,
				stalledFor:   stalledFor,
				missingNodes: missing,
			}
		}
	}

	if _, ok := m.supportedNodes[from]; ok && (msgEpoch != m.epoch || msgSeq != m.targetSeq) {
		res.cmd = &meteringCommand{
			epoch: m.epoch,
			seq:   m.targetSeq,
		}
	}
	return res
}

func (m *meteringCollector) missingSupportedNodesLocked(activeNodes []node.ID) []node.ID {
	var missing []node.ID
	for _, id := range activeNodes {
		if _, ok := m.supportedNodes[id]; !ok {
			continue
		}
		if _, reported := m.roundBytes[id]; !reported {
			missing = append(missing, id)
		}
	}
	slices.Sort(missing)
	return missing
}

func (m *meteringCollector) tryCompleteRoundLocked(activeNodes []node.ID) bool {
	supportedActiveCount := 0
	for _, id := range activeNodes {
		if _, ok := m.supportedNodes[id]; !ok {
			// Skip legacy nodes that have not declared metering support yet during rolling upgrade.
			continue
		}
		supportedActiveCount++
		if _, reported := m.roundBytes[id]; !reported {
			return false
		}
	}
	if supportedActiveCount == 0 {
		return false
	}
	var sum uint64
	for _, n := range m.roundBytes {
		sum += n
	}
	m.completedSeq = m.targetSeq
	m.completedBytes = sum
	clear(m.roundBytes)
	m.roundStartedAt = time.Time{}
	m.lastStallLogAt = time.Time{}
	return true
}

func (m *meteringCollector) GetStatusReport() (meteringRoundReport, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch == 0 || m.completedSeq == 0 {
		return meteringRoundReport{}, false
	}
	return meteringRoundReport{
		epoch: m.epoch,
		seq:   m.completedSeq,
		bytes: m.completedBytes,
	}, true
}

func (m *meteringCollector) OnAck(epoch, ackedSeq uint64, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.epoch == 0 || epoch != m.epoch {
		return
	}
	if ackedSeq > m.ackedSeq {
		m.ackedSeq = ackedSeq
	}
	if m.completedSeq > 0 && m.ackedSeq >= m.completedSeq {
		m.completedSeq = 0
		m.completedBytes = 0
		m.targetSeq = m.ackedSeq + 1
		m.nextRoundAt = now.Add(defaultMeteringRoundInterval)
		m.roundStartedAt = time.Time{}
		m.lastStallLogAt = time.Time{}
		return
	}
	if m.targetSeq <= m.ackedSeq {
		m.targetSeq = m.ackedSeq + 1
	}
}
