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

package replica

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/maintainer/split"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/tikv/client-go/v2/tikv"
)

var defaultRegionCountRefreshInterval = 2 * time.Minute

type regionCountJob struct {
	id   common.DispatcherID
	span *heartbeatpb.TableSpan
}

func queryRegionCount(regionCache split.RegionCache, job regionCountJob) (int, error) {
	regions, err := regionCache.LoadRegionsInKeyRange(
		tikv.NewBackoffer(context.Background(), 500),
		job.span.StartKey, job.span.EndKey,
	)
	if err != nil {
		return 0, err
	}
	return len(regions), nil
}

func runRegionCountJobs(regionCache split.RegionCache, jobs []regionCountJob, apply func(regionCountJob, int, error)) {
	for _, job := range jobs {
		count, err := queryRegionCount(regionCache, job)
		apply(job, count, err)
	}
}

var splitRegionRefresher = newSplitRegionCountManager()

type splitRegionCountManager struct {
	mu      sync.Mutex
	workers map[string]*splitRegionCountWorker
}

func newSplitRegionCountManager() *splitRegionCountManager {
	return &splitRegionCountManager{
		workers: make(map[string]*splitRegionCountWorker),
	}
}

func (m *splitRegionCountManager) register(
	cfID common.ChangeFeedID,
	interval time.Duration,
	checker *SplitSpanChecker,
) func() {
	if interval <= 0 {
		interval = defaultRegionCountRefreshInterval
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := cfID.String()
	worker, ok := m.workers[key]
	if !ok {
		worker = newSplitRegionCountWorker(cfID, interval)
		m.workers[key] = worker
	}
	worker.addChecker(checker)
	return func() {
		m.unregister(cfID, checker)
	}
}

func (m *splitRegionCountManager) unregister(cfID common.ChangeFeedID, checker *SplitSpanChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := cfID.String()
	worker, ok := m.workers[key]
	if !ok {
		return
	}
	worker.removeChecker(checker)
	if worker.isEmpty() {
		worker.stop()
		delete(m.workers, key)
	}
}

type splitRegionCountWorker struct {
	cfID     common.ChangeFeedID
	mu       sync.Mutex
	checkers map[*SplitSpanChecker]struct{}
	interval time.Duration

	stopCh    chan struct{}
	stoppedCh chan struct{}
}

func newSplitRegionCountWorker(cfID common.ChangeFeedID, interval time.Duration) *splitRegionCountWorker {
	worker := &splitRegionCountWorker{
		cfID:      cfID,
		checkers:  make(map[*SplitSpanChecker]struct{}),
		interval:  interval,
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
	go worker.run()
	return worker
}

func (w *splitRegionCountWorker) run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	w.refreshAll()
	for {
		select {
		case <-ticker.C:
			w.refreshAll()
		case <-w.stopCh:
			close(w.stoppedCh)
			return
		}
	}
}

func (w *splitRegionCountWorker) refreshAll() {
	checkers := w.snapshotCheckers()
	if len(checkers) == 0 {
		return
	}
	for _, checker := range checkers {
		checker.refreshRegionCounts()
	}
}

func (w *splitRegionCountWorker) snapshotCheckers() []*SplitSpanChecker {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.checkers) == 0 {
		return nil
	}
	res := make([]*SplitSpanChecker, 0, len(w.checkers))
	for checker := range w.checkers {
		res = append(res, checker)
	}
	return res
}

func (w *splitRegionCountWorker) addChecker(checker *SplitSpanChecker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.checkers[checker] = struct{}{}
}

func (w *splitRegionCountWorker) removeChecker(checker *SplitSpanChecker) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.checkers, checker)
}

func (w *splitRegionCountWorker) isEmpty() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.checkers) == 0
}

func (w *splitRegionCountWorker) stop() {
	close(w.stopCh)
	<-w.stoppedCh
}
