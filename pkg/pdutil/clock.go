// Copyright 2021 PingCAP, Inc.
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

package pdutil

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/retry"
	pclock "github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

const pdTimeUpdateInterval = 10 * time.Millisecond

// Clock is a time source of PD cluster.
type Clock interface {
	// CurrentTime returns approximate current time from pd.
	CurrentTime() time.Time
	// CurrentTS returns the current timestamp from pd.
	CurrentTS() uint64
	Run(ctx context.Context)
	Close()
}

// clock cache time get from PD periodically and cache it
type clock struct {
	pdClient pd.Client
	mu       struct {
		sync.RWMutex
		physicalTs int64
		logicTs    int64
		// The time encoded in PD ts.
		tsEventTime time.Time
		// The time we receive PD ts.
		tsProcessingTime time.Time
	}
	updateInterval time.Duration
	cancel         context.CancelFunc
	stopCh         chan struct{}
	wg             sync.WaitGroup
}

// NewClock return a new clock
func NewClock(ctx context.Context, pdClient pd.Client) (*clock, error) {
	ret := &clock{
		pdClient:       pdClient,
		stopCh:         make(chan struct{}, 1),
		updateInterval: pdTimeUpdateInterval,
	}
	physical, logic, err := pdClient.GetTS(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret.mu.physicalTs = physical
	ret.mu.logicTs = logic
	ret.mu.tsEventTime = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
	ret.mu.tsProcessingTime = time.Now()
	return ret, nil
}

// Run gets time from pd periodically.
func (c *clock) Run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()
	ticker := time.NewTicker(c.updateInterval)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			// c.Stop() was called or parent ctx was canceled
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := retry.Do(ctx, func() error {
					physical, logic, err := c.pdClient.GetTS(ctx)
					if err != nil {
						log.Info("get time from pd failed, retry later", zap.Error(err))
						return err
					}
					c.mu.Lock()
					c.mu.physicalTs = physical
					c.mu.logicTs = logic
					c.mu.tsEventTime = oracle.GetTimeFromTS(oracle.ComposeTS(physical, 0))
					c.mu.tsProcessingTime = time.Now()
					c.mu.Unlock()
					return nil
				}, retry.WithBackoffBaseDelay(200), retry.WithMaxTries(10))
				if err != nil {
					log.Warn("get time from pd failed, do not update time cache",
						zap.Time("cachedTime", c.mu.tsEventTime),
						zap.Time("processingTime", c.mu.tsProcessingTime),
						zap.Error(err))
				}
			}
		}
	}()
}

// CurrentTime returns approximate current time from pd.
func (c *clock) CurrentTime() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tsEventTime := c.mu.tsEventTime
	current := tsEventTime.Add(time.Since(c.mu.tsProcessingTime))
	return current
}

func (c *clock) CurrentTS() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	elapsed := time.Since(c.mu.tsProcessingTime).Milliseconds()
	physical := c.mu.physicalTs + elapsed
	return oracle.ComposeTS(physical, c.mu.logicTs)
}

// Stop clock.
func (c *clock) Close() {
	c.mu.Lock()
	c.cancel()
	c.mu.Unlock()
	c.wg.Wait()
	log.Info("clock is closed")
}

type Clock4Test struct {
	ts uint64
}

// NewClock4Test return a new clock for test.
func NewClock4Test() Clock {
	return &Clock4Test{}
}

func (c *Clock4Test) CurrentTime() time.Time {
	return time.Now()
}

func (c *Clock4Test) CurrentTS() uint64 {
	return oracle.ComposeTS(int64(time.Now().UnixNano()), 0)
}

func (c *Clock4Test) SetTS(ts uint64) {
	c.ts = ts
}

func (c *Clock4Test) Run(ctx context.Context) {
}

func (c *Clock4Test) Close() {
}

type clockWithValue4Test struct {
	value time.Time
}

// NewClockWithValue4Test return a new clock for test.
func NewClockWithValue4Test(value time.Time) *clockWithValue4Test {
	return &clockWithValue4Test{value: value}
}

func (c *clockWithValue4Test) CurrentTime() time.Time {
	return c.value
}

func (c *clockWithValue4Test) CurrentTS() uint64 {
	return oracle.ComposeTS(int64(c.value.UnixNano()), 0)
}

func (c *clockWithValue4Test) Run(ctx context.Context) {
}

func (c *clockWithValue4Test) Close() {
}

type monotonicClock struct {
	clock pclock.Clock
}

// NewMonotonicClock return a new monotonic clock.
func NewMonotonicClock(pClock pclock.Clock) Clock {
	return &monotonicClock{
		clock: pClock,
	}
}

func (c *monotonicClock) CurrentTime() time.Time {
	return c.clock.Now()
}

func (c *monotonicClock) CurrentTS() uint64 {
	return oracle.ComposeTS(c.clock.Now().UnixNano(), 0)
}

func (c *monotonicClock) Run(ctx context.Context) {
}

func (c *monotonicClock) Close() {
}
