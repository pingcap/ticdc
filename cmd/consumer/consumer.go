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

package main

import (
	"cmp"
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

type consumer struct {
	reader            reader
	writer            *writer
	pendingWatermarks []*readResult
	watermarks        map[int64]uint64
}

func newConsumer(ctx context.Context, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) (*consumer, error) {
	memory := &memoryUsage{completed: make(chan struct{}, 1)}
	reader, err := newReader(ctx, upstreamURI, consumerID, timezone, replicaConfig, memory)
	if err != nil {
		return nil, err
	}
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	target, err := sink.New(ctx, &config.ChangefeedConfig{
		ChangefeedID: changefeedID, SinkURI: downstreamURI, SinkConfig: replicaConfig.Sink,
		CaseSensitive: putil.GetOrZero(replicaConfig.CaseSensitive), EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			log.Error("consumer reader close failed", zap.Error(closeErr))
		}
		return nil, err
	}
	return &consumer{
		reader: reader, writer: &writer{downstream: target, memory: memory, mutations: make(map[mutationKey]*writeBatch), serialDML: putil.GetOrZero(replicaConfig.Sink.Protocol) == "csv"},
		watermarks: make(map[int64]uint64),
	}, nil
}

func (c *consumer) start(ctx context.Context) (err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	var wg sync.WaitGroup
	defer func() {
		cancel(err)
		// Close also wakes sinks whose input channel is blocked while idle.
		c.writer.downstream.Close()
		wg.Wait()
		if closeErr := c.reader.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				log.Error("consumer reader close failed", zap.Error(closeErr))
			}
		}
	}()
	results := make(chan *readResult, 64)
	wg.Go(func() {
		defer close(results)
		cancel(c.read(ctx, results))
	})
	wg.Go(func() {
		if err := c.writer.downstream.Run(ctx); err != nil {
			cancel(err)
			return
		}
		if ctx.Err() == nil {
			cancel(errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly"))
		}
	})
	err = c.write(ctx, results)
	return cmp.Or(context.Cause(ctx), err)
}

func (c *consumer) read(ctx context.Context, results chan<- *readResult) error {
	for ctx.Err() == nil {
		result, err := c.reader.Read(ctx)
		if err != nil {
			return err
		}
		if result == nil {
			return errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an empty result")
		}
		select {
		case results <- result:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return context.Cause(ctx)
}

func (c *consumer) write(ctx context.Context, results <-chan *readResult) error {
	memory := c.writer.memory
	tick := time.Tick(progressLogInterval)
	for {
		c.writer.finishBatches()
		if err := c.confirmCompleted(ctx); err != nil {
			return err
		}
		if time.Since(c.writer.lastProgressLog) >= progressLogInterval {
			c.writer.lastProgressLog = time.Now()
			log.Info("consumer progress", zap.Int64("receivedInputs", memory.received.Load()),
				zap.Int64("decodedRows", c.writer.decodedRows), zap.Int64("writtenRows", c.writer.writtenRows),
				zap.Int64("completedInputs", memory.confirmed.Load()), zap.Int("pendingDMLCount", len(c.writer.pendingDML)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int64("uncompletedInputs", memory.received.Load()-memory.confirmed.Load()), zap.Int64("bufferedBytes", memory.used()))
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result, ok := <-results:
			if !ok {
				return context.Cause(ctx)
			}
			if err := c.consumeBatch(ctx, results, result); err != nil {
				return err
			}
		case <-memory.completed:
		case <-tick:
		}
	}
}

func (c *consumer) consumeBatch(ctx context.Context, results <-chan *readResult, result *readResult) error {
	inputs := 0
	for {
		if err := c.consume(ctx, result); err != nil {
			return err
		}
		inputs++
		if inputs >= cap(results) {
			return c.flushDML(ctx, nil)
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case next, ok := <-results:
			if !ok {
				return c.flushDML(ctx, nil)
			}
			result = next
		default:
			return c.flushDML(ctx, nil)
		}
	}
}

func (c *consumer) consume(ctx context.Context, result *readResult) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if result.dml != nil {
		c.writer.pendingDML = append(c.writer.pendingDML, result)
		c.writer.decodedRows += int64(result.dml.Len())
	}
	if result.ddl != nil {
		if err := c.writeDDL(ctx, result); err != nil {
			return err
		}
	}
	if result.hasWatermark {
		c.watermarks[result.tableID] = max(c.watermarks[result.tableID], result.watermark)
		if result.onFlush != nil {
			c.pendingWatermarks = append(c.pendingWatermarks, result)
		}
	}
	return nil
}

func (c *consumer) confirmCompleted(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	remaining := c.pendingWatermarks[:0]
	for _, control := range c.pendingWatermarks {
		blocked := false
		for _, item := range c.writer.pendingDML {
			if (control.tableID == 0 || item.dml.PhysicalTableID == control.tableID) && item.dml.CommitTs <= control.watermark {
				blocked = true
				break
			}
		}
		for _, batch := range c.writer.inFlight {
			for _, item := range batch.items {
				if (control.tableID == 0 || item.dml.PhysicalTableID == control.tableID) && item.dml.CommitTs <= control.watermark {
					blocked = true
					break
				}
			}
		}
		if blocked {
			remaining = append(remaining, control)
			continue
		}
		control.onFlush()
	}
	clear(c.pendingWatermarks[len(remaining):])
	c.pendingWatermarks = remaining
	if err := c.reader.Confirm(ctx); err != nil {
		return err
	}
	for tableID, watermark := range c.watermarks {
		c.writer.advanceReplay(watermark, tableID)
	}
	return nil
}
