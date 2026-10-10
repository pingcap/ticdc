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
	"net/http"
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
	wg        sync.WaitGroup
	reader    reader
	assembler *assembler
	writer    *writer
	decoding  []*writeEvent
}

func newConsumer(ctx context.Context, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) (*consumer, error) {
	memory := &memoryUsage{completed: make(chan struct{}, 1), confirmable: make(chan struct{}, 1)}
	reader, decoding, err := newReader(ctx, upstreamURI, consumerID, timezone, replicaConfig, memory)
	if err != nil {
		return nil, err
	}
	assembler, err := newAssembler(ctx, decoding, replicaConfig, memory)
	if err != nil {
		if closeErr := reader.Close(); closeErr != nil {
			log.Error("consumer reader close failed", zap.Error(closeErr))
		}
		if decoding.upstreamDB != nil {
			_ = decoding.upstreamDB.Close()
		}
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
		if assembler.upstreamDB != nil {
			if closeErr := assembler.upstreamDB.Close(); closeErr != nil {
				log.Error("consumer upstream database close failed", zap.Error(errors.WrapError(errors.ErrMySQLConnectionError, closeErr, "close consumer upstream TiDB")))
			}
		}
		return nil, err
	}
	return &consumer{
		reader: reader, assembler: assembler, writer: &writer{
			downstream: target, memory: memory, mutations: make(map[mutationKey]*writeBatch),
			watermarks: make(map[int64]uint64), progressTick: time.Tick(progressLogInterval),
			ddlJobs: make(chan *writeEvent, 1), ddlDone: make(chan struct{}, 1),
		},
	}, nil
}

func (c *consumer) stop(cancel context.CancelCauseFunc, profileServer *http.Server, err error) error {
	cancel(err)
	if profileServer != nil {
		if closeErr := profileServer.Close(); closeErr != nil {
			log.Error("consumer profiling server close failed", zap.Error(closeErr))
		}
	}
	// Close also wakes sinks whose input channel is blocked while idle.
	c.writer.downstream.Close()
	c.wg.Wait()
	if closeErr := c.reader.Close(); closeErr != nil {
		if err != nil {
			log.Error("consumer reader close failed", zap.Error(closeErr))
		}
		err = cmp.Or(err, closeErr)
	}
	if c.assembler.upstreamDB != nil {
		if closeErr := c.assembler.upstreamDB.Close(); closeErr != nil {
			closeErr = errors.WrapError(errors.ErrMySQLConnectionError, closeErr, "close consumer upstream TiDB")
			if err != nil {
				log.Error("consumer upstream database close failed", zap.Error(closeErr))
			}
			err = cmp.Or(err, closeErr)
		}
	}
	return err
}

func (c *consumer) read(ctx context.Context, results chan<- *writeEvent) error {
	for ctx.Err() == nil {
		result, err := c.assembler.next(ctx, c.reader)
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

func (c *consumer) write(ctx context.Context, results <-chan *writeEvent) error {
	memory := c.writer.memory
	for {
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
			if err := c.collectDecoded(ctx); err != nil {
				return err
			}
		case <-c.writer.ddlDone:
			c.writer.ddlInFlight = nil
			c.writer.progressChanged = true
			if err := c.writer.flushDML(ctx); err != nil {
				return err
			}
		case <-c.writer.progressTick:
			log.Info("consumer progress", zap.Int64("receivedInputs", memory.received.Load()),
				zap.Int64("decodedRows", c.writer.decodedRows), zap.Int64("writtenRows", c.writer.writtenRows),
				zap.Int64("completedInputs", memory.confirmed.Load()), zap.Int("pendingDMLCount", len(c.writer.pendingDML)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int64("uncompletedInputs", memory.received.Load()-memory.confirmed.Load()), zap.Int64("bufferedBytes", memory.used()))
		}
	}
}

func (c *consumer) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.writer.memory.confirmable:
			if err := c.reader.Confirm(ctx); err != nil {
				return err
			}
		}
	}
}

func (c *consumer) executeDDL(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result := <-c.writer.ddlJobs:
			if err := c.writer.writeDDL(ctx, result); err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case c.writer.ddlDone <- struct{}{}:
			}
		}
	}
}

func (c *consumer) consumeBatch(ctx context.Context, results <-chan *writeEvent, result *writeEvent) error {
	items := []*writeEvent{result}
	rows, bytes := int64(0), int64(0)
collect:
	for len(items) < batchRows && rows < batchRows && bytes < batchBytes {
		last := items[len(items)-1]
		bytes += last.bytes
		if last.dml != nil {
			rows += int64(last.dml.Len())
		}
		if rows >= batchRows || bytes >= batchBytes {
			break
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case next, ok := <-results:
			if !ok {
				break collect
			}
			items = append(items, next)
		default:
			break collect
		}
	}
	c.decoding = append(c.decoding, items...)
	return c.collectDecoded(ctx)
}

// Join conversion results in table order. A slow table does not hold up other
// tables, but controls cannot pass unfinished conversions in their scope.
func (c *consumer) collectDecoded(ctx context.Context) error {
	var (
		items           []*writeEvent
		blockedRows     []*writeEvent
		blockedControls []*writeEvent
		blockedTables   map[int64]bool
	)
	remaining := c.decoding[:0]
	for _, item := range c.decoding {
		ready := true
		if item.decoding != nil {
			select {
			case <-item.decoding.done:
			default:
				ready = false
			}
		}
		if ready && len(remaining) != 0 {
			ready = !c.assembler.mergeRows && (item.dml == nil || !blockedTables[item.dml.PhysicalTableID])
			for _, prior := range blockedControls {
				if item.hasWatermark || (prior.ddl != nil && (item.dml == nil || ddlBlocksTable(prior.ddl, item.dml))) {
					ready = false
					break
				}
			}
			// Ordinary DML only checks its table and the few pending controls.
			// Scan row scopes when a control itself needs a completion barrier.
			if item.dml == nil {
				for _, prior := range blockedRows {
					if (item.ddl != nil && ddlBlocksTable(item.ddl, prior.dml)) ||
						(item.hasWatermark && prior.dml.CommitTs <= item.watermark && (item.tableID == 0 || prior.dml.PhysicalTableID == item.tableID)) {
						ready = false
						break
					}
				}
			}
		}
		if !ready {
			if item.dml != nil {
				if blockedTables == nil {
					blockedTables = make(map[int64]bool)
				}
				blockedTables[item.dml.PhysicalTableID] = true
				blockedRows = append(blockedRows, item)
			} else {
				blockedControls = append(blockedControls, item)
			}
			remaining = append(remaining, item)
			continue
		}
		if task := item.decoding; task != nil {
			task.dml.AddPostFlushFunc(item.dml.PostFlush)
			item.dml, item.bytes, item.decoding = task.dml, item.bytes+task.bytes, nil
		}
		items = append(items, item)
	}
	clear(c.decoding[len(remaining):])
	c.decoding = remaining
	items, err := c.assembler.prepare(ctx, items)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := c.writer.consume(ctx, item); err != nil {
			return err
		}
	}
	return c.writer.flushDML(ctx)
}
