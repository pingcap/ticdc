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
	"context"
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

func runConsumer(parentCtx context.Context, wg *sync.WaitGroup, input reader, downstreamURI string, replicaConfig *config.ReplicaConfig, memory *bufferUsage) (err error) {
	readCtx, cancelRead := context.WithCancelCause(parentCtx)
	defer cancelRead(nil)
	writeCtx, cancelWrite := context.WithCancelCause(context.WithoutCancel(parentCtx))
	defer cancelWrite(nil)
	defer func() {
		if closeErr := input.Close(); closeErr != nil {
			if err == nil {
				err = closeErr
			} else {
				log.Error("consumer reader close failed", zap.Error(closeErr))
			}
		}
	}()
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	downstream, err := sink.New(writeCtx, &config.ChangefeedConfig{
		ChangefeedID: changefeedID, SinkURI: downstreamURI, SinkConfig: replicaConfig.Sink,
		CaseSensitive: putil.GetOrZero(replicaConfig.CaseSensitive), EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		return err
	}
	c := &consumer{
		reader: input, writer: &writer{downstream: downstream, memory: memory, mutations: make(map[mutationKey]*writeBatch)},
		watermarks: make(map[int64]uint64),
	}
	c.writer.confirm = c.confirmCompleted
	memory.completed = make(chan struct{}, 1)
	results := make(chan *readResult, 64)
	readDone, sinkDone := make(chan bool), make(chan bool)
	wg.Go(func() {
		defer close(readDone)
		defer close(results)
		for {
			result, err := input.Read(readCtx)
			if err != nil {
				cancelRead(err)
				return
			}
			if result == nil {
				cancelRead(errors.ErrInternalCheckFailed.FastGenByArgs("reader returned an empty result"))
				return
			}
			select {
			case results <- result:
			case <-readCtx.Done():
				return
			}
		}
	})
	wg.Go(func() {
		defer close(sinkDone)
		if err := downstream.Run(writeCtx); err != nil {
			cancelWrite(err)
			cancelRead(err)
			return
		}
		if writeCtx.Err() == nil {
			err := errors.ErrInternalCheckFailed.FastGenByArgs("downstream sink stopped unexpectedly")
			cancelWrite(err)
			cancelRead(err)
		}
	})
	wg.Go(func() {
		select {
		case <-parentCtx.Done():
			log.Info("consumer stopping", zap.Duration("shutdownTimeout", shutdownTimeout))
		case <-writeCtx.Done():
			return
		}
		timer := time.NewTimer(shutdownTimeout)
		defer timer.Stop()
		select {
		case <-writeCtx.Done():
		case <-timer.C:
			cancelWrite(errors.ErrInternalCheckFailed.FastGenByArgs("consumer shutdown drain timed out"))
		}
	})
	defer func() {
		cancelRead(nil)
		<-readDone
		cancelWrite(nil)
		// Close also wakes sinks whose input channel is blocked while idle.
		downstream.Close()
		<-sinkDone
	}()
	tick := time.Tick(progressLogInterval)
	var pendingDDL *readResult
run:
	for {
		c.writer.finishBatches()
		if err = c.confirmCompleted(readCtx); err != nil {
			break
		}
		if time.Since(c.writer.lastProgressLog) >= progressLogInterval {
			c.writer.lastProgressLog = time.Now()
			log.Info("consumer progress", zap.Int64("receivedInputs", memory.received.Load()),
				zap.Int64("decodedRows", c.writer.decodedRows), zap.Int64("writtenRows", c.writer.writtenRows),
				zap.Int64("completedInputs", memory.confirmed.Load()), zap.Int("pendingDMLCount", len(c.writer.pendingDML)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int64("uncompletedInputs", memory.records.Load()), zap.Int64("readerBufferedBytes", input.BufferedBytes()),
				zap.Int64("bufferedBytes", c.writer.bufferedBytes()))
		}
		select {
		case <-readCtx.Done():
			err = context.Cause(readCtx)
			break run
		case result, ok := <-results:
			if !ok {
				err = context.Cause(readCtx)
				break run
			}
			rows, bytes, inputs := int64(0), int64(0), 0
		drain:
			for {
				if result.ddl != nil {
					pendingDDL = result
				}
				if err = c.consume(readCtx, writeCtx, result); err != nil {
					break run
				}
				pendingDDL = nil
				inputs++
				if result.dml != nil {
					rows += int64(result.dml.Len())
					bytes += result.bytes
				}
				if rows >= batchRows || bytes >= batchBytes || inputs >= cap(results) {
					break
				}
				select {
				case <-readCtx.Done():
					err = context.Cause(readCtx)
					break run
				case result, ok = <-results:
					if !ok {
						break drain
					}
				default:
					break drain
				}
			}
			if err = c.writer.flushDML(readCtx, writeCtx); err != nil {
				break run
			}
		case <-memory.completed:
		case <-tick:
		}
	}
	if writeErr := context.Cause(writeCtx); writeErr != nil {
		return writeErr
	}
	if parentCtx.Err() == nil || !errors.Is(err, parentCtx.Err()) {
		return err
	}
	cancelRead(nil)
	<-readDone
	drainCtx, cancelDrain := context.WithTimeout(writeCtx, shutdownTimeout)
	defer cancelDrain()
	if pendingDDL != nil {
		if err := c.writer.writeDDL(drainCtx, writeCtx, pendingDDL); err != nil {
			return err
		}
	}
	// The reader has stopped registering effects. Drain only results it handed
	// over, leaving incomplete decoding or ordering groups unconfirmed.
	for result := range results {
		if err := c.consume(drainCtx, writeCtx, result); err != nil {
			return err
		}
	}
	if err := c.writer.flushDML(drainCtx, writeCtx); err != nil {
		return err
	}
	for len(c.writer.inFlight) != 0 {
		if err := c.writer.waitBatch(drainCtx, c.writer.inFlight[0]); err != nil {
			return err
		}
	}
	if err := c.confirmCompleted(drainCtx); err != nil {
		return err
	}
	return context.Cause(writeCtx)
}

func (c *consumer) consume(ctx, writeCtx context.Context, result *readResult) error {
	if result.dml != nil {
		c.writer.pendingDML = append(c.writer.pendingDML, &pendingDML{event: result.dml, bytes: result.bytes})
		c.writer.dmlBytes += result.bytes
		c.writer.decodedRows += int64(result.dml.Len())
	}
	if result.ddl != nil {
		if err := c.writer.writeDDL(ctx, writeCtx, result); err != nil {
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
	remaining := c.pendingWatermarks[:0]
	for _, control := range c.pendingWatermarks {
		blocked := false
		for _, item := range c.writer.pendingDML {
			if (control.tableID == 0 || item.event.PhysicalTableID == control.tableID) && item.event.CommitTs <= control.watermark {
				blocked = true
				break
			}
		}
		for _, batch := range c.writer.inFlight {
			for _, item := range batch.items {
				if (control.tableID == 0 || item.event.PhysicalTableID == control.tableID) && item.event.CommitTs <= control.watermark {
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
