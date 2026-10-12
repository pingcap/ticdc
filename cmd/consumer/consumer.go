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
	wg          sync.WaitGroup
	reader      reader
	assembler   *assembler
	writer      *writer
	progress    *inputProgress
	completed   chan struct{}
	confirmable chan struct{}
}

func newConsumer(ctx context.Context, upstreamURI *url.URL, downstreamURI, consumerID, timezone string, replicaConfig *config.ReplicaConfig) (c *consumer, err error) {
	memory := &memoryUsage{}
	progress := &inputProgress{memory: memory}
	schemas := &schemaUsage{memory: memory}
	completed := make(chan struct{}, 1)
	confirmable := make(chan struct{}, 1)
	reader, decoding, err := newReader(ctx, upstreamURI, consumerID, timezone, replicaConfig, progress, schemas, confirmable)
	if err != nil {
		return nil, err
	}
	defer func() {
		if c != nil {
			return
		}
		if closeErr := reader.Close(); closeErr != nil {
			log.Error("consumer reader close failed", zap.Error(closeErr))
		}
		if decoding.upstreamDB != nil {
			if closeErr := decoding.upstreamDB.Close(); closeErr != nil {
				log.Error("consumer upstream database close failed", zap.Error(errors.WrapError(errors.ErrMySQLConnectionError, closeErr, "close consumer upstream TiDB")))
			}
		}
	}()
	assembler, err := newAssembler(ctx, decoding, replicaConfig, progress, schemas, completed)
	if err != nil {
		return nil, err
	}
	replicaConfig.Sink.TiDBSourceID = 1
	changefeedID := common.NewChangeFeedIDWithName("consumer", common.DefaultKeyspaceName)
	target, err := sink.New(ctx, &config.ChangefeedConfig{
		ChangefeedID:           changefeedID,
		SinkURI:                downstreamURI,
		SinkConfig:             replicaConfig.Sink,
		CaseSensitive:          putil.GetOrZero(replicaConfig.CaseSensitive),
		EnableTableAcrossNodes: putil.GetOrZero(replicaConfig.Scheduler.EnableTableAcrossNodes),
	}, changefeedID, common.DefaultKeyspaceID)
	if err != nil {
		return nil, err
	}
	return &consumer{
		reader:      reader,
		assembler:   assembler,
		progress:    progress,
		completed:   completed,
		confirmable: confirmable,
		writer: &writer{
			downstream:   target,
			memory:       memory,
			progress:     progress,
			completed:    completed,
			mutations:    make(map[mutationKey]*writeBatch),
			progressTick: time.Tick(progressLogInterval),
			ddlJobs:      make(chan *writeEvent, 1),
			ddlDone:      make(chan struct{}, 1),
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
	progress := c.progress
	for {
		var (
			items []*writeEvent
			err   error
		)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case result, ok := <-results:
			if !ok {
				return context.Cause(ctx)
			}
			items, err = c.assembler.assemble(ctx, results, result)
		case <-c.completed:
			items, err = c.assembler.collectDecoded(ctx)
		case <-c.writer.progressTick:
			log.Info("consumer progress", zap.Int64("receivedInputs", progress.received.Load()),
				zap.Int64("decodedRows", c.writer.decodedRows), zap.Int64("writtenRows", c.writer.writtenRows),
				zap.Int64("completedInputs", progress.confirmed.Load()), zap.Int("pendingDMLCount", len(c.writer.pendingDML)),
				zap.Int("inFlightBatches", len(c.writer.inFlight)), zap.Int64("inFlightBytes", c.writer.inFlightBytes),
				zap.Int64("uncompletedInputs", progress.received.Load()-progress.confirmed.Load()), zap.Int64("bufferedBytes", progress.memory.used()))
			continue
		}
		if err != nil {
			return err
		}
		for _, item := range items {
			if err := c.writer.consume(ctx, item); err != nil {
				return err
			}
		}
		if err := c.writer.flushDML(ctx); err != nil {
			return err
		}
	}
}

func (c *consumer) confirm(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-c.confirmable:
			if err := c.reader.Confirm(ctx); err != nil {
				return err
			}
		}
	}
}
