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
	"runtime"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Protocol cursors and schema updates remain with the source decoder. Workers only
// use captured messages; their number does not depend on input partitions.
type decoderGroup struct {
	input     chan *decodeTask
	workers   int
	memory    *memoryBudget
	completed chan struct{}
	validator dmlValidator
}

type decodeTask struct {
	commitTs          uint64
	stream            int32
	offset            int64 // Kafka record offset for source-side validation.
	skipKeyOnlyDelete bool
	boundary          *readBoundary
	message           *codecCommon.DMLMessage
	name              event.SchemaTableName
	dml               *event.DMLEvent
	bytes             int64
	done              chan struct{}
	table             *common.TableInfo
	tableID           int64
}

type dmlValidator interface {
	validateDML(task *decodeTask, dml *event.DMLEvent)
}

func newDecoderGroup(memory *memoryBudget, completed chan struct{}, validator dmlValidator) *decoderGroup {
	workers := runtime.GOMAXPROCS(0)
	return &decoderGroup{
		input:     make(chan *decodeTask, workers*64),
		workers:   workers,
		memory:    memory,
		completed: completed,
		validator: validator,
	}
}

func (d *decoderGroup) submit(ctx context.Context, task *decodeTask) error {
	// Conversion can finish out of order. The conductor joins results in table
	// order before allowing controls or writes to pass them.
	select {
	case d.input <- task:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (d *decoderGroup) start(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for range d.workers {
		g.Go(func() error {
			return d.decodeRows(ctx)
		})
	}
	<-ctx.Done()
	return g.Wait()
}

func (d *decoderGroup) decodeRows(ctx context.Context) error {
	for {
		var task *decodeTask
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case task = <-d.input:
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		dml := task.message.ToDMLEvent()
		if dml == nil {
			log.Panic("consumer DML conversion failed", zap.String("schema", task.message.Schema), zap.String("table", task.message.Table), zap.Uint64("commitTs", task.message.GetCommitTs()))
		}
		if task.table != nil {
			dml.PhysicalTableID = task.tableID
			if dml.TableInfo.UpdateTS != task.table.UpdateTS {
				// A CSV column selection may share this metadata with other
				// conversions. Keep the captured schema immutable.
				dml.TableInfo = dml.TableInfo.CloneWithRouting(dml.TableInfo.GetTargetSchemaName(), dml.TableInfo.GetTargetTableName())
				dml.TableInfo.UpdateTS = task.table.UpdateTS
			}
		}
		if d.validator != nil {
			d.validator.validateDML(task, dml)
		}
		bytes := dml.Rows.MemoryUsage()
		d.memory.ForceAcquire(bytes)
		capturedBytes := task.bytes
		task.dml, task.bytes = dml, bytes
		task.message, task.table = nil, nil
		d.memory.Release(capturedBytes)
		close(task.done)
		select {
		case d.completed <- struct{}{}:
		default:
		}
	}
}
