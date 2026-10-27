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

// Protocol cursors and schema updates remain with the source parser. Workers only
// use captured messages; their number does not depend on input partitions.
type decoderGroup struct {
	g         *errgroup.Group
	input     chan *decodeTask
	workers   int
	memory    *memoryUsage
	completed chan struct{}
}

type decodeTask struct {
	commitTs uint64
	stream   int32
	boundary *readBoundary
	waiting  bool
	message  *codecCommon.DMLMessage
	name     event.SchemaTableName
	record   *ack
	dml      *event.DMLEvent
	bytes    int64
	done     chan struct{}
	table    *common.TableInfo
	tableID  int64
}

func newDecoderGroup(memory *memoryUsage, completed chan struct{}) *decoderGroup {
	workers := runtime.GOMAXPROCS(0)
	return &decoderGroup{
		input:     make(chan *decodeTask, workers*64),
		workers:   workers,
		memory:    memory,
		completed: completed,
	}
}

func (d *decoderGroup) submit(ctx context.Context, task *decodeTask) error {
	task.record.retain()
	// Conversion can finish out of order. The conductor joins results in table
	// order before allowing controls or writes to pass them.
	select {
	case d.input <- task:
		return nil
	default:
	}
	select {
	case d.input <- task:
		return nil
	case <-ctx.Done():
		task.record.release()
		return context.Cause(ctx)
	}
}

func (d *decoderGroup) start(ctx context.Context) error {
	d.g, ctx = errgroup.WithContext(ctx)
	for range d.workers {
		d.g.Go(func() error {
			return d.decodeRows(ctx)
		})
	}
	// The caller cancels shared input resources before close joins workers.
	<-ctx.Done()
	return context.Cause(ctx)
}

func (d *decoderGroup) close() {
	if d.g != nil {
		_ = d.g.Wait()
	}
}

func (d *decoderGroup) decodeRows(ctx context.Context) error {
	for {
		var task *decodeTask
		select {
		case task = <-d.input:
		default:
			// Buffered work only takes the input-channel lock. A blocking
			// receive also listens for cancellation when the queue is idle.
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case task = <-d.input:
			}
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
		bytes := dml.Rows.MemoryUsage()
		d.memory.reserve(bytes)
		task.dml, task.bytes = dml, bytes
		task.record.release()
		task.message, task.table = nil, nil
		close(task.done)
		select {
		case d.completed <- struct{}{}:
		default:
		}
	}
}
