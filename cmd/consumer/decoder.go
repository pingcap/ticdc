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

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	codecCommon "github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// Protocol cursors and schema updates remain with the assembler. Workers only
// use captured messages; their number does not depend on input partitions.
type decoderGroup struct {
	input     chan *decodeTask
	workers   int
	memory    *memoryUsage
	completed chan<- struct{}
}

type decodeTask struct {
	message       *codecCommon.DMLMessage
	record        *ack
	dml           *event.DMLEvent
	bytes         int64
	done          chan struct{}
	table         *common.TableInfo
	tableID       int64
	retainedBytes int64
}

func newDecoderGroup(memory *memoryUsage, completed chan<- struct{}) *decoderGroup {
	workers := runtime.GOMAXPROCS(0)
	return &decoderGroup{
		input:     make(chan *decodeTask, workers*64),
		workers:   workers,
		memory:    memory,
		completed: completed,
	}
}

func (g *decoderGroup) submit(ctx context.Context, task *decodeTask) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	task.record.retainDecode()
	// Conversion can finish out of order. The assembler joins results in table
	// order before allowing controls or writes to pass them.
	select {
	case g.input <- task:
		return nil
	default:
	}
	select {
	case g.input <- task:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (g *decoderGroup) decode(ctx context.Context) error {
	for {
		var task *decodeTask
		select {
		case task = <-g.input:
		default:
			// Buffered work only takes the input-channel lock. A blocking
			// receive also listens for cancellation when the queue is idle.
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case task = <-g.input:
			}
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		dml := task.message.ToDMLEvent()
		if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
			return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
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
		if err := g.memory.retainSchema(ctx, dml.TableInfo); err != nil {
			return err
		}
		bytes := dml.Rows.MemoryUsage() + int64(len(dml.RowTypes))*64
		if err := g.memory.reserve(ctx, bytes); err != nil {
			return err
		}
		dml.AddPostFlushFunc(func() { g.memory.releaseSchema(dml.TableInfo) })
		task.dml, task.bytes = dml, bytes
		g.memory.decoded(task.record, task.retainedBytes)
		task.message, task.record, task.table = nil, nil, nil
		close(task.done)
		select {
		case g.completed <- struct{}{}:
		default:
		}
	}
}
