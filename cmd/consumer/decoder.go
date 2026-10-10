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
	inputs []chan *decodeTask
	memory *memoryUsage
}

type decodeTask struct {
	message       *codecCommon.DMLMessage
	records       []*ack
	dml           *event.DMLEvent
	bytes         int64
	done          chan struct{}
	table         *common.TableInfo
	tableID       int64
	retainedBytes int64
}

func newDecoderGroup(memory *memoryUsage) *decoderGroup {
	inputs := make([]chan *decodeTask, runtime.GOMAXPROCS(0))
	for i := range inputs {
		inputs[i] = make(chan *decodeTask, 64)
	}
	return &decoderGroup{inputs: inputs, memory: memory}
}

func (g *decoderGroup) submit(ctx context.Context, task *decodeTask) error {
	for _, record := range task.records {
		record.decodes.Inc()
		record.refs.Inc()
	}
	// A table keeps conversion order as well as write order. Different tables
	// share CPU workers, independently of their upstream partition placement.
	index := uint64(task.message.TableID) % uint64(len(g.inputs))
	select {
	case g.inputs[index] <- task:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (g *decoderGroup) decode(ctx context.Context, input <-chan *decodeTask) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case task := <-input:
			if err := context.Cause(ctx); err != nil {
				return err
			}
			dml := task.message.ToDMLEvent()
			if dml == nil || dml.TableInfo == nil || dml.Rows == nil || dml.Len() == 0 {
				return errors.ErrCodecDecode.FastGenByArgs("DML cannot be materialized into nonempty rows with table metadata")
			}
			if task.table != nil {
				dml.PhysicalTableID = task.tableID
				if dml.TableInfo != task.table {
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
			for _, record := range task.records {
				g.memory.decoded(record, task.retainedBytes)
			}
			task.message, task.records, task.table = nil, nil, nil
			close(task.done)
			select {
			case g.memory.completed <- struct{}{}:
			default:
			}
		}
	}
}
