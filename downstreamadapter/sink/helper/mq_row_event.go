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
// See the License for the specific language governing permissions and
// limitations under the License.

package helper

import (
	"github.com/pingcap/ticdc/downstreamadapter/sink/columnselector"
	"github.com/pingcap/ticdc/downstreamadapter/sink/eventrouter/partition"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
)

func NewMQRowEvents(
	event *commonEvent.DMLEvent,
	topic string,
	partitionNum int32,
	partitionGenerator partition.Generator,
	selector commonEvent.Selector,
) ([]*commonEvent.MQRowEvent, error) {
	rowEvents := NewRowEvents(event, selector, NewTxnPostFlushRowCallback(event, uint64(event.Len())))
	events := make([]*commonEvent.MQRowEvent, 0, len(rowEvents))

	for _, rowEvent := range rowEvents {
		index, key, err := partitionGenerator.GeneratePartitionIndexAndKey(
			&rowEvent.Event, partitionNum, event.TableInfo, event.CommitTs)
		if err != nil {
			return nil, err
		}

		events = append(events, &commonEvent.MQRowEvent{
			Key: commonEvent.TopicPartitionKey{
				Topic:          topic,
				Partition:      index,
				PartitionKey:   key,
				TotalPartition: partitionNum,
			},
			RowEvent: *rowEvent,
		})
	}
	return events, nil
}

func NewRowEvents(
	event *commonEvent.DMLEvent,
	selector commonEvent.Selector,
	callback func(),
) []*commonEvent.RowEvent {
	if selector == nil {
		selector = columnselector.NewDefaultColumnSelector()
	}

	events := make([]*commonEvent.RowEvent, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}

		events = append(events, &commonEvent.RowEvent{
			PhysicalTableID: event.PhysicalTableID,
			TableInfo:       event.TableInfo,
			StartTs:         event.StartTs,
			CommitTs:        event.CommitTs,
			Event:           row,
			Callback:        callback,
			ColumnSelector:  selector,
			Checksum:        row.Checksum,
		})
	}
	return events
}
