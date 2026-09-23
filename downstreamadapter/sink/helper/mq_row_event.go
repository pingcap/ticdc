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
	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
)

func NewMQRowEvents(
	event *commonEvent.DMLEvent,
	topic string,
	partitionNum int32,
	partitionGenerator partition.Generator,
	selector commonEvent.Selector,
) ([]*commonEvent.MQRowEvent, error) {
	callback := NewPostFlushRowCallback(event, uint64(event.Len()))
	events := make([]*commonEvent.MQRowEvent, 0, event.Len())
	if selector == nil {
		selector = columnselector.NewDefaultColumnSelector()
	}

	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}

		index, key, err := partitionGenerator.GeneratePartitionIndexAndKey(
			&row, partitionNum, event.TableInfo, event.CommitTs)
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
			RowEvent: commonEvent.RowEvent{
				Snapshot:        snapshotRow(event, row),
				PhysicalTableID: event.PhysicalTableID,
				TableInfo:       event.TableInfo,
				StartTs:         event.StartTs,
				CommitTs:        event.CommitTs,
				Event:           row,
				Callback:        callback,
				ColumnSelector:  selector,
				Checksum:        row.Checksum,
			},
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
			Snapshot:        snapshotRow(event, row),
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

// A mounted snapshot batch shares generation metadata, but every row has its
// own identity. Never copy a batch-level record ID to all rows in the batch.
func snapshotRow(event *commonEvent.DMLEvent, row commonEvent.RowChange) *commonEvent.SnapshotRow {
	if event.Snapshot == nil {
		return nil
	}
	return &commonEvent.SnapshotRow{
		ID:         protocol.RecordID(event.Snapshot.SnapshotID, event.PhysicalTableID, row.RowKey),
		SnapshotID: event.Snapshot.SnapshotID,
		Timestamp:  event.Snapshot.Timestamp,
	}
}
