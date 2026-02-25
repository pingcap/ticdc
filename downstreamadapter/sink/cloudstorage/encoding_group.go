// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package cloudstorage

import (
	"context"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/sink/failpointrecord"
	"github.com/pingcap/ticdc/utils/chann"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

type encodingGroup struct {
	changeFeedID commonType.ChangeFeedID
	codecConfig  *common.Config

	concurrency int

	inputCh  *chann.UnlimitedChannel[eventFragment, any]
	outputCh chan<- eventFragment

	closed *atomic.Bool
}

func newEncodingGroup(
	changefeedID commonType.ChangeFeedID,
	codecConfig *common.Config,
	concurrency int,
	inputCh *chann.UnlimitedChannel[eventFragment, any],
	outputCh chan<- eventFragment,
) *encodingGroup {
	return &encodingGroup{
		changeFeedID: changefeedID,
		codecConfig:  codecConfig,
		concurrency:  concurrency,
		inputCh:      inputCh,
		outputCh:     outputCh,

		closed: atomic.NewBool(false),
	}
}

func (eg *encodingGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < eg.concurrency; i++ {
		g.Go(func() error {
			return eg.runEncoder(ctx)
		})
	}
	return g.Wait()
}

func (eg *encodingGroup) runEncoder(ctx context.Context) error {
	encoder, err := codec.NewTxnEventEncoder(eg.codecConfig)
	if err != nil {
		return err
	}
	defer eg.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		default:
			frag, ok := eg.inputCh.Get()
			if !ok || eg.closed.Load() {
				return nil
			}
			err = encoder.AppendTxnEvent(frag.event)
			if err != nil {
				return err
			}
			frag.encodedMsgs = encoder.Build()
			// Global switch for cloudstorage sink message failpoints.
			// Usage:
			//   failpoint.Enable(".../cloudStorageSinkMessageFailpointSwitch", "return(false)") // disable
			//   failpoint.Enable(".../cloudStorageSinkMessageFailpointSwitch", "return(true)")  // enable
			failpoint.Inject("cloudStorageSinkMessageFailpointSwitch", func(val failpoint.Value) {
				if enabled, ok := val.(bool); ok && enabled {
					eg.applyFailpointsOnEncodedMessages(frag)
				}
			})

			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case eg.outputCh <- frag:
			}
		}
	}
}

func (eg *encodingGroup) close() {
	eg.closed.Store(true)
}

func (eg *encodingGroup) applyFailpointsOnEncodedMessages(frag eventFragment) {
	rowRecordsByMsg := splitRowRecordsByMessages(frag.encodedMsgs, dmlEventToRowRecords(frag.event))
	for idx, msg := range frag.encodedMsgs {
		var rowRecords []failpointrecord.RowRecord
		if idx < len(rowRecordsByMsg) {
			rowRecords = rowRecordsByMsg[idx]
		}
		failpoint.Inject("cloudStorageSinkDropMessage", func() {
			log.Warn("cloudStorageSinkDropMessage: dropping message to simulate data loss",
				zap.String("keyspace", eg.changeFeedID.Keyspace()),
				zap.Stringer("changefeed", eg.changeFeedID.ID()),
				zap.Any("rows", rowRecords))
			failpointrecord.Write("cloudStorageSinkDropMessage", rowRecords)
			// Keep callback flow unchanged while dropping data payload.
			msg.Key = nil
			msg.Value = nil
			msg.SetRowsCount(0)
			failpoint.Continue()
		})
		failpoint.Inject("cloudStorageSinkMutateValue", func() {
			log.Warn("cloudStorageSinkMutateValue: mutating message value to simulate data inconsistency",
				zap.String("keyspace", eg.changeFeedID.Keyspace()),
				zap.Stringer("changefeed", eg.changeFeedID.ID()),
				zap.Any("rows", rowRecords))
			mutatedRows, originTsMutatedRows := mutateMessageValueForFailpoint(msg, rowRecords)
			if len(mutatedRows) > 0 {
				failpointrecord.Write("cloudStorageSinkMutateValue", mutatedRows)
			}
			if len(originTsMutatedRows) > 0 {
				failpointrecord.Write("cloudStorageSinkMutateValueTidbOriginTs", originTsMutatedRows)
			}
		})
	}
}

func splitRowRecordsByMessages(messages []*common.Message, rows []failpointrecord.RowRecord) [][]failpointrecord.RowRecord {
	if len(messages) == 0 {
		return nil
	}
	ret := make([][]failpointrecord.RowRecord, 0, len(messages))
	rowIdx := 0
	for _, msg := range messages {
		rowsNeeded := msg.GetRowsCount()
		if rowsNeeded <= 0 || rowIdx >= len(rows) {
			ret = append(ret, nil)
			continue
		}
		end := rowIdx + rowsNeeded
		if end > len(rows) {
			end = len(rows)
		}
		ret = append(ret, rows[rowIdx:end])
		rowIdx = end
	}
	return ret
}

func dmlEventToRowRecords(event *commonEvent.DMLEvent) []failpointrecord.RowRecord {
	if event == nil || event.TableInfo == nil {
		return nil
	}
	indexes, columns := (&commonEvent.RowEvent{TableInfo: event.TableInfo}).PrimaryKeyColumn()
	originTsCol, hasOriginTsCol := event.TableInfo.GetColumnInfoByName(commonEvent.OriginTsColumn)
	originTsOffset, hasOriginTsOffset := event.TableInfo.GetColumnOffsetByName(commonEvent.OriginTsColumn)
	rowRecords := make([]failpointrecord.RowRecord, 0, event.Len())
	for {
		row, ok := event.GetNextRow()
		if !ok {
			event.Rewind()
			break
		}
		rowData := row.Row
		if row.RowType == commonType.RowTypeDelete {
			rowData = row.PreRow
		}

		pks := make(map[string]any, len(columns))
		for i, col := range columns {
			if col == nil {
				continue
			}
			pks[col.Name.String()] = commonType.ExtractColVal(&rowData, col, indexes[i])
		}
		originTs := uint64(0)
		if hasOriginTsCol && hasOriginTsOffset {
			originTs = failpointrecord.NormalizeOriginTs(
				commonType.ExtractColVal(&rowData, originTsCol, originTsOffset),
			)
		}
		rowRecords = append(rowRecords, failpointrecord.RowRecord{
			CommitTs:    event.CommitTs,
			OriginTs:    originTs,
			PrimaryKeys: pks,
		})
	}
	return rowRecords
}
