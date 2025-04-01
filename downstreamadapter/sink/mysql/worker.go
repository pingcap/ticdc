// Copyright 2024 PingCAP, Inc.
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

package mysql

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/ticdc/pkg/sink/util"
	"go.uber.org/zap"
)

// dmlWorker is used to flush the dml event downstream
type dmlWorker struct {
	changefeedID common.ChangeFeedID

	eventChan   <-chan *commonEvent.DMLEvent
	mysqlWriter *mysql.Writer
	id          int

	maxRows int
}

func NewMysqlDMLWorker(
	ctx context.Context,
	db *sql.DB,
	config *mysql.MysqlConfig,
	id int,
	changefeedID common.ChangeFeedID,
	statistics *metrics.Statistics,
	formatVectorType bool,
	eventChan <-chan *commonEvent.DMLEvent,
) *dmlWorker {
	return &dmlWorker{
		mysqlWriter:  mysql.NewMysqlWriter(ctx, db, config, changefeedID, statistics, formatVectorType),
		id:           id,
		maxRows:      config.MaxTxnRow,
		eventChan:    eventChan,
		changefeedID: changefeedID,
	}
}

// func (w *dmlWorker) GetEventChan() <-chan *commonEvent.DMLEvent {
// 	return w.eventChan
// }

func (w *dmlWorker) Run(ctx context.Context) error {
	namespace := w.changefeedID.Namespace()
	changefeed := w.changefeedID.Name()

	workerFlushDuration := metrics.WorkerFlushDuration.WithLabelValues(namespace, changefeed, strconv.Itoa(w.id))
	workerTotalDuration := metrics.WorkerTotalDuration.WithLabelValues(namespace, changefeed, strconv.Itoa(w.id))
	workerHandledRows := metrics.WorkerHandledRows.WithLabelValues(namespace, changefeed, strconv.Itoa(w.id))

	defer func() {
		metrics.WorkerFlushDuration.DeleteLabelValues(namespace, changefeed, strconv.Itoa(w.id))
		metrics.WorkerTotalDuration.DeleteLabelValues(namespace, changefeed, strconv.Itoa(w.id))
		metrics.WorkerHandledRows.DeleteLabelValues(namespace, changefeed, strconv.Itoa(w.id))
	}()

	totalStart := time.Now()
	events := make([]*commonEvent.DMLEvent, 0)
	rows := 0
	for {
		needFlush := false
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case txnEvent := <-w.eventChan:
			events = append(events, txnEvent)
			rows += int(txnEvent.Len())
			if rows > w.maxRows {
				needFlush = true
			}
			if !needFlush {
				delay := time.NewTimer(10 * time.Millisecond)
				for !needFlush {
					select {
					case txnEvent := <-w.eventChan:
						workerHandledRows.Add(float64(txnEvent.Len()))
						events = append(events, txnEvent)
						rows += int(txnEvent.Len())
						if rows > w.maxRows {
							needFlush = true
						}
					case <-delay.C:
						needFlush = true
					}
				}
				// Release resources promptly
				if !delay.Stop() {
					select {
					case <-delay.C:
					default:
					}
				}
			}
			start := time.Now()
			err := w.mysqlWriter.Flush(events)
			if err != nil {
				return errors.Trace(err)
			}
			workerFlushDuration.Observe(time.Since(start).Seconds())
			// we record total time to calcuate the worker busy ratio.
			// so we record the total time after flushing, to unified statistics on
			// flush time and total time
			workerTotalDuration.Observe(time.Since(totalStart).Seconds())
			totalStart = time.Now()
			events = events[:0]
			rows = 0
		}
	}
}

func (w *dmlWorker) Close() {
	w.mysqlWriter.Close()
}

// func (w *dmlWorker) AddDMLEvent(event *commonEvent.DMLEvent) {
// 	w.eventChan <- event
// }

// ddlWorker is use to flush the ddl event and sync point eventdownstream
type ddlWorker struct {
	changefeedID common.ChangeFeedID
	mysqlWriter  *mysql.Writer
}

func NewMysqlDDLWorker(
	ctx context.Context,
	db *sql.DB,
	config *mysql.MysqlConfig,
	changefeedID common.ChangeFeedID,
	statistics *metrics.Statistics,
	formatVectorType bool,
) *ddlWorker {
	return &ddlWorker{
		changefeedID: changefeedID,
		mysqlWriter:  mysql.NewMysqlWriter(ctx, db, config, changefeedID, statistics, formatVectorType),
	}
}

func (w *ddlWorker) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	w.mysqlWriter.SetTableSchemaStore(tableSchemaStore)
}

func (w *ddlWorker) GetStartTsList(tableIds []int64, startTsList []int64) ([]int64, []bool, error) {
	ddlTsList, isSyncpointList, err := w.mysqlWriter.GetStartTsList(tableIds)
	if err != nil {
		return nil, nil, err
	}
	resTs := make([]int64, len(ddlTsList))
	for idx, ddlTs := range ddlTsList {
		if startTsList[idx] > ddlTs {
			isSyncpointList[idx] = false
		}
		resTs[idx] = max(ddlTs, startTsList[idx])
	}

	return resTs, isSyncpointList, nil
}

func (w *ddlWorker) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch event.GetType() {
	case commonEvent.TypeDDLEvent:
		err := w.mysqlWriter.FlushDDLEvent(event.(*commonEvent.DDLEvent))
		if err != nil {
			return errors.Trace(err)
		}
	case commonEvent.TypeSyncPointEvent:
		err := w.mysqlWriter.FlushSyncPointEvent(event.(*commonEvent.SyncPointEvent))
		if err != nil {
			return errors.Trace(err)
		}
	default:
		log.Error("unknown event type",
			zap.String("namespace", w.changefeedID.Namespace()),
			zap.String("changefeed", w.changefeedID.Name()),
			zap.Any("event", event))
	}
	return nil
}

func (w *ddlWorker) RemoveDDLTsItem() error {
	return w.mysqlWriter.RemoveDDLTsItem()
}

func (w *ddlWorker) Close() {
	w.mysqlWriter.Close()
}
