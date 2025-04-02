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

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
)

// dmlWorker is used to flush the dml event downstream
type dmlWorker struct {
	changefeedID common.ChangeFeedID

	eventChan <-chan *commonEvent.DMLEvent
	writer    *mysql.Writer
	id        int

	maxRows int
}

func newDMLWorker(
	ctx context.Context,
	db *sql.DB,
	config *mysql.Config,
	id int,
	changefeedID common.ChangeFeedID,
	statistics *metrics.Statistics,
	formatVectorType bool,
	eventChan <-chan *commonEvent.DMLEvent,
) *dmlWorker {
	return &dmlWorker{
		writer:       mysql.NewWriter(ctx, db, config, changefeedID, statistics, formatVectorType),
		id:           id,
		maxRows:      config.MaxTxnRow,
		eventChan:    eventChan,
		changefeedID: changefeedID,
	}
}

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
			err := w.writer.Flush(events)
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
	w.writer.Close()
}
