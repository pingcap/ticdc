// Copyright 2025 PingCAP, Inc.
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

package eventservice

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/ticdc/eventpb"
	"github.com/pingcap/ticdc/heartbeatpb"
	"github.com/pingcap/ticdc/logservice/eventstore"
	bf "github.com/pingcap/ticdc/pkg/binlog-filter"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/integrity"
)

type disableDMLTypeFastPathFilter struct {
	filter.Filter
}

func (f disableDMLTypeFastPathFilter) ShouldIgnoreDMLByEventType(
	common.RowType,
	*common.TableInfo,
	uint64,
) (bool, error) {
	return false, nil
}

type benchmarkEventGetter struct {
	raw  *common.RawKVEntry
	rows int
}

func (g *benchmarkEventGetter) GetIterator(
	common.DispatcherID,
	common.DataRange,
) (eventstore.EventIterator, error) {
	return &singleTxnIterator{
		raw:  g.raw,
		rows: g.rows,
	}, nil
}

type singleTxnIterator struct {
	raw  *common.RawKVEntry
	rows int
	next int
}

func (i *singleTxnIterator) Next() (*common.RawKVEntry, bool) {
	if i.next >= i.rows {
		return nil, false
	}
	isNewTxn := i.next == 0
	i.next++
	return i.raw, isNewTxn
}

func (i *singleTxnIterator) Close() (int64, error) {
	return int64(i.next), nil
}

func newBenchmarkDispatcherInfo(
	startTs uint64,
	dispatcherID common.DispatcherID,
	tableID int64,
) *mockDispatcherInfo {
	return &mockDispatcherInfo{
		clusterID:    1,
		serverID:     "server1",
		id:           dispatcherID,
		changefeedID: common.NewChangefeedID4Test("default", "bench"),
		topic:        "topic1",
		span: &heartbeatpb.TableSpan{
			TableID:  tableID,
			StartKey: []byte("a"),
			EndKey:   []byte("z"),
		},
		startTs:    startTs,
		actionType: eventpb.ActionType_ACTION_TYPE_REGISTER,
		filterConfig: &eventpb.FilterConfig{
			FilterConfig: &eventpb.InnerFilterConfig{
				Rules: []string{"*.*"},
			},
		},
		bdrMode:   false,
		integrity: config.GetDefaultReplicaConfig().Integrity,
	}
}

func BenchmarkDMLProcessorIgnoreDelete(b *testing.B) {
	helper := event.NewEventTestHelper(b)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`,
		`insert into test.t(id,c) values (1, "c1")`)
	tableInfo := ddlEvent.TableInfo
	tableID := ddlEvent.GetTableID()
	deleteRow := insertToDeleteRow(kvEvents[0])
	dispatcherID := common.NewDispatcherID()
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	ignoreDeleteFilter, err := filter.NewFilter(&config.FilterConfig{
		Rules: []string{"test.*"},
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.t"},
				IgnoreEvent: []bf.EventType{bf.DeleteEvent},
			},
		},
	}, "UTC", false, false)
	if err != nil {
		b.Fatal(err)
	}

	bench := func(b *testing.B, changefeedFilter filter.Filter) {
		processor := newDMLProcessor(
			event.NewMounter(time.UTC, &integrity.Config{}),
			mockSchemaGetter,
			changefeedFilter,
			false,
			common.DefaultMode,
			false,
		)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := processor.startTxn(dispatcherID, tableID, tableInfo, deleteRow.StartTs, deleteRow.CRTs, false); err != nil {
				b.Fatal(err)
			}
			if err := processor.appendRow(deleteRow); err != nil {
				b.Fatal(err)
			}
			if err := processor.commitTxn(); err != nil {
				b.Fatal(err)
			}
			processor.resetBatchDML()
		}
	}

	b.Run("fast_path", func(b *testing.B) {
		bench(b, ignoreDeleteFilter)
	})
	b.Run("after_decode", func(b *testing.B) {
		bench(b, disableDMLTypeFastPathFilter{Filter: ignoreDeleteFilter})
	})
}

func BenchmarkEventScannerIgnoreDelete(b *testing.B) {
	helper := event.NewEventTestHelper(b)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`,
		`insert into test.t(id,c) values (1, "c1")`)
	tableID := ddlEvent.GetTableID()
	deleteRow := insertToDeleteRow(kvEvents[0])
	dispatcherID := common.NewDispatcherID()
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	ignoreDeleteFilter, err := filter.NewFilter(&config.FilterConfig{
		Rules: []string{"test.*"},
		EventFilters: []*config.EventFilterRule{
			{
				Matcher:     []string{"test.t"},
				IgnoreEvent: []bf.EventType{bf.DeleteEvent},
			},
		},
	}, "UTC", false, false)
	if err != nil {
		b.Fatal(err)
	}

	rowsPerScan := 50000
	bench := func(b *testing.B, changefeedFilter filter.Filter) {
		disInfo := newBenchmarkDispatcherInfo(
			ddlEvent.FinishedTs,
			dispatcherID,
			tableID,
		)
		changefeedStatus := newChangefeedStatus(disInfo.GetChangefeedID(), 0)
		changefeedStatus.filter = changefeedFilter
		disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)
		makeDispatcherReady(disp)
		scanner := newEventScanner(
			&benchmarkEventGetter{raw: deleteRow, rows: rowsPerScan},
			mockSchemaGetter,
			event.NewMounter(time.UTC, &integrity.Config{}),
			common.DefaultMode,
		)
		dataRange := common.DataRange{
			Span:          disInfo.GetTableSpan(),
			CommitTsStart: ddlEvent.FinishedTs,
			CommitTsEnd:   deleteRow.CRTs + 1,
		}
		limit := scanLimit{maxDMLBytes: 1 << 60}

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			_, _, interrupted, err := scanner.scan(context.Background(), disp, dataRange, limit)
			if err != nil {
				b.Fatal(err)
			}
			if interrupted {
				b.Fatal("scan interrupted")
			}
		}
		b.ReportMetric(float64(b.N*rowsPerScan)/time.Since(start).Seconds(), "rows/s")
	}

	b.Run("fast_path", func(b *testing.B) {
		bench(b, ignoreDeleteFilter)
	})
	b.Run("after_decode", func(b *testing.B) {
		bench(b, disableDMLTypeFastPathFilter{Filter: ignoreDeleteFilter})
	})
}

func BenchmarkEventScannerNoIgnoreDelete(b *testing.B) {
	helper := event.NewEventTestHelper(b)
	defer helper.Close()

	ddlEvent, kvEvents := genEvents(helper, `create table test.t(id int primary key, c char(50))`,
		`insert into test.t(id,c) values (1, "c1")`)
	tableID := ddlEvent.GetTableID()
	deleteRow := insertToDeleteRow(kvEvents[0])
	dispatcherID := common.NewDispatcherID()
	mockSchemaGetter := NewMockSchemaStore()
	mockSchemaGetter.AppendDDLEvent(tableID, ddlEvent)

	changefeedFilter, err := filter.NewFilter(&config.FilterConfig{
		Rules: []string{"test.*"},
	}, "UTC", false, false)
	if err != nil {
		b.Fatal(err)
	}

	rowsPerScan := 50000
	bench := func(b *testing.B, parallel bool) {
		disInfo := newBenchmarkDispatcherInfo(
			ddlEvent.FinishedTs,
			dispatcherID,
			tableID,
		)
		changefeedStatus := newChangefeedStatus(disInfo.GetChangefeedID(), 0)
		changefeedStatus.filter = changefeedFilter
		disp := newDispatcherStat(disInfo, 1, 1, nil, changefeedStatus)
		makeDispatcherReady(disp)
		opts := []eventScannerOption{}
		if parallel {
			opts = append(opts,
				withEventScannerMounterFactory(func() event.Mounter {
					return event.NewMounter(time.UTC, &integrity.Config{})
				}),
				withEventScannerParallelDecodeWorkers(defaultParallelDecodeWorkers))
		}
		scanner := newEventScanner(
			&benchmarkEventGetter{raw: deleteRow, rows: rowsPerScan},
			mockSchemaGetter,
			event.NewMounter(time.UTC, &integrity.Config{}),
			common.DefaultMode,
			opts...,
		)
		dataRange := common.DataRange{
			Span:          disInfo.GetTableSpan(),
			CommitTsStart: ddlEvent.FinishedTs,
			CommitTsEnd:   deleteRow.CRTs + 1,
		}
		limit := scanLimit{maxDMLBytes: 1 << 60}

		b.ReportAllocs()
		b.ResetTimer()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			_, _, interrupted, err := scanner.scan(context.Background(), disp, dataRange, limit)
			if err != nil {
				b.Fatal(err)
			}
			if interrupted {
				b.Fatal("scan interrupted")
			}
		}
		b.ReportMetric(float64(b.N*rowsPerScan)/time.Since(start).Seconds(), "rows/s")
	}

	b.Run("sequential", func(b *testing.B) {
		bench(b, false)
	})
	b.Run("parallel", func(b *testing.B) {
		bench(b, true)
	})
}
