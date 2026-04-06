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

package cloudstorage

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	sinkiceberg "github.com/pingcap/ticdc/pkg/sink/iceberg"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/utils/chann"
	"github.com/pingcap/tidb/br/pkg/storage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type icebergPendingTxn struct {
	commitTs  uint64
	tableID   int64
	tableInfo *commonType.TableInfo
	rows      []sinkiceberg.ChangeRow
	callback  func()
}

type icebergColumnPlanColumn struct {
	idx  int
	name string
	ft   *types.FieldType
}

type icebergTableColumnPlan struct {
	updateTs    uint64
	dataColumns []icebergColumnPlanColumn
}

type icebergTableBuffer struct {
	pending []icebergPendingTxn
}

type icebergProcessor struct {
	changefeedID commonType.ChangeFeedID
	cfg          *sinkiceberg.Config
	tableWriter  *sinkiceberg.TableWriter
	statistics   *metrics.Statistics

	dmlCh *chann.UnlimitedChannel[*commonEvent.DMLEvent, any]

	commitMu sync.Mutex
	mu       sync.Mutex
	planMu   sync.Mutex

	maxCommitTs atomic.Uint64
	closed      atomic.Bool

	buffers     map[int64]*icebergTableBuffer
	columnPlans map[int64]*icebergTableColumnPlan
	committed   map[int64]uint64

	isNormal *atomic.Bool
}

func verifyIcebergStorageProtocol(
	ctx context.Context,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
) error {
	icebergCfg := sinkiceberg.NewConfig()
	if err := icebergCfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return err
	}
	if icebergCfg.Mode != sinkiceberg.ModeAppend {
		return errors.ErrSinkURIInvalid.GenWithStackByArgs("storage sink only supports iceberg append mode")
	}

	st, err := storageFromSinkURI(ctx, sinkURI)
	if err != nil {
		return err
	}
	defer st.Close()

	tableWriter := sinkiceberg.NewTableWriter(icebergCfg, st)
	if err := tableWriter.VerifyCatalog(ctx); err != nil {
		return err
	}
	return nil
}

func newIcebergProcessor(
	ctx context.Context,
	changefeedID commonType.ChangeFeedID,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	st storage.ExternalStorage,
	statistics *metrics.Statistics,
	isNormal *atomic.Bool,
) (*icebergProcessor, error) {
	icebergCfg := sinkiceberg.NewConfig()
	if err := icebergCfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return nil, err
	}
	if icebergCfg.Mode != sinkiceberg.ModeAppend {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("storage sink only supports iceberg append mode")
	}

	tableWriter := sinkiceberg.NewTableWriter(icebergCfg, st)
	if err := tableWriter.VerifyCatalog(ctx); err != nil {
		return nil, err
	}

	return &icebergProcessor{
		changefeedID: changefeedID,
		cfg:          icebergCfg,
		tableWriter:  tableWriter,
		statistics:   statistics,
		dmlCh:        chann.NewUnlimitedChannelDefault[*commonEvent.DMLEvent](),
		buffers:      make(map[int64]*icebergTableBuffer),
		columnPlans:  make(map[int64]*icebergTableColumnPlan),
		committed:    make(map[int64]uint64),
		isNormal:     isNormal,
	}, nil
}

func storageFromSinkURI(ctx context.Context, sinkURI *url.URL) (storage.ExternalStorage, error) {
	return util.GetExternalStorageWithDefaultTimeout(ctx, sinkURI.String())
}

func (p *icebergProcessor) run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		<-ctx.Done()
		p.close()
		return nil
	})

	g.Go(func() error {
		return p.bufferLoop(ctx)
	})

	g.Go(func() error {
		return p.commitLoop(ctx)
	})

	err := g.Wait()
	if err != nil {
		p.isNormal.Store(false)
	}
	return err
}

func (p *icebergProcessor) close() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	p.dmlCh.Close()
}

func (p *icebergProcessor) addDMLEvent(event *commonEvent.DMLEvent) {
	p.dmlCh.Push(event)
}

func (p *icebergProcessor) flushDMLBeforeBlock(ctx context.Context, event commonEvent.BlockEvent) error {
	if event == nil {
		return nil
	}
	p.commitMu.Lock()
	defer p.commitMu.Unlock()
	return p.commitOnce(ctx, event.GetCommitTs())
}

func (p *icebergProcessor) handleDDLEvent(ctx context.Context, event *commonEvent.DDLEvent) error {
	if event == nil {
		return nil
	}

	return p.statistics.RecordDDLExecution(func() (string, error) {
		for _, e := range event.GetEvents() {
			if e == nil || e.NotSync || e.TableInfo == nil {
				continue
			}
			switch e.GetDDLType() {
			case timodel.ActionDropTable, timodel.ActionDropSchema:
				continue
			case timodel.ActionTruncateTablePartition,
				timodel.ActionAddTablePartition, timodel.ActionDropTablePartition,
				timodel.ActionExchangeTablePartition, timodel.ActionReorganizePartition,
				timodel.ActionAlterTablePartitioning, timodel.ActionRemovePartitioning:
				return "", errors.ErrSinkURIInvalid.GenWithStackByArgs(
					fmt.Sprintf("iceberg storage sink does not support ddl action: %s", e.GetDDLType()),
				)
			default:
			}

			if err := p.tableWriter.EnsureTable(ctx, p.changefeedID, e.TableInfo); err != nil {
				return "", err
			}
		}
		return event.GetDDLType().String(), nil
	})
}

func (p *icebergProcessor) bufferLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		default:
		}

		ev, ok := p.dmlCh.Get()
		if !ok {
			return nil
		}
		if ev == nil || ev.TableInfo == nil || ev.Rows == nil {
			continue
		}

		plan, err := p.getColumnPlan(ev.PhysicalTableID, ev.TableInfo)
		if err != nil {
			p.isNormal.Store(false)
			return err
		}

		rows, err := convertToIcebergRows(ev, plan)
		if err != nil {
			p.isNormal.Store(false)
			return err
		}
		if len(rows) == 0 {
			ev.PostFlush()
			continue
		}

		txn := icebergPendingTxn{
			commitTs:  ev.CommitTs,
			tableID:   ev.PhysicalTableID,
			tableInfo: ev.TableInfo,
			rows:      rows,
			callback:  ev.PostFlush,
		}

		for {
			current := p.maxCommitTs.Load()
			if txn.commitTs <= current {
				break
			}
			if p.maxCommitTs.CAS(current, txn.commitTs) {
				break
			}
		}

		p.mu.Lock()
		buf := p.buffers[txn.tableID]
		if buf == nil {
			buf = &icebergTableBuffer{}
			p.buffers[txn.tableID] = buf
		}
		buf.pending = append(buf.pending, txn)
		p.mu.Unlock()
	}
}

func (p *icebergProcessor) commitLoop(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(context.Cause(ctx))
		case <-ticker.C:
			resolvedTs := p.maxCommitTs.Load()
			if resolvedTs == 0 {
				continue
			}
			p.commitMu.Lock()
			err := p.commitOnce(ctx, resolvedTs)
			p.commitMu.Unlock()
			if err != nil {
				p.isNormal.Store(false)
				return err
			}
		}
	}
}

func (p *icebergProcessor) commitOnce(ctx context.Context, resolvedTs uint64) error {
	type task struct {
		tableID   int64
		tableInfo *commonType.TableInfo
		txns      []icebergPendingTxn
	}

	prunedCallbacks, err := p.pruneCommittedTxns(ctx)
	if err != nil {
		return err
	}
	for _, cb := range prunedCallbacks {
		cb()
	}

	var tasks []task
	p.mu.Lock()
	for tableID, buf := range p.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}

		n := 0
		for n < len(buf.pending) && buf.pending[n].commitTs <= resolvedTs {
			n++
		}
		if n == 0 {
			continue
		}

		txns := append([]icebergPendingTxn(nil), buf.pending[:n]...)
		tasks = append(tasks, task{
			tableID:   tableID,
			tableInfo: txns[len(txns)-1].tableInfo,
			txns:      txns,
		})
	}
	p.mu.Unlock()

	for _, t := range tasks {
		if t.tableInfo == nil {
			continue
		}
		taskResolvedTs := t.txns[len(t.txns)-1].commitTs
		rows := make([]sinkiceberg.ChangeRow, 0, len(t.txns))
		callbacks := make([]func(), 0, len(t.txns))
		for _, txn := range t.txns {
			rows = append(rows, txn.rows...)
			if txn.callback != nil {
				callbacks = append(callbacks, txn.callback)
			}
		}
		if len(rows) == 0 {
			continue
		}

		start := time.Now()
		commitResult, err := p.appendRows(ctx, t.tableInfo, t.tableID, rows, taskResolvedTs)
		if err != nil {
			return err
		}
		if commitResult == nil {
			continue
		}

		for _, cb := range callbacks {
			cb()
		}

		p.mu.Lock()
		buf := p.buffers[t.tableID]
		if buf != nil && len(buf.pending) >= len(t.txns) {
			buf.pending = buf.pending[len(t.txns):]
		}
		p.committed[t.tableID] = taskResolvedTs
		p.mu.Unlock()

		log.Info("iceberg storage sink table committed",
			zap.String("namespace", p.changefeedID.Keyspace()),
			zap.String("changefeed", p.changefeedID.Name()),
			zap.String("schema", t.tableInfo.GetSchemaName()),
			zap.String("table", t.tableInfo.GetTableName()),
			zap.Int64("tableID", t.tableID),
			zap.Int64("snapshotID", commitResult.SnapshotID),
			zap.Uint64("resolvedTs", taskResolvedTs),
			zap.Int("rows", len(rows)),
			zap.Int64("bytes", commitResult.BytesWritten),
			zap.Duration("duration", time.Since(start)))
	}

	return nil
}

func (p *icebergProcessor) appendRows(
	ctx context.Context,
	tableInfo *commonType.TableInfo,
	tableID int64,
	rows []sinkiceberg.ChangeRow,
	resolvedTs uint64,
) (*sinkiceberg.CommitResult, error) {
	var result *sinkiceberg.CommitResult
	err := p.statistics.RecordBatchExecution(func() (int, int64, error) {
		commitResult, err := p.tableWriter.AppendChangelog(ctx, p.changefeedID, tableInfo, tableID, rows, resolvedTs)
		if err != nil {
			return 0, 0, err
		}
		result = commitResult
		if commitResult == nil {
			return 0, 0, nil
		}
		return len(rows), commitResult.BytesWritten, nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (p *icebergProcessor) pruneCommittedTxns(ctx context.Context) ([]func(), error) {
	type tableMeta struct {
		tableID   int64
		tableInfo *commonType.TableInfo
	}

	var toLoad []tableMeta
	p.mu.Lock()
	for tableID, buf := range p.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}
		if _, ok := p.committed[tableID]; ok {
			continue
		}
		toLoad = append(toLoad, tableMeta{
			tableID:   tableID,
			tableInfo: buf.pending[len(buf.pending)-1].tableInfo,
		})
	}
	p.mu.Unlock()

	loaded := make(map[int64]uint64, len(toLoad))
	for _, t := range toLoad {
		committedTs, err := p.tableWriter.GetLastCommittedResolvedTs(ctx, t.tableInfo)
		if err != nil {
			return nil, err
		}
		loaded[t.tableID] = committedTs
	}

	var callbacks []func()
	p.mu.Lock()
	for tableID, committedTs := range loaded {
		p.committed[tableID] = committedTs
	}
	for tableID, buf := range p.buffers {
		if buf == nil || len(buf.pending) == 0 {
			continue
		}
		committedTs := p.committed[tableID]
		n := 0
		for n < len(buf.pending) && buf.pending[n].commitTs <= committedTs {
			if cb := buf.pending[n].callback; cb != nil {
				callbacks = append(callbacks, cb)
			}
			n++
		}
		if n > 0 {
			buf.pending = buf.pending[n:]
		}
	}
	p.mu.Unlock()
	return callbacks, nil
}

func (p *icebergProcessor) getColumnPlan(tableID int64, tableInfo *commonType.TableInfo) (*icebergTableColumnPlan, error) {
	if tableInfo == nil {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	updateTs := tableInfo.UpdateTS
	p.planMu.Lock()
	plan := p.columnPlans[tableID]
	if plan != nil && plan.updateTs == updateTs {
		p.planMu.Unlock()
		return plan, nil
	}
	newPlan, err := buildIcebergColumnPlan(tableInfo)
	if err != nil {
		p.planMu.Unlock()
		return nil, err
	}
	newPlan.updateTs = updateTs
	p.columnPlans[tableID] = newPlan
	p.planMu.Unlock()
	return newPlan, nil
}

func buildIcebergColumnPlan(tableInfo *commonType.TableInfo) (*icebergTableColumnPlan, error) {
	if tableInfo == nil {
		return nil, errors.ErrSinkURIInvalid.GenWithStackByArgs("table info is nil")
	}

	colInfos := tableInfo.GetColumns()
	if len(colInfos) == 0 {
		return &icebergTableColumnPlan{}, nil
	}

	dataColumns := make([]icebergColumnPlanColumn, 0, len(colInfos))
	for idx, colInfo := range colInfos {
		if colInfo == nil || colInfo.IsVirtualGenerated() {
			continue
		}
		dataColumns = append(dataColumns, icebergColumnPlanColumn{
			idx:  idx,
			name: colInfo.Name.O,
			ft:   &colInfo.FieldType,
		})
	}

	return &icebergTableColumnPlan{
		dataColumns: dataColumns,
	}, nil
}

func convertToIcebergRows(event *commonEvent.DMLEvent, plan *icebergTableColumnPlan) ([]sinkiceberg.ChangeRow, error) {
	if event == nil || plan == nil || len(plan.dataColumns) == 0 {
		return nil, nil
	}

	commitTime := oracle.GetTimeFromTS(event.CommitTs).UTC()
	commitTimeStr := commitTime.Format(time.RFC3339Nano)
	commitTsStr := strconv.FormatUint(event.CommitTs, 10)

	event.Rewind()
	defer event.Rewind()

	rows := make([]sinkiceberg.ChangeRow, 0, event.Len())
	for {
		change, ok := event.GetNextRow()
		if !ok {
			break
		}

		var (
			op  string
			row chunk.Row
		)
		switch change.RowType {
		case commonType.RowTypeInsert:
			op = "I"
			row = change.Row
		case commonType.RowTypeUpdate:
			op = "U"
			row = change.Row
		case commonType.RowTypeDelete:
			op = "D"
			row = change.PreRow
		default:
			continue
		}

		columns := make(map[string]*string, len(plan.dataColumns))
		for _, col := range plan.dataColumns {
			v, err := formatIcebergColumnValue(&row, col.idx, col.ft)
			if err != nil {
				return nil, err
			}
			columns[col.name] = v
		}

		rows = append(rows, sinkiceberg.ChangeRow{
			Op:         op,
			CommitTs:   commitTsStr,
			CommitTime: commitTimeStr,
			Columns:    columns,
		})
	}
	return rows, nil
}

func formatIcebergColumnValue(row *chunk.Row, idx int, ft *types.FieldType) (*string, error) {
	if row.IsNull(idx) {
		return nil, nil
	}

	d := row.GetDatum(idx, ft)
	var value string

	switch ft.GetType() {
	case mysql.TypeBit:
		v, err := d.GetMysqlBit().ToInt(types.DefaultStmtNoWarningContext)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = strconv.FormatUint(v, 10)
	case mysql.TypeTimestamp, mysql.TypeDate, mysql.TypeDatetime:
		value = d.GetMysqlTime().String()
	case mysql.TypeDuration:
		value = d.GetMysqlDuration().String()
	case mysql.TypeNewDecimal:
		value = d.GetMysqlDecimal().String()
	case mysql.TypeJSON:
		value = d.GetMysqlJSON().String()
	case mysql.TypeEnum:
		v := d.GetMysqlEnum().Value
		enumVal, err := types.ParseEnumValue(ft.GetElems(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = enumVal.Name
	case mysql.TypeSet:
		v := d.GetMysqlSet().Value
		setVal, err := types.ParseSetValue(ft.GetElems(), v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		value = setVal.Name
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		value = base64.StdEncoding.EncodeToString(d.GetBytes())
	case mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeString:
		if mysql.HasBinaryFlag(ft.GetFlag()) {
			value = base64.StdEncoding.EncodeToString(d.GetBytes())
		} else {
			value = d.GetString()
		}
	default:
		switch v := d.GetValue().(type) {
		case int64:
			value = strconv.FormatInt(v, 10)
		case uint64:
			value = strconv.FormatUint(v, 10)
		case float32:
			value = strconv.FormatFloat(float64(v), 'f', -1, 32)
		case float64:
			value = strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			value = v
		case []byte:
			if mysql.HasBinaryFlag(ft.GetFlag()) {
				value = base64.StdEncoding.EncodeToString(v)
			} else {
				value = string(v)
			}
		default:
			value = fmt.Sprintf("%v", v)
		}
	}

	return &value, nil
}
