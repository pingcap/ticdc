// Copyright 2022 PingCAP, Inc.
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

package schemastore

import (
	"encoding/json"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type tableTask struct {
	schema string
	values [][]byte
}

// tableVerifier encapsulates the concurrent verification logic used by VerifyTables.
// It keeps worker coordination, error propagation, and result aggregation together to
// reduce the complexity of VerifyTables while retaining the worker-pool optimization.
type tableVerifier struct {
	filter filter.Filter

	tasks chan tableTask
	done  chan struct{}

	appendMu sync.Mutex
	workerWg sync.WaitGroup

	errOnce  sync.Once
	firstErr error

	tableInfos       []*common.TableInfo
	ineligibleTables []string
	eligibleTables   []string
}

func newTableVerifier(f filter.Filter, tableWorkers int) *tableVerifier {
	return &tableVerifier{
		filter: f,
		tasks:  make(chan tableTask, tableWorkers*2),
		done:   make(chan struct{}),

		tableInfos:       make([]*common.TableInfo, 0),
		ineligibleTables: make([]string, 0),
		eligibleTables:   make([]string, 0),
	}
}

func (v *tableVerifier) setErr(err error) {
	v.errOnce.Do(func() {
		v.firstErr = err
		close(v.done)
	})
}

func (v *tableVerifier) start(tableWorkers int) {
	v.workerWg.Add(tableWorkers)
	for i := 0; i < tableWorkers; i++ {
		go v.worker()
	}
}

func (v *tableVerifier) closeAndWait() {
	close(v.tasks)
	v.workerWg.Wait()
}

func (v *tableVerifier) sendTask(task tableTask) bool {
	if len(task.values) == 0 {
		return true
	}
	select {
	case v.tasks <- task:
		return true
	case <-v.done:
		return false
	}
}

func (v *tableVerifier) appendResults(
	tableInfos []*common.TableInfo, ineligibleTables []string, eligibleTables []string,
) {
	if len(tableInfos) == 0 {
		return
	}
	v.appendMu.Lock()
	v.tableInfos = append(v.tableInfos, tableInfos...)
	v.ineligibleTables = append(v.ineligibleTables, ineligibleTables...)
	v.eligibleTables = append(v.eligibleTables, eligibleTables...)
	v.appendMu.Unlock()
}

func (v *tableVerifier) worker() {
	defer v.workerWg.Done()
	for {
		var task tableTask
		select {
		case <-v.done:
			return
		case t, ok := <-v.tasks:
			if !ok {
				return
			}
			task = t
		}

		localInfos := make([]*common.TableInfo, 0, len(task.values))
		localIneligible := make([]string, 0, len(task.values))
		localEligible := make([]string, 0, len(task.values))

		for _, value := range task.values {
			tbInfo := &timodel.TableInfo{}
			if err := json.Unmarshal(value, tbInfo); err != nil {
				v.setErr(errors.Trace(err))
				return
			}

			tableName := tbInfo.Name.O
			if v.filter.ShouldIgnoreTable(task.schema, tableName) {
				log.Debug("ignore table", zap.String("schema", task.schema), zap.String("table", tableName))
				continue
			}
			// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
			// See https://github.com/pingcap/tiflow/issues/4559
			if tbInfo.Sequence != nil {
				continue
			}

			tableInfo := common.WrapTableInfo(task.schema, tbInfo)
			localInfos = append(localInfos, tableInfo)
			if !tableInfo.IsEligible(false /* forceReplicate */) {
				localIneligible = append(localIneligible, tableInfo.GetTableName())
			} else {
				localEligible = append(localEligible, tableInfo.GetTableName())
			}
		}

		v.appendResults(localInfos, localIneligible, localEligible)
	}
}

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(f filter.Filter, storage tidbkv.Storage, startTs uint64) (
	[]*common.TableInfo, []string, []string, error,
) {
	const (
		// A fixed-size worker pool is used to parallelize the JSON unmarshal of
		// timodel.TableInfo while keeping goroutine count bounded.
		tableWorkers = 16
		batchSize    = 1024
	)

	meta := getSnapshotMeta(storage, startTs)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, nil, nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}

	verifier := newTableVerifier(f, tableWorkers)
	verifier.start(tableWorkers)

dbLoop:
	for _, dbinfo := range dbinfos {
		select {
		case <-verifier.done:
			break dbLoop
		default:
		}
		schemaName := dbinfo.Name.O
		if f.ShouldIgnoreSchema(schemaName) {
			log.Debug("ignore schema", zap.String("schema", schemaName))
			continue
		}

		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		if err != nil {
			verifier.setErr(cerror.WrapError(cerror.ErrMetaListDatabases, err))
			break
		}

		batch := make([][]byte, 0, batchSize)
		for _, r := range rawTables {
			if !strings.HasPrefix(string(r.Field), mTablePrefix) {
				continue
			}
			batch = append(batch, r.Value)
			if len(batch) >= batchSize {
				if !verifier.sendTask(tableTask{schema: schemaName, values: batch}) {
					break dbLoop
				}
				batch = make([][]byte, 0, batchSize)
			}
		}

		if !verifier.sendTask(tableTask{schema: schemaName, values: batch}) {
			break
		}
	}

	verifier.closeAndWait()

	if verifier.firstErr != nil {
		return nil, nil, nil, verifier.firstErr
	}
	return verifier.tableInfos, verifier.ineligibleTables, verifier.eligibleTables, nil
}
