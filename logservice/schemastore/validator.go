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
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(f filter.Filter, storage tidbkv.Storage, startTs uint64) (
	[]*common.TableInfo, []string, []string, error,
) {
	start := time.Now()
	defer func() {
		log.Info("VerifyTables took %s", zap.Any("timecost", time.Since(start)))
	}()
	// NOTE: We keep a fixed number of workers here. The goal is to parallelize the
	// JSON unmarshal of timodel.TableInfo while avoiding excessive goroutines and
	// memory pressure for huge table counts.
	const (
		tableWorkers = 16
		batchSize    = 1024
	)

	meta := getSnapshotMeta(storage, startTs)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, nil, nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}

	tableInfos := make([]*common.TableInfo, 0)
	ineligibleTables := make([]string, 0)
	eligibleTables := make([]string, 0)

	type tableTask struct {
		schema string
		values [][]byte
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		appendMu sync.Mutex
		taskWg   sync.WaitGroup
		workerWg sync.WaitGroup

		errOnce  sync.Once
		firstErr error
	)

	setErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = err
			cancel()
		})
	}

	tasks := make(chan tableTask, tableWorkers*2)

	worker := func() {
		defer workerWg.Done()
		for task := range tasks {
			func() {
				defer taskWg.Done()

				if ctx.Err() != nil {
					return
				}

				localInfos := make([]*common.TableInfo, 0, len(task.values))
				localIneligible := make([]string, 0)
				localEligible := make([]string, 0)

				for _, value := range task.values {
					if ctx.Err() != nil {
						return
					}

					tbInfo := &timodel.TableInfo{}
					if err := json.Unmarshal(value, tbInfo); err != nil {
						setErr(errors.Trace(err))
						return
					}

					tableName := tbInfo.Name.O
					if f.ShouldIgnoreTable(task.schema, tableName) {
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

				if len(localInfos) == 0 {
					return
				}

				appendMu.Lock()
				tableInfos = append(tableInfos, localInfos...)
				ineligibleTables = append(ineligibleTables, localIneligible...)
				eligibleTables = append(eligibleTables, localEligible...)
				appendMu.Unlock()
			}()
		}
	}

	workerWg.Add(tableWorkers)
	for i := 0; i < tableWorkers; i++ {
		go worker()
	}

	sendTask := func(task tableTask) bool {
		if len(task.values) == 0 {
			return true
		}
		taskWg.Add(1)
		select {
		case tasks <- task:
			return true
		case <-ctx.Done():
			taskWg.Done()
			return false
		}
	}

	for _, dbinfo := range dbinfos {
		if ctx.Err() != nil {
			break
		}
		if f.ShouldIgnoreSchema(dbinfo.Name.O) {
			continue
		}

		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		if err != nil {
			setErr(cerror.WrapError(cerror.ErrMetaListDatabases, err))
			break
		}

		batch := make([][]byte, 0, batchSize)
		for _, r := range rawTables {
			if ctx.Err() != nil {
				break
			}
			if !strings.HasPrefix(string(r.Field), mTablePrefix) {
				continue
			}
			batch = append(batch, r.Value)
			if len(batch) >= batchSize {
				if !sendTask(tableTask{schema: dbinfo.Name.O, values: batch}) {
					break
				}
				batch = make([][]byte, 0, batchSize)
			}
		}

		if ctx.Err() == nil {
			_ = sendTask(tableTask{schema: dbinfo.Name.O, values: batch})
		}

		taskWg.Wait()
	}

	close(tasks)
	workerWg.Wait()

	if firstErr != nil {
		return nil, nil, nil, firstErr
	}
	return tableInfos, ineligibleTables, eligibleTables, nil
}
