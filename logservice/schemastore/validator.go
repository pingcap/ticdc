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
	meta := getSnapshotMeta(storage, startTs)
	dbinfos, err := meta.ListDatabases()
	if err != nil {
		return nil, nil, nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
	}
	tableInfos := make([]*common.TableInfo, 0)
	ineligibleTables := make([]string, 0)
	eligibleTables := make([]string, 0)
	for _, dbinfo := range dbinfos {
		if f.ShouldIgnoreSchema(dbinfo.Name.O) {
			log.Debug("ignore database", zap.Stringer("db", dbinfo.Name))
			continue
		}

		rawTables, err := meta.GetMetasByDBID(dbinfo.ID)
		if err != nil {
			return nil, nil, nil, cerror.WrapError(cerror.ErrMetaListDatabases, err)
		}
		for _, r := range rawTables {
			tableKey := string(r.Field)
			if !strings.HasPrefix(tableKey, mTablePrefix) {
				continue
			}

			tbName := &timodel.TableNameInfo{}
			err := json.Unmarshal(r.Value, tbName)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}

			tbInfo := &timodel.TableInfo{}
			err = json.Unmarshal(r.Value, tbInfo)
			if err != nil {
				return nil, nil, nil, errors.Trace(err)
			}
			tableInfo := common.WrapTableInfo(dbinfo.Name.O, tbInfo)
			if f.ShouldIgnoreTable(dbinfo.Name.O, tbName.Name.O) {
				log.Debug("ignore table", zap.String("db", dbinfo.Name.O),
					zap.String("table", tbName.Name.O))
				continue
			}
			// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
			// See https://github.com/pingcap/tiflow/issues/4559
			if tableInfo.IsSequence() {
				continue
			}
			tableInfos = append(tableInfos, tableInfo)
			if !tableInfo.IsEligible(false /* forceReplicate */) {
				ineligibleTables = append(ineligibleTables, tableInfo.GetTableName())
			} else {
				eligibleTables = append(eligibleTables, tableInfo.GetTableName())
			}
		}
	}

	return tableInfos, ineligibleTables, eligibleTables, nil
}
