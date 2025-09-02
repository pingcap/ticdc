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
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/filter"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
)

// VerifyTables catalog tables specified by ReplicaConfig into
// eligible (has an unique index or primary key) and ineligible tables.
func VerifyTables(
	f filter.Filter,
	storage tidbkv.Storage,
	startTs uint64) (
	tableInfos []*common.TableInfo,
	ineligibleTables,
	eligibleTables []string,
	err error,
) {
	meta := getSnapshotMeta(storage, startTs)
	snap, err := NewSnapshotFromMeta(
		common.NewChangefeedID4Test("api", "verifyTable"),
		meta, startTs, false /* explicitTables */, f)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	snap.IterTables(true, func(tableInfo *common.TableInfo) {
		if f.ShouldIgnoreTable(tableInfo.TableName.Schema, tableInfo.TableName.Table, tableInfo.ToTiDBTableInfo()) {
			return
		}
		// Sequence is not supported yet, TiCDC needs to filter all sequence tables.
		// See https://github.com/pingcap/tiflow/issues/4559
		if tableInfo.IsSequence() {
			return
		}
		tableInfos = append(tableInfos, tableInfo)
		if !tableInfo.IsEligible(false /* forceReplicate */) {
			ineligibleTables = append(ineligibleTables, tableInfo.GetTableName())
		} else {
			eligibleTables = append(eligibleTables, tableInfo.GetTableName())
		}
	})
	return
}
