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
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestVerifyTablesCompletionLog(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tableCount int
		corrupt    bool
	}{
		{name: "empty"},
		{name: "success", tableCount: 1},
		{name: "worker error", tableCount: 1, corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := mockstore.NewMockStore()
			require.NoError(t, err)
			defer func() { require.NoError(t, store.Close()) }()
			err = kv.RunInNewTxn(context.Background(), store, false, func(_ context.Context, txn kv.Transaction) error {
				m := meta.NewMutator(txn)
				if err := m.CreateDatabase(&model.DBInfo{ID: 1, Name: ast.NewCIStr("sales")}); err != nil {
					return err
				}
				if tc.tableCount == 0 {
					return nil
				}
				if tc.corrupt {
					// Supply malformed table JSON to the real verifier worker.
					return structure.NewStructure(txn, txn, []byte("m")).HSet([]byte("DB:1"), []byte("Table:2"), []byte("{"))
				}
				return m.CreateTableOrView(1, &model.TableInfo{ID: 2, Name: ast.NewCIStr("orders")})
			})
			require.NoError(t, err)
			version, err := store.CurrentVersion(kv.GlobalTxnScope)
			require.NoError(t, err)
			f, err := filter.NewFilter(config.GetDefaultReplicaConfig().Filter, "", false, false)
			require.NoError(t, err)
			core, logs := observer.New(zapcore.InfoLevel)
			restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core})
			defer restore()

			infos, ineligible, eligible, all, err := VerifyTables(f, store, version.Ver)
			if tc.corrupt {
				require.Error(t, err)
				require.IsType(t, &json.SyntaxError{}, errors.Cause(err))
				require.EqualError(t, err, "unexpected end of JSON input")
				require.Nil(t, infos)
				require.Nil(t, ineligible)
				require.Nil(t, eligible)
				require.Nil(t, all)
				require.Empty(t, logs.All())
				return
			}
			require.NoError(t, err)
			require.Len(t, infos, tc.tableCount)
			require.Len(t, all, tc.tableCount)
			entries := logs.FilterMessage("table replication eligibility verified").All()
			require.Len(t, entries, 1)
			require.EqualValues(t, tc.tableCount, entries[0].ContextMap()["tableCount"])
			require.Equal(t, version.Ver, entries[0].ContextMap()["startTs"])
		})
	}
}
