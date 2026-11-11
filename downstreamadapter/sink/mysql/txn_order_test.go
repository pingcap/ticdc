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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderTablesBacklog(t *testing.T) {
	helper := commonEvent.NewEventTestHelper(t)
	defer helper.Close()
	helper.DDL2Job("create table test.ordered (id int primary key, v int)")
	hot := helper.DML2Event("test", "ordered", "insert into test.ordered values (1, 1)")
	cold := helper.DML2Event("test", "ordered", "insert into test.ordered values (2, 1)")
	bulk := helper.DML2Event("test", "ordered", "insert into test.ordered values (3, 1)", "insert into test.ordered values (4, 1)", "insert into test.ordered values (5, 1)", "insert into test.ordered values (6, 1)", "insert into test.ordered values (7, 1)")
	require.Equal(t, int32(5), bulk.Len())

	for _, workers := range []int{1, 4} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			cfg := mysql.New()
			cfg.WorkerCount = workers
			cfg.MaxTxnRow = 4
			cfg.MaxAllowedPacket = int64(vardef.DefMaxAllowedPacket)
			cfg.CachePrepStmts = false
			cfg.MultiStmtEnable = false
			cfg.BatchDMLEnable = false
			cfg.SafeMode = false
			cfg.DMLMaxRetry = 1
			cfg.TxnOrderFilter, err = config.ParseTxnOrderTables([]string{"test.ordered"}, false)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			sink := NewMySQLSink(ctx, common.NewChangefeedID4Test("test", fmt.Sprint(workers)), cfg, db, false, false, time.Minute, common.DefaultKeyspaceID)
			done := make(chan error, 1)
			t.Cleanup(func() {
				cancel()
				sink.conflictDetector.Close()
				<-done
				mock.ExpectClose()
				sink.Close()
				require.NoError(t, mock.ExpectationsWereMet())
			})

			const hotCount = 2100 // Exceed the conflict detector's 1024-entry worker cache.
			flushed := make(chan struct{}, hotCount+2)
			for i := 0; i < hotCount+2; i++ {
				template := hot
				if i == hotCount {
					template = cold
				}
				if i == hotCount+1 {
					template = bulk
				}
				e := *template
				e.CommitTs = uint64(i + 1)
				e.PostTxnFlushed = []func(){func() { flushed <- struct{}{} }}
				mock.ExpectBegin()
				if i == hotCount+1 {
					for id := 3; id <= 7; id++ {
						mock.ExpectExec("INSERT INTO "+"`test`.`ordered` (`id`,`v`) VALUES (?,?)").WithArgs(id, 1).WillReturnResult(sqlmock.NewResult(0, 1))
					}
				} else {
					id := 1
					if i == hotCount {
						id = 2
					}
					mock.ExpectExec("INSERT INTO "+"`test`.`ordered` (`id`,`v`) VALUES (?,?)").WithArgs(id, 1).WillReturnResult(sqlmock.NewResult(0, 1))
				}
				mock.ExpectCommit()
				sink.AddDMLEvent(&e)
			}
			go func() { done <- sink.Run(ctx) }()
			timeout := time.NewTimer(30 * time.Second)
			defer timeout.Stop()
			for i := 0; i < hotCount+2; i++ {
				select {
				case <-flushed:
				case err := <-done:
					done <- err
					require.NoError(t, err)
					t.Fatal("sink stopped before flushing its backlog")
				case <-timeout.C:
					t.Fatal("timed out waiting for ordered transactions")
				}
			}
		})
	}
}
