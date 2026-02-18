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

package sqlmodel

import (
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/stretchr/testify/require"
)

var (
	sharedTableInfo     *common.TableInfo
	sharedTableInfoOnce sync.Once
)

// getSharedTableInfo returns a TableInfo for schema "test", table "t" with
// columns (id int primary key, name varchar(32)). It uses a single
// EventTestHelper for the entire package to avoid the "duplicate metrics
// collector registration" panic that occurs when multiple TiDB domains are
// bootstrapped in the same process.
func getSharedTableInfo(t *testing.T) *common.TableInfo {
	sharedTableInfoOnce.Do(func() {
		helper := commonEvent.NewEventTestHelper(t)
		// We intentionally do not close the helper. Closing the TiDB domain
		// does not unregister prometheus metrics from the global registry, so
		// a subsequent BootstrapSession would panic.

		helper.Tk().MustExec("use test")
		job := helper.DDL2Job("create table t (id int primary key, name varchar(32));")
		require.NotNil(t, job)

		event := helper.DML2Event("test", "t", "insert into t values (1, 'dummy');")
		require.NotNil(t, event)
		sharedTableInfo = event.TableInfo
	})
	return sharedTableInfo
}
