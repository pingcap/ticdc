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

package redo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestTxnOrderConfig(t *testing.T) {
	for _, tc := range []struct {
		rule    string
		invalid bool
	}{
		{"APP.orders", false}, {"invalid", true},
	} {
		path := filepath.Join(t.TempDir(), "changefeed.toml")
		require.NoError(t, os.WriteFile(path, []byte("case-sensitive = true\n[sink.mysql-config]\nworker-count = 7\ntxn-order-tables = [\""+tc.rule+"\"]\n"), 0o600))
		o := newapplyRedoOptions()
		cmd := &cobra.Command{}
		o.addFlags(cmd)
		require.NoError(t, cmd.Flags().Set("config", path))
		require.NoError(t, cmd.Flags().Set("sink-uri", "mysql://127.0.0.1/?safe-mode=false"))
		err := o.complete(cmd)
		if tc.invalid {
			require.ErrorContains(t, err, "txn-order-tables")
		} else {
			require.NoError(t, err)
			require.Equal(t, []string{tc.rule}, o.cfg.Sink.MySQLConfig.TxnOrderTables)
			require.Equal(t, 7, *o.cfg.Sink.MySQLConfig.WorkerCount)
			require.True(t, util.GetOrZero(o.cfg.CaseSensitive))
			require.Contains(t, o.sinkURI, "safe-mode=true")
		}
	}
}
