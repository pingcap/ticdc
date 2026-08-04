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

package cli

import (
	"bytes"
	"context"
	stderrors "errors"
	"testing"

	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type verifyGCSafepointTestPDClient struct {
	physical int64
	logical  int64
	closed   bool
}

func (c *verifyGCSafepointTestPDClient) GetTS(context.Context) (int64, int64, error) {
	return c.physical, c.logical, nil
}

func (c *verifyGCSafepointTestPDClient) Close() {
	c.closed = true
}

func TestVerifyGCSafepointRun(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			physical: 1000,
			logical:  7,
		}
		snapshotTS := oracle.ComposeTS(pdClient.physical, pdClient.logical)
		o := &verifyGCSafepointOptions{
			keyspace: "essential-v1",
			pdClient: pdClient,
			listDatabases: func(_ context.Context, keyspace string, ts uint64) (int, error) {
				require.Equal(t, "essential-v1", keyspace)
				require.Equal(t, snapshotTS, ts)
				return 3, nil
			},
		}
		cmd := &cobra.Command{}
		output := new(bytes.Buffer)
		cmd.SetOut(output)

		require.NoError(t, o.run(cmd))
		require.True(t, pdClient.closed)
		require.Contains(t, output.String(), "WARNING: no service safepoint blocks GC safepoint advancement")
		require.Contains(t, output.String(), "databases: 3")
	})

	t.Run("snapshot error is returned", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{physical: 1000}
		o := &verifyGCSafepointOptions{
			keyspace: "essential-v1",
			pdClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				return 0, cerrors.WrapError(cerrors.ErrMetaListDatabases, stderrors.New("snapshot read failed"))
			},
		}
		cmd := &cobra.Command{}
		output := new(bytes.Buffer)
		cmd.SetOut(output)

		err := o.run(cmd)
		require.ErrorContains(t, err, "meta store list databases")
		require.True(t, pdClient.closed)
		require.Contains(t, output.String(), "may fail with a safepoint error")
	})
}
