// Copyright 2023 PingCAP, Inc.
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

package pdutil

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
)

func TestGetSourceID(t *testing.T) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	domain, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer domain.Close()
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	_, err = se.Execute(context.Background(), "set @@global.tidb_source_id=2;")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		client := store.(kv.StorageWithPD).GetPDClient()
		sourceID, err := GetSourceID(context.Background(), client)
		require.NoError(t, err)
		return sourceID == 2
	}, 5*time.Second, 100*time.Millisecond)
}

func TestAdvanceChangefeedEpoch(t *testing.T) {
	t.Parallel()

	epoch, err := AdvanceChangefeedEpoch(10, 8)
	require.NoError(t, err)
	require.Equal(t, uint64(10), epoch)

	epoch, err = AdvanceChangefeedEpoch(10, 12)
	require.NoError(t, err)
	require.Equal(t, uint64(13), epoch)

	_, err = AdvanceChangefeedEpoch(10, ^uint64(0))
	require.Error(t, err)
	require.ErrorContains(t, err, "changefeed epoch overflow")
}
