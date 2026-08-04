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

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

type verifyGCSafepointTestPDClient struct {
	physical         int64
	logical          int64
	keyspaceID       uint32
	setServiceID     string
	setKeyspaceID    uint32
	setTTL           int64
	setSafePoint     uint64
	minSafePoint     uint64
	deleteServiceID  string
	deleteKeyspaceID uint32
	deleteContextErr error
	deleteErr        error
	closed           bool
}

func (c *verifyGCSafepointTestPDClient) GetTS(context.Context) (int64, int64, error) {
	return c.physical, c.logical, nil
}

func (c *verifyGCSafepointTestPDClient) LoadKeyspace(context.Context, string) (*keyspacepb.KeyspaceMeta, error) {
	return &keyspacepb.KeyspaceMeta{Id: c.keyspaceID}, nil
}

func (*verifyGCSafepointTestPDClient) GetMinServiceSafePointV2(context.Context, uint32) (uint64, error) {
	return 0, nil
}

func (c *verifyGCSafepointTestPDClient) SetServiceSafePointV2(
	_ context.Context, keyspaceID uint32, serviceID string, ttl int64, safePoint uint64,
) (uint64, error) {
	c.setKeyspaceID = keyspaceID
	c.setServiceID = serviceID
	c.setTTL = ttl
	c.setSafePoint = safePoint
	if c.minSafePoint != 0 {
		return c.minSafePoint, nil
	}
	return safePoint, nil
}

func (c *verifyGCSafepointTestPDClient) DeleteServiceSafePointV2(
	ctx context.Context, keyspaceID uint32, serviceID string,
) (uint64, error) {
	c.deleteKeyspaceID = keyspaceID
	c.deleteServiceID = serviceID
	c.deleteContextErr = ctx.Err()
	return 0, c.deleteErr
}

func (c *verifyGCSafepointTestPDClient) Close() {
	c.closed = true
}

func TestVerifyGCSafepointRun(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			physical:   1000,
			logical:    7,
			keyspaceID: 42,
		}
		snapshotTS := oracle.ComposeTS(pdClient.physical, pdClient.logical)
		o := &verifyGCSafepointOptions{
			keyspace:     "essential-v1",
			pdClient:     pdClient,
			legacyClient: pdClient,
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
		require.Equal(t, snapshotTS+1, pdClient.setSafePoint)
		require.Equal(t, uint32(42), pdClient.setKeyspaceID)
		require.Equal(t, verifyGCSafepointTTLSeconds, pdClient.setTTL)
		require.Equal(t, pdClient.setServiceID, pdClient.deleteServiceID)
		require.Equal(t, uint32(42), pdClient.deleteKeyspaceID)
		require.NoError(t, pdClient.deleteContextErr)
		require.True(t, pdClient.closed)
		require.Contains(t, output.String(), "databases: 3")
	})

	t.Run("cleanup uses fresh context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		pdClient := &verifyGCSafepointTestPDClient{physical: 1000, keyspaceID: 42}
		o := &verifyGCSafepointOptions{
			keyspace:     "essential-v1",
			pdClient:     pdClient,
			legacyClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				cancel()
				return 0, cerrors.WrapError(cerrors.ErrMetaListDatabases, stderrors.New("snapshot read failed"))
			},
		}
		cmd := &cobra.Command{}
		cmd.SetContext(ctx)

		err := o.run(cmd)
		require.ErrorContains(t, err, "meta store list databases")
		require.NoError(t, pdClient.deleteContextErr)
		require.True(t, pdClient.closed)
	})

	t.Run("rejected safepoint skips snapshot read", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{physical: 1000, keyspaceID: 42}
		snapshotTS := oracle.ComposeTS(pdClient.physical, pdClient.logical)
		pdClient.minSafePoint = snapshotTS + 2
		listCalled := false
		o := &verifyGCSafepointOptions{
			keyspace:     "essential-v1",
			pdClient:     pdClient,
			legacyClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				listCalled = true
				return 0, nil
			},
		}

		err := o.run(&cobra.Command{})
		require.ErrorContains(t, err, "minimum service safepoint")
		require.False(t, listCalled)
		require.Equal(t, pdClient.setServiceID, pdClient.deleteServiceID)
		require.True(t, pdClient.closed)
	})

	t.Run("cleanup failure is returned", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			physical:   1000,
			keyspaceID: 42,
			deleteErr:  stderrors.New("delete failed"),
		}
		o := &verifyGCSafepointOptions{
			keyspace:     "essential-v1",
			pdClient:     pdClient,
			legacyClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				return 3, nil
			},
		}

		err := o.run(&cobra.Command{})
		require.ErrorContains(t, err, "delete service safepoint failed")
		require.True(t, pdClient.closed)
	})
}
