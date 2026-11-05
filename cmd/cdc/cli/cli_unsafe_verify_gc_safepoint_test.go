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
	"testing"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type verifyGCSafepointTestPDClient struct {
	pdgc.LegacyClientV2
	keyspaceID          uint32
	txnSafePoint        uint64
	gcSafePoint         uint64
	legacySafePoint     uint64
	loadedKeyspace      string
	requestedKeyspaceID uint32
	legacyKeyspaceID    uint32
	gcStateRequested    bool
	closed              bool
}

func (c *verifyGCSafepointTestPDClient) LoadKeyspace(_ context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error) {
	c.loadedKeyspace = keyspace
	return &keyspacepb.KeyspaceMeta{Keyspace: &keyspacepb.KeyspaceMeta_Id{Id: c.keyspaceID}}, nil
}

func (c *verifyGCSafepointTestPDClient) GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient {
	c.gcStateRequested = true
	c.requestedKeyspaceID = keyspaceID
	return &verifyGCSafepointTestGCStatesClient{
		gcState: pdgc.NewGCStateWithoutGCBarriers(
			keyspaceID,
			c.txnSafePoint,
			c.gcSafePoint,
		),
	}
}

func (c *verifyGCSafepointTestPDClient) GetMinServiceSafePointV2(_ context.Context, keyspaceID uint32) (uint64, error) {
	c.legacyKeyspaceID = keyspaceID
	return c.legacySafePoint, nil
}

func (c *verifyGCSafepointTestPDClient) Close() {
	c.closed = true
}

type verifyGCSafepointTestGCStatesClient struct {
	pdgc.GCStatesClient
	gcState pdgc.GCState
}

func (c *verifyGCSafepointTestGCStatesClient) GetGCState(context.Context, ...pdgc.GCStatesAPIOption) (pdgc.GCState, error) {
	return c.gcState, nil
}

func TestVerifyGCSafepointRun(t *testing.T) {
	t.Run("uses the current keyspace transaction safepoint", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			keyspaceID:   42,
			txnSafePoint: 123,
			gcSafePoint:  100,
		}
		o := &verifyGCSafepointOptions{
			keyspace: "essential-v1",
			pdClient: pdClient,
			listDatabases: func(_ context.Context, keyspace string, ts uint64) (int, error) {
				require.Equal(t, "essential-v1", keyspace)
				require.Equal(t, pdClient.txnSafePoint, ts)
				return 3, nil
			},
		}
		cmd := &cobra.Command{}
		output := new(bytes.Buffer)
		cmd.SetOut(output)

		require.NoError(t, o.run(cmd))
		require.Equal(t, "essential-v1", pdClient.loadedKeyspace)
		require.Equal(t, pdClient.keyspaceID, pdClient.requestedKeyspaceID)
		require.True(t, pdClient.closed)
		require.Contains(t, output.String(), "WARNING: this verification holds no service safepoint")
		require.Contains(t, output.String(), "databases: 3")
	})

	t.Run("uses the legacy minimum service safepoint when requested", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			keyspaceID:      42,
			txnSafePoint:    123,
			legacySafePoint: 456,
		}
		o := &verifyGCSafepointOptions{
			keyspace:        "essential-v1",
			legacySafepoint: true,
			pdClient:        pdClient,
			listDatabases: func(_ context.Context, keyspace string, ts uint64) (int, error) {
				require.Equal(t, "essential-v1", keyspace)
				require.Equal(t, pdClient.legacySafePoint, ts)
				return 3, nil
			},
		}
		cmd := &cobra.Command{}
		cmd.SetContext(context.Background())
		cmd.SetOut(new(bytes.Buffer))

		require.NoError(t, o.run(cmd))
		require.Equal(t, pdClient.keyspaceID, pdClient.legacyKeyspaceID)
		require.False(t, pdClient.gcStateRequested)
		require.True(t, pdClient.closed)
	})

	t.Run("snapshot error is returned", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			keyspaceID:   42,
			txnSafePoint: 123,
		}
		o := &verifyGCSafepointOptions{
			keyspace: "essential-v1",
			pdClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				return 0, errors.WrapError(errors.ErrMetaListDatabases, context.Canceled)
			},
		}
		cmd := &cobra.Command{}
		output := new(bytes.Buffer)
		cmd.SetOut(output)

		err := o.run(cmd)
		require.ErrorContains(t, err, "meta store list databases")
		require.True(t, pdClient.closed)
		require.Contains(t, output.String(), "before the read completes")
	})

	t.Run("zero safepoint is rejected", func(t *testing.T) {
		pdClient := &verifyGCSafepointTestPDClient{
			keyspaceID:   42,
			txnSafePoint: 0,
		}
		o := &verifyGCSafepointOptions{
			keyspace: "essential-v1",
			pdClient: pdClient,
			listDatabases: func(context.Context, string, uint64) (int, error) {
				t.Fatal("listDatabases must not be called with a zero safepoint")
				return 0, nil
			},
		}
		cmd := &cobra.Command{}
		cmd.SetOut(new(bytes.Buffer))

		err := o.run(cmd)
		require.ErrorContains(t, err, "safepoint is zero")
		require.True(t, pdClient.closed)
	})
}

func TestVerifyGCSafepointFlags(t *testing.T) {
	o := &verifyGCSafepointOptions{}
	cmd := &cobra.Command{}
	o.addFlags(cmd)

	require.NoError(t, cmd.ParseFlags([]string{"--legacy-safepoint"}))
	require.True(t, o.legacySafepoint)
}
