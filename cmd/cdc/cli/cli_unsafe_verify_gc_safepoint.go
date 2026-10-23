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
	"context"
	"net/url"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/upstream"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/spf13/cobra"
	tikvconfig "github.com/tikv/client-go/v2/config"
	pdgc "github.com/tikv/pd/client/clients/gc"
)

type verifyGCSafepointPDClient interface {
	LoadKeyspace(ctx context.Context, keyspace string) (*keyspacepb.KeyspaceMeta, error)
	GetGCStatesClient(keyspaceID uint32) pdgc.GCStatesClient
	Close()
}

type verifyGCSafepointOptions struct {
	keyspace        string
	legacySafepoint bool
	pdClient        verifyGCSafepointPDClient
	listDatabases   func(ctx context.Context, keyspace string, snapshotTS uint64) (int, error)
}

func (o *verifyGCSafepointOptions) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.keyspace, "keyspace", "k", "", "Keyspace to verify")
	cmd.Flags().BoolVar(&o.legacySafepoint, "legacy-safepoint", false, "Read the keyspace-v2 minimum service safepoint")
	_ = cmd.MarkFlagRequired("keyspace")
}

func (o *verifyGCSafepointOptions) complete(f factory.Factory) error {
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	o.pdClient = pdClient
	o.listDatabases = func(_ context.Context, keyspace string, snapshotTS uint64) (int, error) {
		return listDatabasesAtSnapshot(f.GetPdAddr(), f.GetCredential(), keyspace, snapshotTS)
	}
	return nil
}

func (o *verifyGCSafepointOptions) run(cmd *cobra.Command) error {
	defer o.pdClient.Close()
	ctx := cmd.Context()
	keyspaceMeta, err := o.pdClient.LoadKeyspace(ctx, o.keyspace)
	if err != nil {
		return errors.WrapError(errors.ErrLoadKeyspaceFailed, err)
	}

	var snapshotTS uint64
	if o.legacySafepoint {
		legacyClient, ok := o.pdClient.(pdgc.LegacyClientV2)
		if !ok {
			return errors.ErrGetServiceSafepointFailed.GenWithStackByArgs("PD client does not support LegacyClientV2")
		}
		snapshotTS, err = legacyClient.GetMinServiceSafePointV2(ctx, keyspaceMeta.GetId())
		if err != nil {
			return errors.WrapError(errors.ErrGetServiceSafepointFailed, err)
		}
	} else {
		gcState, err := o.pdClient.GetGCStatesClient(keyspaceMeta.GetId()).GetGCState(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		// Schema store uses the transaction safepoint as its initial metadata snapshot.
		snapshotTS = gcState.TxnSafePoint
	}
	if snapshotTS == 0 {
		return errors.ErrGetServiceSafepointFailed.GenWithStackByArgs(
			"safepoint is zero, keyspace GC state may be uninitialized")
	}
	cmd.Printf("WARNING: this verification holds no service safepoint; GC may advance past snapshot %d before the read completes.\n", snapshotTS)

	databaseCount, err := o.listDatabases(ctx, o.keyspace, snapshotTS)
	if err != nil {
		return err
	}
	cmd.Printf("GC safepoint and ListDatabases verified, snapshot-ts: %d, databases: %d\n", snapshotTS, databaseCount)
	return nil
}

func listDatabasesAtSnapshot(
	pdAddr string,
	credential *security.Credential,
	keyspace string,
	snapshotTS uint64,
) (int, error) {
	pdURLs, err := common.NewURLsValue(pdAddr)
	if err != nil {
		return 0, errors.WrapError(errors.ErrNewStore, err)
	}
	tiURL := &url.URL{Scheme: "tikv", Host: pdURLs.HostString()}
	query := tiURL.Query()
	query.Set("disableGC", "true")
	query.Set("keyspaceName", keyspace)
	tiURL.RawQuery = query.Encode()

	securityConfig := tikvconfig.Security{
		ClusterSSLCA:    credential.CAPath,
		ClusterSSLCert:  credential.CertPath,
		ClusterSSLKey:   credential.KeyPath,
		ClusterVerifyCN: credential.CertAllowedCN,
	}
	tiStore, err := (&driver.TiKVDriver{}).OpenWithOptions(
		tiURL.String(), driver.WithSecurity(securityConfig),
	)
	if err != nil {
		return 0, errors.WrapError(errors.ErrNewStore, err)
	}
	defer func() { _ = tiStore.Close() }()
	if err := upstream.DisablePDRouterClient(tiStore); err != nil {
		return 0, errors.WrapError(errors.ErrNewStore, err)
	}

	databases, err := meta.NewReader(tiStore.GetSnapshot(tidbkv.NewVersion(snapshotTS))).ListDatabases()
	if err != nil {
		return 0, errors.WrapError(errors.ErrMetaListDatabases, err)
	}
	return len(databases), nil
}

func newCmdVerifyGCSafepoint(f factory.Factory) *cobra.Command {
	o := &verifyGCSafepointOptions{}
	command := &cobra.Command{
		Use:   "verify-gc-safepoint",
		Short: "Verify a keyspace GC safepoint by listing databases at its current snapshot",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}
	o.addFlags(command)
	return command
}
