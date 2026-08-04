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

	"github.com/pingcap/ticdc/cmd/cdc/factory"
	"github.com/pingcap/ticdc/cmd/util"
	"github.com/pingcap/ticdc/pkg/common"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/security"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/store/driver"
	"github.com/spf13/cobra"
	tikvconfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/oracle"
)

type verifyGCSafepointPDClient interface {
	GetTS(ctx context.Context) (physical int64, logical int64, err error)
	Close()
}

type verifyGCSafepointOptions struct {
	keyspace      string
	pdClient      verifyGCSafepointPDClient
	listDatabases func(ctx context.Context, keyspace string, snapshotTS uint64) (int, error)
}

func (o *verifyGCSafepointOptions) addFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(&o.keyspace, "keyspace", "k", "", "Keyspace to verify")
	_ = cmd.MarkFlagRequired("keyspace")
}

func (o *verifyGCSafepointOptions) complete(f factory.Factory) error {
	pdClient, err := f.PdClient()
	if err != nil {
		return err
	}
	o.pdClient = pdClient
	o.listDatabases = func(ctx context.Context, keyspace string, snapshotTS uint64) (int, error) {
		return listDatabasesAtSnapshot(ctx, f.GetPdAddr(), f.GetCredential(), keyspace, snapshotTS)
	}
	return nil
}

func (o *verifyGCSafepointOptions) run(cmd *cobra.Command) error {
	defer o.pdClient.Close()
	ctx := cmd.Context()
	physical, logical, err := o.pdClient.GetTS(ctx)
	if err != nil {
		return cerrors.WrapError(cerrors.ErrPDEtcdAPIError, err)
	}
	snapshotTS := oracle.ComposeTS(physical, logical)
	cmd.Printf("WARNING: no service safepoint blocks GC safepoint advancement; ListDatabases at snapshot %d may fail with a safepoint error.\n", snapshotTS)

	databaseCount, err := o.listDatabases(ctx, o.keyspace, snapshotTS)
	if err != nil {
		return err
	}
	cmd.Printf("GC safepoint and ListDatabases verified, snapshot-ts: %d, databases: %d\n", snapshotTS, databaseCount)
	return nil
}

func listDatabasesAtSnapshot(
	ctx context.Context,
	pdAddr string,
	credential *security.Credential,
	keyspace string,
	snapshotTS uint64,
) (int, error) {
	pdURLs, err := common.NewURLsValue(pdAddr)
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrNewStore, err)
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
		return 0, cerrors.WrapError(cerrors.ErrNewStore, err)
	}
	defer func() { _ = tiStore.Close() }()

	databases, err := meta.NewReader(tiStore.GetSnapshot(tidbkv.NewVersion(snapshotTS))).ListDatabases()
	if err != nil {
		return 0, cerrors.WrapError(cerrors.ErrMetaListDatabases, err)
	}
	return len(databases), nil
}

func newCmdVerifyGCSafepoint(f factory.Factory) *cobra.Command {
	o := &verifyGCSafepointOptions{}
	command := &cobra.Command{
		Use:   "verify-gc-safepoint",
		Short: "Verify keyspace GC safepoint protection by listing databases at a protected snapshot",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			util.CheckErr(o.complete(f))
			util.CheckErr(o.run(cmd))
		},
	}
	o.addFlags(command)
	return command
}
