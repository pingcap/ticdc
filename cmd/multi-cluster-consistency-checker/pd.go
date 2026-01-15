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

package main

import (
	"context"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/security"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
	"google.golang.org/grpc"
)

func newClient(ctx context.Context, pdAddr string, securityConfig *security.Credential) (pd.Client, *etcd.CDCEtcdClientImpl, error) {
	pdClient, err := pd.NewClientWithContext(
		ctx, "consistency-checker", []string{pdAddr}, securityConfig.PDSecurityOption(),
		pdopt.WithCustomTimeoutOption(10*time.Second),
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	etcdCli, err := etcd.CreateRawEtcdClient(securityConfig, grpc.EmptyDialOption{}, pdAddr)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	cdcEtcdClient, err := etcd.NewCDCEtcdClient(ctx, etcdCli, "default")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return pdClient, cdcEtcdClient, nil
}
