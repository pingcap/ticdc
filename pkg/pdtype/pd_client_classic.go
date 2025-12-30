// Copyright 2025 PingCAP, Inc.
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

//go:build !nextgen

package pdtype

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

func GetKeyspaceMetaByID(ctx context.Context, client pdhttp.Client, keyspaceID uint32) (*keyspacepb.KeyspaceMeta, error) {
	return &keyspacepb.KeyspaceMeta{}, nil
}

func CollectMemberEndpoints(ctx context.Context, grpcClient pd.Client) ([]string, error) {
	members, err := grpcClient.GetAllMembers(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	result := make([]string, 0, len(members))
	for _, m := range members {
		clientUrls := m.GetClientUrls()
		if len(clientUrls) > 0 {
			result = append(result, clientUrls[0])
		}
	}
	return result, nil
}
