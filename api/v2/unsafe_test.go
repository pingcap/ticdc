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

package v2

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang/mock/gomock"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

// The mock store supports exclusive locks. Supply a shared wrapper on ScanLock
// and record the individual rollback RPCs sent for its two secondary holders.
type sharedHolderClient struct {
	tikv.Client
	rolledBack map[uint64]string
}

func (c *sharedHolderClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if err != nil {
		return nil, err
	}
	switch req.Type {
	case tikvrpc.CmdScanLock:
		page := resp.Resp.(*kvrpcpb.ScanLockResponse)
		scan := req.ScanLock()
		key := []byte("s")
		if bytes.Compare(key, scan.StartKey) < 0 || len(scan.EndKey) > 0 && bytes.Compare(key, scan.EndKey) >= 0 {
			break
		}
		wrapper := &kvrpcpb.LockInfo{LockType: kvrpcpb.Op_SharedLock}
		for _, member := range []*kvrpcpb.LockInfo{
			{Key: key, PrimaryLock: []byte("p1"), LockVersion: 30, LockType: kvrpcpb.Op_PessimisticLock},
			{Key: key, PrimaryLock: []byte("p2"), LockVersion: 40, LockType: kvrpcpb.Op_Lock},
		} {
			if _, ok := c.rolledBack[member.LockVersion]; ok {
				continue
			}
			if scan.Limit > 0 && len(page.Locks)+len(wrapper.SharedLockInfos) >= int(scan.Limit) {
				break
			}
			wrapper.SharedLockInfos = append(wrapper.SharedLockInfos, member)
		}
		if len(wrapper.SharedLockInfos) > 0 {
			page.Locks = append(page.Locks, wrapper)
		}
	case tikvrpc.CmdPessimisticRollback:
		r := req.PessimisticRollback()
		for _, key := range r.Keys {
			if string(key) == "s" {
				c.rolledBack[r.StartVersion] = string(key)
			}
		}
	case tikvrpc.CmdResolveLock:
		r := req.ResolveLock()
		for _, key := range r.Keys {
			if string(key) == "s" {
				c.rolledBack[r.StartVersion] = string(key)
			}
		}
	}
	return resp, nil
}

func TestUnsafeResolveLockCleansUpLockOnlyTransactions(t *testing.T) {
	var regionID uint64
	rolledBack := make(map[uint64]string)
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) { _, _, regionID = mockstore.BootstrapWithSingleStore(c) }),
		mockstore.WithClientHijacker(func(c tikv.Client) tikv.Client { return &sharedHolderClient{c, rolledBack} }),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	s := store.(tikv.Storage)
	bo := tikv.NewGcResolveLockMaxBackoffer(context.Background())
	loc, err := s.GetRegionCache().LocateRegionByID(bo, regionID)
	require.NoError(t, err)
	send := func(req *tikvrpc.Request) *tikvrpc.Response {
		resp, err := s.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		require.NoError(t, err)
		return resp
	}
	pess := send(tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &kvrpcpb.PessimisticLockRequest{
		Mutations:   []*kvrpcpb.Mutation{{Op: kvrpcpb.Op_PessimisticLock, Key: []byte("a")}},
		PrimaryLock: []byte("a"), StartVersion: 10, ForUpdateTs: 10, LockTtl: 1,
	}))
	require.Empty(t, pess.Resp.(*kvrpcpb.PessimisticLockResponse).Errors)
	// The pessimistic primary and these prewrites leave room for only one
	// of the two shared holders in the first 1024-holder page.
	var mutations []*kvrpcpb.Mutation
	for i := range 1022 {
		mutations = append(mutations, &kvrpcpb.Mutation{Op: kvrpcpb.Op_Lock, Key: []byte(fmt.Sprintf("b%04d", i))})
	}
	prewrite := send(tikvrpc.NewRequest(tikvrpc.CmdPrewrite, &kvrpcpb.PrewriteRequest{
		Mutations: mutations, PrimaryLock: mutations[0].Key, StartVersion: 20, LockTtl: 1,
	}))
	require.Empty(t, prewrite.Resp.(*kvrpcpb.PrewriteResponse).Errors)
	scan := func() []*kvrpcpb.LockInfo {
		resp := send(tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{Limit: 1024, MaxVersion: 100}))
		return resp.Resp.(*kvrpcpb.ScanLockResponse).GetLocks()
	}
	page := scan()
	require.Len(t, page, 1024)
	require.Len(t, page[len(page)-1].SharedLockInfos, 1)

	meta := &keyspacepb.KeyspaceMeta{Name: "test"}
	manager := keyspace.NewMockManager(gomock.NewController(t))
	manager.EXPECT().GetKeyspaceByID(gomock.Any(), meta.GetId()).Return(meta, nil)
	manager.EXPECT().GetStorage(gomock.Any(), meta.Name).Return(store, nil)
	previous, _ := appcontext.TryGetService[keyspace.Manager](appcontext.KeyspaceManager)
	defer appcontext.SetService(appcontext.KeyspaceManager, previous)
	appcontext.SetService[keyspace.Manager](appcontext.KeyspaceManager, manager)
	w := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(w)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/api/v2/unsafe/resolve_lock",
		strings.NewReader(fmt.Sprintf(`{"region_id":%d,"ts":100}`, regionID)))
	new(OpenAPIV2).ResolveLock(ctx)
	require.Empty(t, ctx.Errors)
	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, map[uint64]string{30: "s", 40: "s"}, rolledBack)
	require.Empty(t, scan())
}
