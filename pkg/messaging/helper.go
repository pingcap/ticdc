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

package messaging

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/phayes/freeport"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func NewMessageCenterForTest(t *testing.T) (*messageCenter, string, func()) {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	mcConfig := config.NewDefaultMessageCenterConfig(addr)
	id := node.NewID()
	mc := NewMessageCenter(ctx, id, mcConfig, nil)
	mcs := NewMessageCenterServer(mc)
	proto.RegisterMessageServiceServer(grpcServer, mcs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	stop := func() {
		grpcServer.Stop()
		cancel()
		wg.Wait()
	}
	return mc, string(addr), stop
}
