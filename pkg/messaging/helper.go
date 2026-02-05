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
	"net"
	"sync"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"github.com/pingcap/ticdc/pkg/node"
	"google.golang.org/grpc"
)

func NewMessageCenterForTest(t *testing.T) (*messageCenter, string, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		// Some sandboxed environments disallow binding to local TCP ports. Fall back
		// to a local-only message center which is sufficient for most unit tests.
		t.Logf("failed to listen on 127.0.0.1:0, falling back to local-only message center: %v", err)

		ctx, cancel := context.WithCancel(context.Background())
		mcConfig := config.NewDefaultMessageCenterConfig("")
		id := node.NewID()
		mc := NewMessageCenter(ctx, id, mcConfig, nil)
		mc.Run(ctx)
		stop := func() {
			cancel()
			mc.Close()
		}
		return mc, "", stop
	}

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	ctx, cancel := context.WithCancel(context.Background())
	addr := lis.Addr().String()
	mcConfig := config.NewDefaultMessageCenterConfig(addr)
	id := node.NewID()
	mc := NewMessageCenter(ctx, id, mcConfig, nil)
	mcs := NewMessageCenterServer(mc)
	proto.RegisterMessageServiceServer(grpcServer, mcs)

	mc.Run(ctx)
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
		mc.Close()
	}
	return mc, string(addr), stop
}
