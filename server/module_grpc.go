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

package server

import (
	"context"
	"net"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/messaging/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

type GrpcModule struct {
	grpcServer *grpc.Server
	lis        net.Listener
}

func NewGrpcServer(lis net.Listener) common.SubModule {
	option := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(256 * 1024 * 1024), // 256MB
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             20 * time.Second,
		PermitWithoutStream: true,
	}

	kasp := keepalive.ServerParameters{
		Time:    1 * time.Minute,
		Timeout: 1 * time.Minute,
	}

	option = append(option, grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))

	grpcServer := grpc.NewServer(option...)
	proto.RegisterMessageServiceServer(grpcServer, messaging.NewMessageCenterServer(appcontext.GetService[messaging.MessageCenter](appcontext.MessageCenter)))
	return &GrpcModule{
		grpcServer: grpcServer,
		lis:        lis,
	}
}

func (g *GrpcModule) Run(ctx context.Context) error {
	log.Info("grpc server start to serve")
	defer func() {
		log.Info("grpc server exited")
	}()
	// we must to exit if the context is done.
	ch := make(chan error)
	go func() {
		err := g.grpcServer.Serve(g.lis)
		if err != nil {
			log.Error("grpc server error", zap.Error(err))
		}
		ch <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (g *GrpcModule) Close(ctx context.Context) error {
	g.grpcServer.Stop()
	return nil
}

func (g *GrpcModule) Name() string {
	return "grpc"
}
