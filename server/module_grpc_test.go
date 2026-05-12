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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/diagnosticspb"
	appcontext "github.com/pingcap/ticdc/pkg/common/context"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/messaging"
	"github.com/pingcap/ticdc/pkg/node"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestGrpcModule_SearchLog(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "ticdc.log")
	logContent := "[2025/12/30 00:00:00.000 +00:00] [INFO] hello world\n" +
		"[2025/12/30 00:00:01.000 +00:00] [ERROR] something bad\n"
	err := os.WriteFile(logPath, []byte(logContent), 0o600)
	require.NoError(t, err)

	originalConfig := config.GetGlobalServerConfig()
	cfg := originalConfig.Clone()
	cfg.LogFile = logPath
	config.StoreGlobalServerConfig(cfg)
	t.Cleanup(func() {
		config.StoreGlobalServerConfig(originalConfig)
	})

	mcCfg := config.NewDefaultMessageCenterConfig("127.0.0.1:0")
	mc := messaging.NewMessageCenter(context.Background(), node.ID("test-node"), mcCfg, &security.Credential{})
	appcontext.SetService(appcontext.MessageCenter, mc)

	lis := bufconn.Listen(1024 * 1024)
	m := NewGrpcServer(lis).(*GrpcModule)
	t.Cleanup(func() {
		m.grpcServer.Stop()
	})
	go func() {
		_ = m.grpcServer.Serve(lis)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	cli := diagnosticspb.NewDiagnosticsClient(conn)
	stream, err := cli.SearchLog(ctx, &diagnosticspb.SearchLogRequest{
		StartTime: 0,
		EndTime:   0,
		Levels:    []diagnosticspb.LogLevel{diagnosticspb.LogLevel_Info},
		Patterns:  []string{"(?i)hello"},
	})
	require.NoError(t, err)

	resp, err := stream.Recv()
	require.NoError(t, err)
	require.NotEmpty(t, resp.Messages)

	found := false
	for _, msg := range resp.Messages {
		if strings.Contains(msg.Message, "hello world") {
			found = true
			break
		}
	}
	require.True(t, found, "expected log message to contain hello world")
}
