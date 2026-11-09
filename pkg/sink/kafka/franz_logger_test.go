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

package kafka

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestClientLoggerConfiguration covers the franz-go level mapping.
func TestClientLoggerConfiguration(t *testing.T) {
	restore := log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{
		Core:  zapcore.NewNopCore(),
		Level: zap.NewAtomicLevelAt(log.GetLevel()),
	})
	defer restore()

	clientLogger := newClientLogger(
		common.NewChangefeedID4Test("test-keyspace", "test-changefeed"))

	log.SetLevel(zapcore.InfoLevel)
	require.Equal(t, kgo.LogLevelWarn, clientLogger.Level())
	log.SetLevel(zapcore.DebugLevel)
	require.Equal(t, kgo.LogLevelInfo, clientLogger.Level())
}
