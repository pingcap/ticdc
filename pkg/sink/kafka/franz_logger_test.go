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
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestClientLoggerConfiguration covers what newClientLogger decides for the
// franz-go client: the level mapping, the fields identifying the changefeed,
// the unchanged pass-through of client fields, and the sampler.
func TestClientLoggerConfiguration(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	// The properties must be non-nil so the test can change the global level.
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
		Core:  core,
		Level: zap.NewAtomicLevelAt(log.GetLevel()),
	})
	defer restore()

	// The logger must be built after ReplaceGlobals to observe its output.
	clientLogger := newClientLogger(
		common.NewChangefeedID4Test("test-keyspace", "test-changefeed"))

	log.SetLevel(zapcore.InfoLevel)
	require.Equal(t, kgo.LogLevelWarn, clientLogger.Level())
	log.SetLevel(zapcore.DebugLevel)
	require.Equal(t, kgo.LogLevelInfo, clientLogger.Level())

	clientLogger.Log(
		kgo.LogLevelWarn,
		"connection failed",
		"broker", "127.0.0.1:9092",
		"payload", strings.Repeat("x", 2048),
		"odd",
	)

	entries := logs.FilterMessage("connection failed").AllUntimed()
	require.Len(t, entries, 1)

	fields := entries[0].ContextMap()
	require.Equal(t, "kafka-client", fields["component"])
	require.Equal(t, "test-keyspace", fields["keyspace"])
	require.Equal(t, "test-changefeed", fields["changefeed"])
	// Client fields are passed through, including long values.
	require.Equal(t, "127.0.0.1:9092", fields["broker"])
	require.Equal(t, strings.Repeat("x", 2048), fields["payload"])
	// A key without a value is dropped instead of panicking.
	require.NotContains(t, fields, "odd")

	// franz-go info records are dropped before field conversion unless TiCDC logs
	// at debug level, where they are emitted as debug records.
	log.SetLevel(zapcore.InfoLevel)
	clientLogger.Log(kgo.LogLevelInfo, "info record", "broker", "127.0.0.1:9092")
	require.Empty(t, logs.FilterMessage("info record").AllUntimed())

	log.SetLevel(zapcore.DebugLevel)
	clientLogger.Log(kgo.LogLevelInfo, "info record", "broker", "127.0.0.1:9092")
	require.Len(t, logs.FilterMessage("info record").AllUntimed(), 1)

	// Repeated messages are sampled so a chatty client cannot flood the log.
	for range 105 {
		clientLogger.Log(kgo.LogLevelWarn, "repeated")
	}
	require.Len(t, logs.FilterMessage("repeated").AllUntimed(), 6)
}
