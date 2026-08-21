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

package franz

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLoggerLevelAndFiltering(t *testing.T) {
	oldLevel := log.GetLevel()
	defer log.SetLevel(oldLevel)

	clientLogger := newLogger(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "logger"), "producer").(*logger)

	log.SetLevel(zapcore.InfoLevel)
	require.Equal(t, kgo.LogLevelWarn, clientLogger.Level())

	log.SetLevel(zapcore.DebugLevel)
	require.Equal(t, kgo.LogLevelInfo, clientLogger.Level())

	for _, key := range []string{"password", "access_token", "key", "value", "sasl-user"} {
		require.True(t, isSensitiveLogKey(key))
	}

	require.NotPanics(t, func() {
		clientLogger.Log(kgo.LogLevelWarn, "odd key value", "key-only")
	})
}

func TestLoggerSamplingIsConcurrent(t *testing.T) {
	clientLogger := newLogger(common.NewChangefeedID4Test(common.DefaultKeyspaceName, "logger"), "producer").(*logger)
	now := time.Now()
	clientLogger.now = func() time.Time { return now }

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			clientLogger.shouldLog(kgo.LogLevelWarn, "repeat")
		})
	}

	wg.Wait()
	require.Equal(t, uint64(20), clientLogger.counts["2:repeat"])
}

func TestLoggerPreservesContextAndRedactsValues(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()

	clientLogger := newLogger(common.NewChangefeedID4Test("keyspace", "changefeed"), "producer")
	clientLogger.Log(
		kgo.LogLevelWarn,
		"connection failed",
		"password", "secret",
		"payload", strings.Repeat("x", logValueLimit+10),
		"odd",
	)

	entries := logs.FilterMessage("connection failed").AllUntimed()
	require.Len(t, entries, 1)

	fields := entries[0].ContextMap()
	require.Equal(t, "kafka-client", fields["component"])
	require.Equal(t, "keyspace", fields["keyspace"])
	require.Equal(t, "changefeed", fields["changefeed"])
	require.Equal(t, "producer", fields["role"])
	require.Equal(t, "[redacted]", fields["password"])
	require.Equal(t, strings.Repeat("x", logValueLimit), fields["payload"])
	require.Equal(t, "<missing>", fields["odd"])
}

func TestLoggerSamplesRepeatedMessages(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()

	clientLogger := newLogger(common.NewChangefeedID4Test("keyspace", "changefeed"), "producer")
	for range 105 {
		clientLogger.Log(kgo.LogLevelWarn, "repeated")
	}

	require.Len(t, logs.FilterMessage("repeated").AllUntimed(), 6)
}
