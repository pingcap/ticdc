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

package logger

import (
	stdlog "log"
	"testing"

	"github.com/IBM/sarama"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
)

func TestIsDebugEnabled(t *testing.T) {
	oldLevel := log.GetLevel()
	defer log.SetLevel(oldLevel)

	log.SetLevel(zapcore.InfoLevel)
	require.False(t, IsDebugEnabled())

	log.SetLevel(zapcore.DebugLevel)
	require.True(t, IsDebugEnabled())
}

func TestInitSaramaLoggerResetsWhenInfoEnabled(t *testing.T) {
	originalLogger := sarama.Logger
	defer func() {
		sarama.Logger = originalLogger
	}()

	require.NoError(t, initSaramaLogger(zapcore.DebugLevel))
	debugLogger := sarama.Logger

	require.NoError(t, initSaramaLogger(zapcore.InfoLevel))
	require.NotSame(t, debugLogger, sarama.Logger)
	require.IsType(t, stdlog.New(nil, "", 0), sarama.Logger)
}
