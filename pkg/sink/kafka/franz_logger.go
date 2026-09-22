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
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// clientLogger forwards franz-go log fields unchanged: the library logs request
// metadata only, never record payloads, so no field needs filtering here.
type clientLogger struct{ logger *zap.Logger }

func newClientLogger(changefeedID common.ChangeFeedID) kgo.Logger {
	logger := log.L().With(
		zap.String("component", "kafka-client"),
		zap.String("keyspace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
	).WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(core, time.Minute, 5, 100)
	}))
	return &clientLogger{logger: logger}
}

func (l *clientLogger) Level() kgo.LogLevel {
	if debugLoggingEnabled() {
		return kgo.LogLevelInfo
	}
	return kgo.LogLevelWarn
}

// debugLoggingEnabled reports whether TiCDC runs at debug level, where franz-go
// info and debug records are kept.
func debugLoggingEnabled() bool {
	return log.GetLevel() <= zapcore.DebugLevel
}

func (l *clientLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	// franz-go emits info and debug records without consulting Level, so drop them
	// here instead of building fields that zap would discard anyway.
	if level > kgo.LogLevelWarn && !debugLoggingEnabled() {
		return
	}

	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i+1 < len(keyvals); i += 2 {
		fields = append(fields, zap.Any(fmt.Sprint(keyvals[i]), keyvals[i+1]))
	}

	switch level {
	case kgo.LogLevelError:
		l.logger.Error(msg, fields...)
	case kgo.LogLevelWarn:
		l.logger.Warn(msg, fields...)
	default:
		l.logger.Debug(msg, fields...)
	}
}
