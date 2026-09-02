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
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// logValueLimit bounds individual string fields emitted by the franz-go logger.
const logValueLimit = 1024

type clientLogger struct {
	logger *zap.Logger
}

func newClientLogger(changefeedID common.ChangeFeedID, role string) kgo.Logger {
	logger := log.L().With(
		zap.String("component", "kafka-client"),
		zap.String("keyspace", changefeedID.Keyspace()),
		zap.String("changefeed", changefeedID.Name()),
		zap.String("role", role),
	).WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(core, time.Minute, 5, 100)
	}))

	return &clientLogger{logger: logger}
}

func (l *clientLogger) Level() kgo.LogLevel {
	if log.GetLevel() <= zapcore.DebugLevel {
		return kgo.LogLevelInfo
	}
	return kgo.LogLevelWarn
}

func (l *clientLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	fields := make([]zap.Field, 0, (len(keyvals)+1)/2)

	for i := 0; i < len(keyvals); i += 2 {
		key := fmt.Sprint(keyvals[i])
		value := any("<missing>")
		if i+1 < len(keyvals) {
			value = keyvals[i+1]
		}

		if isSensitiveLogKey(key) {
			value = "[redacted]"
		} else if text, ok := value.(string); ok && len(text) > logValueLimit {
			value = text[:logValueLimit]
		}
		fields = append(fields, zap.Any(key, value))
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

func isSensitiveLogKey(key string) bool {
	key = strings.ToLower(key)
	if key == "key" || key == "value" {
		return true
	}

	for _, fragment := range []string{
		"password",
		"passwd",
		"secret",
		"token",
		"authorization",
		"credential",
		"sasl",
	} {
		if strings.Contains(key, fragment) {
			return true
		}
	}

	return false
}
