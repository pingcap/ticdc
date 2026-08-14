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

package franz

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// logValueLimit bounds individual string fields emitted by the franz-go logger.
const logValueLimit = 1024

type logger struct {
	changefeedID common.ChangeFeedID
	role         string
	now          func() time.Time
	mu           sync.Mutex
	windowStart  time.Time
	counts       map[string]uint64
}

func newLogger(changefeedID common.ChangeFeedID, role string) kgo.Logger {
	return &logger{changefeedID: changefeedID, role: role, now: time.Now, counts: make(map[string]uint64)}
}

func (l *logger) Level() kgo.LogLevel {
	if log.GetLevel() <= zapcore.DebugLevel {
		return kgo.LogLevelInfo
	}
	return kgo.LogLevelWarn
}

func (l *logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if !l.shouldLog(level, msg) {
		return
	}
	fields := []zap.Field{
		zap.String("component", "kafka-client"),
		zap.String("keyspace", l.changefeedID.Keyspace()),
		zap.String("changefeed", l.changefeedID.Name()),
		zap.String("role", l.role),
	}

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
		log.Error(msg, fields...)
	case kgo.LogLevelWarn:
		log.Warn(msg, fields...)
	default:
		log.Debug(msg, fields...)
	}
}

func (l *logger) shouldLog(level kgo.LogLevel, msg string) bool {
	now := l.now()
	key := fmt.Sprintf("%d:%s", level, msg)

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= time.Minute {
		l.windowStart, l.counts = now, make(map[string]uint64)
	}

	l.counts[key]++

	return l.counts[key] <= 5 || l.counts[key]%100 == 0
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
