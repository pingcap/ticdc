// Copyright 2024 PingCAP, Inc.
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

package redact

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestIntegrationWithLogger tests redaction with actual zap logger
func TestIntegrationWithLogger(t *testing.T) {
	originalMode := GetRedactMode()
	defer SetRedactMode(originalMode)

	testCases := []struct {
		name     string
		mode     RedactMode
		logFunc  func(*zap.Logger)
		expected string
		notExpected string
	}{
		{
			name: "SQL redaction OFF",
			mode: Off,
			logFunc: func(logger *zap.Logger) {
				logger.Info("executing query", zap.String("sql", SQL("SELECT * FROM users WHERE id = 123")))
			},
			expected: "SELECT * FROM users WHERE id = 123",
		},
		{
			name: "SQL redaction ON",
			mode: On,
			logFunc: func(logger *zap.Logger) {
				logger.Info("executing query", zap.String("sql", SQL("SELECT * FROM users WHERE id = 123")))
			},
			expected:    "?",
			notExpected: "SELECT",
		},
		{
			name: "SQL redaction MARKER",
			mode: Marker,
			logFunc: func(logger *zap.Logger) {
				logger.Info("executing query", zap.String("sql", SQL("SELECT * FROM users WHERE id = 123")))
			},
			expected: "‹SELECT * FROM users WHERE id = 123›",
		},
		{
			name: "Value redaction OFF",
			mode: Off,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("column value", zap.String("value", Value("sensitive_data")))
			},
			expected: "sensitive_data",
		},
		{
			name: "Value redaction ON",
			mode: On,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("column value", zap.String("value", Value("sensitive_data")))
			},
			expected:    "?",
			notExpected: "sensitive_data",
		},
		{
			name: "Value redaction MARKER",
			mode: Marker,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("column value", zap.String("value", Value("sensitive_data")))
			},
			expected: "‹sensitive_data›",
		},
		{
			name: "Key redaction OFF",
			mode: Off,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("processing key", zap.String("key", Key([]byte{0x12, 0x34, 0xAB})))
			},
			expected: "1234AB",
		},
		{
			name: "Key redaction ON",
			mode: On,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("processing key", zap.String("key", Key([]byte{0x12, 0x34, 0xAB})))
			},
			expected:    "?",
			notExpected: "1234AB",
		},
		{
			name: "Key redaction MARKER",
			mode: Marker,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("processing key", zap.String("key", Key([]byte{0x12, 0x34, 0xAB})))
			},
			expected: "‹1234AB›",
		},
		{
			name: "Args redaction OFF",
			mode: Off,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("query args", zap.String("args", Args([]interface{}{1, "john", "password123"})))
			},
			expected: "(1, john, password123)",
		},
		{
			name: "Args redaction ON",
			mode: On,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("query args", zap.String("args", Args([]interface{}{1, "john", "password123"})))
			},
			expected:    "(?, ?, ?)",
			notExpected: "password123",
		},
		{
			name: "Args redaction MARKER",
			mode: Marker,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("query args", zap.String("args", Args([]interface{}{1, "john", "password123"})))
			},
			expected: "(‹1›, ‹john›, ‹password123›)",
		},
		{
			name: "Any redaction OFF",
			mode: Off,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("value", zap.String("data", Any(map[string]string{"key": "value"})))
			},
			expected: "map[key:value]",
		},
		{
			name: "Any redaction ON",
			mode: On,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("value", zap.String("data", Any(map[string]string{"key": "value"})))
			},
			expected:    "?",
			notExpected: "map[key:value]",
		},
		{
			name: "Any redaction MARKER",
			mode: Marker,
			logFunc: func(logger *zap.Logger) {
				logger.Debug("value", zap.String("data", Any(map[string]string{"key": "value"})))
			},
			expected: "‹map[key:value]›",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create an observer core to capture log output
			core, logs := observer.New(zapcore.DebugLevel)
			logger := zap.New(core)

			// Set redaction mode
			SetRedactMode(tc.mode)

			// Execute the log function
			tc.logFunc(logger)

			// Get all logged entries
			entries := logs.All()
			require.Len(t, entries, 1, "Expected exactly one log entry")

			// Check the log message contains expected content
			logMessage := entries[0].Message
			for field := range entries[0].Context {
				logMessage += " " + entries[0].Context[field].String
			}

			require.Contains(t, logMessage, tc.expected,
				"Log should contain expected redacted content")

			if tc.notExpected != "" {
				require.NotContains(t, logMessage, tc.notExpected,
					"Log should NOT contain sensitive content")
			}
		})
	}
}

// TestIntegrationMarkerEscaping tests that markers are properly escaped in actual logs
func TestIntegrationMarkerEscaping(t *testing.T) {
	originalMode := GetRedactMode()
	defer SetRedactMode(originalMode)

	core, logs := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	SetRedactMode(Marker)

	// Log a value that contains markers
	logger.Info("test", zap.String("value", String("text with ‹ and › markers")))

	entries := logs.All()
	require.Len(t, entries, 1)

	logMessage := entries[0].Context[0].String
	require.Contains(t, logMessage, "‹text with ‹‹ and ›› markers›",
		"Markers should be properly escaped")
}

// TestIntegrationConcurrent tests concurrent logging with redaction
func TestIntegrationConcurrent(t *testing.T) {
	originalMode := GetRedactMode()
	defer SetRedactMode(originalMode)

	core, _ := observer.New(zapcore.InfoLevel)
	logger := zap.New(core)

	// Test that concurrent logging with mode changes doesn't crash
	done := make(chan bool)

	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				mode := []RedactMode{Off, On, Marker}[j%3]
				SetRedactMode(mode)
				logger.Info("concurrent log",
					zap.Int("goroutine", id),
					zap.String("query", SQL("SELECT * FROM test")))
			}
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

func TestIntegrationNilValues(t *testing.T) {
	originalMode := GetRedactMode()
	defer SetRedactMode(originalMode)

	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	SetRedactMode(On)

	// Test nil handling in various functions
	logger.Debug("nil tests",
		zap.String("key", Key(nil)),
		zap.String("bytes", Bytes(nil)),
		zap.String("any", Any(nil)))

	entries := logs.All()
	require.Len(t, entries, 1)

	// All nil values should show as NULL (not redacted)
	for i := 0; i < 3; i++ {
		require.Equal(t, "NULL", entries[0].Context[i].String,
			"Nil values should appear as NULL")
	}
}
