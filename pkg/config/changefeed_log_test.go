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

package config

import (
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestRmUnusedFieldsMasksUnparsableSinkURIInLog(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	restore := log.ReplaceGlobals(zap.New(core), nil)
	defer restore()

	// An unparsable sink URI, the *url.Error returned by url.Parse embeds it.
	info := &ChangeFeedInfo{SinkURI: "mysql://root:verysecure@127.0.0.1:port/", Config: GetDefaultReplicaConfig()}
	info.RmUnusedFields()

	entries := logs.FilterMessage("failed to parse the sink uri").All()
	require.Len(t, entries, 1)
	for _, field := range entries[0].Context {
		require.NotContains(t, field.String, "verysecure")
		if err, ok := field.Interface.(error); ok {
			require.NotContains(t, err.Error(), "verysecure")
		}
	}
}
