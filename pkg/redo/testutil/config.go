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

package testutil

import (
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
)

func NewConsistentConfig(storage string) *config.ConsistentConfig {
	level := string(redo.ConsistentLevelEventual)
	maxLogSize := int64(redo.DefaultMaxLogSize)
	flushIntervalInMs := int64(redo.DefaultFlushIntervalInMs)
	metaFlushIntervalInMs := int64(redo.MinFlushIntervalInMs)
	encodingWorkerNum := redo.DefaultEncodingWorkerNum
	flushWorkerNum := redo.DefaultFlushWorkerNum
	compressionType := compression.None
	flushConcurrency := 1

	return &config.ConsistentConfig{
		Level:                 util.AddressOf(level),
		MaxLogSize:            util.AddressOf(maxLogSize),
		Storage:               util.AddressOf(storage),
		FlushIntervalInMs:     util.AddressOf(flushIntervalInMs),
		MetaFlushIntervalInMs: util.AddressOf(metaFlushIntervalInMs),
		EncodingWorkerNum:     util.AddressOf(encodingWorkerNum),
		FlushWorkerNum:        util.AddressOf(flushWorkerNum),
		Compression:           util.AddressOf(compressionType),
		FlushConcurrency:      util.AddressOf(flushConcurrency),
	}
}
