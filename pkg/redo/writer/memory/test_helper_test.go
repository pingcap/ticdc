//  Copyright 2026 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package memory

import (
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
)

func newTestConsistentConfig(storage string) *config.ConsistentConfig {
	maxLogSize := int64(redo.DefaultMaxLogSize)
	flushIntervalInMs := int64(redo.DefaultFlushIntervalInMs)
	encodingWorkerNum := redo.DefaultEncodingWorkerNum
	flushWorkerNum := redo.DefaultFlushWorkerNum
	compressionType := compression.None
	flushConcurrency := 1
	return &config.ConsistentConfig{
		MaxLogSize:        util.AddressOf(maxLogSize),
		FlushIntervalInMs: util.AddressOf(flushIntervalInMs),
		EncodingWorkerNum: util.AddressOf(encodingWorkerNum),
		FlushWorkerNum:    util.AddressOf(flushWorkerNum),
		Storage:           util.AddressOf(storage),
		Compression:       util.AddressOf(compressionType),
		FlushConcurrency:  util.AddressOf(flushConcurrency),
	}
}
