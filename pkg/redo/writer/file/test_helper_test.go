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

package file

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/compression"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/stretchr/testify/require"
)

func newTestWriterConfig(
	t *testing.T,
	changefeedID common.ChangeFeedID,
	consistentCfg *config.ConsistentConfig,
) *writer.Config {
	if consistentCfg == nil {
		consistentCfg = &config.ConsistentConfig{}
	}
	if len(util.GetOrZero(consistentCfg.Storage)) == 0 {
		consistentCfg.Storage = util.AddressOf("file://" + t.TempDir())
	}
	if util.GetOrZero(consistentCfg.MaxLogSize) == 0 {
		consistentCfg.MaxLogSize = util.AddressOf(redo.DefaultMaxLogSize)
	}
	if util.GetOrZero(consistentCfg.FlushIntervalInMs) == 0 {
		consistentCfg.FlushIntervalInMs = util.AddressOf(int64(redo.DefaultFlushIntervalInMs))
	}
	if util.GetOrZero(consistentCfg.EncodingWorkerNum) == 0 {
		consistentCfg.EncodingWorkerNum = util.AddressOf(redo.DefaultEncodingWorkerNum)
	}
	if util.GetOrZero(consistentCfg.FlushWorkerNum) == 0 {
		consistentCfg.FlushWorkerNum = util.AddressOf(redo.DefaultFlushWorkerNum)
	}
	if len(util.GetOrZero(consistentCfg.Compression)) == 0 {
		consistentCfg.Compression = util.AddressOf(compression.None)
	}
	if util.GetOrZero(consistentCfg.FlushConcurrency) == 0 {
		consistentCfg.FlushConcurrency = util.AddressOf(1)
	}
	cfg, err := writer.NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)
	return cfg
}

func newTestLocalExternalStorage(t *testing.T, dir string) storage.ExternalStorage {
	extStorage, _, err := util.GetTestExtStorage(context.Background(), dir)
	require.NoError(t, err)
	return extStorage
}
