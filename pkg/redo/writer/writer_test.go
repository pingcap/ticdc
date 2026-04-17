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

package writer

import (
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/testutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewConfigUsesConsistentConfigValues(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	maxLogSize := int64(128)
	flushIntervalInMs := int64(1234)
	encodingWorkerNum := 5
	flushWorkerNum := 6
	compressionType := "lz4"
	flushConcurrency := 7
	consistentCfg := testutil.NewConsistentConfig("nfs:///tmp/redo")
	consistentCfg.MaxLogSize = util.AddressOf(maxLogSize)
	consistentCfg.FlushIntervalInMs = util.AddressOf(flushIntervalInMs)
	consistentCfg.EncodingWorkerNum = util.AddressOf(encodingWorkerNum)
	consistentCfg.FlushWorkerNum = util.AddressOf(flushWorkerNum)
	consistentCfg.Compression = util.AddressOf(compressionType)
	consistentCfg.FlushConcurrency = util.AddressOf(flushConcurrency)
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	require.Equal(t, changefeedID, cfg.ChangeFeedID())
	require.Equal(t, config.GetGlobalServerConfig().AdvertiseAddr, cfg.CaptureID())
	require.NotNil(t, cfg.URI())
	require.Equal(t, "file", cfg.URI().Scheme)
	require.Equal(t, "/tmp/redo", cfg.Dir())
	require.True(t, cfg.UseExternalStorage())
	require.Equal(t, maxLogSize*redo.Megabyte, cfg.MaxLogSizeInBytes())
	require.Equal(t, flushIntervalInMs, cfg.FlushIntervalInMs())
	require.Equal(t, encodingWorkerNum, cfg.EncodingWorkerNum())
	require.Equal(t, flushWorkerNum, cfg.FlushWorkerNum())
	require.Equal(t, flushConcurrency, cfg.FlushConcurrency())
	require.Equal(t, compressionType, cfg.Compression())
	require.False(t, cfg.UseFileBackend())
}

func TestNewConfigInitializesFileBackendDirForExternalStorage(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	consistentCfg := testutil.NewConsistentConfig("s3://bucket/prefix")
	consistentCfg.UseFileBackend = util.AddressOf(true)
	cfg, err := NewConfig(changefeedID, consistentCfg)
	require.NoError(t, err)

	require.NotNil(t, cfg.URI())
	require.Equal(t, "s3", cfg.URI().Scheme)
	require.True(t, cfg.UseExternalStorage())
	require.True(t, cfg.UseFileBackend())
	require.Equal(t,
		filepath.Join(config.GetGlobalServerConfig().DataDir, config.DefaultRedoDir, changefeedID.Keyspace(), changefeedID.Name()),
		cfg.Dir())
}

func TestNewConfigLeavesDirEmptyForRemoteMemoryBackend(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	cfg, err := NewConfig(changefeedID, testutil.NewConsistentConfig("s3://bucket/prefix"))
	require.NoError(t, err)

	require.NotNil(t, cfg.URI())
	require.Equal(t, "s3", cfg.URI().Scheme)
	require.True(t, cfg.UseExternalStorage())
	require.False(t, cfg.UseFileBackend())
	require.Empty(t, cfg.Dir())
}

func TestNewConfigReturnsErrorForInvalidStorageURI(t *testing.T) {
	t.Parallel()

	_, err := NewConfig(
		common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName),
		testutil.NewConsistentConfig("://bad-uri"),
	)
	require.Error(t, err)
}

func TestNewConfigReturnsErrorForUnsupportedStorageScheme(t *testing.T) {
	t.Parallel()

	_, err := NewConfig(
		common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName),
		testutil.NewConsistentConfig("mysql://127.0.0.1:3306/test"),
	)
	require.Error(t, err)
}
