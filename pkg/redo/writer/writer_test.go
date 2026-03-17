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
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNewConfigInitializesDefaultsFromConsistentConfig(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	maxLogSize := int64(64)
	storageURI := "nfs:///tmp/redo"
	cfg, err := NewConfig(changefeedID, &config.ConsistentConfig{
		Storage:    util.AddressOf(storageURI),
		MaxLogSize: util.AddressOf(maxLogSize),
	})
	require.NoError(t, err)

	require.Equal(t, changefeedID, cfg.ChangeFeedID)
	require.Equal(t, config.GetGlobalServerConfig().AdvertiseAddr, cfg.CaptureID)
	require.NotNil(t, cfg.URI)
	require.Equal(t, "file", cfg.URI.Scheme)
	require.Equal(t, "/tmp/redo", cfg.Dir)
	require.True(t, cfg.UseExternalStorage)
	require.Equal(t, maxLogSize*redo.Megabyte, cfg.MaxLogSizeInBytes)
}

func TestNewConfigInitializesFileBackendDirForExternalStorage(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	storageURI := "s3://bucket/prefix"
	cfg, err := NewConfig(changefeedID, &config.ConsistentConfig{
		Storage:        util.AddressOf(storageURI),
		UseFileBackend: util.AddressOf(true),
		MaxLogSize:     util.AddressOf(int64(1)),
	})
	require.NoError(t, err)

	require.NotNil(t, cfg.URI)
	require.Equal(t, "s3", cfg.URI.Scheme)
	require.True(t, cfg.UseExternalStorage)
	require.Equal(t,
		filepath.Join(config.GetGlobalServerConfig().DataDir, config.DefaultRedoDir, changefeedID.Keyspace(), changefeedID.Name()),
		cfg.Dir)
}

func TestNewConfigAppliesOptions(t *testing.T) {
	t.Parallel()

	changefeedID := common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName)
	cfg, err := NewConfig(
		changefeedID,
		&config.ConsistentConfig{
			Storage: util.AddressOf("s3://bucket/path"),
		},
		WithCaptureID("capture-1"),
		WithDir("/tmp/custom-redo"),
		WithMaxLogSizeInBytes(123),
	)
	require.NoError(t, err)

	require.Equal(t, "capture-1", cfg.CaptureID)
	require.NotNil(t, cfg.URI)
	require.Equal(t, "s3", cfg.URI.Scheme)
	require.True(t, cfg.UseExternalStorage)
	require.Equal(t, "/tmp/custom-redo", cfg.Dir)
	require.EqualValues(t, 123, cfg.MaxLogSizeInBytes)
}

func TestNewConfigReturnsErrorForInvalidStorageURI(t *testing.T) {
	t.Parallel()

	_, err := NewConfig(
		common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName),
		&config.ConsistentConfig{Storage: util.AddressOf("://bad-uri")},
	)
	require.Error(t, err)
}

func TestNewConfigReturnsErrorForUnsupportedStorageScheme(t *testing.T) {
	t.Parallel()

	_, err := NewConfig(
		common.NewChangeFeedIDWithName("test-cf", common.DefaultKeyspaceName),
		&config.ConsistentConfig{Storage: util.AddressOf("mysql://127.0.0.1:3306/test")},
	)
	require.Error(t, err)
}
