//  Copyright 2021 PingCAP, Inc.
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
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// Config is the config for redo log writer.
type Config struct {
	captureID    config.CaptureID
	changefeedID common.ChangeFeedID

	uri               *url.URL
	maxLogSizeInBytes int64

	useFileBackend bool

	flushIntervalInMs int64

	// the number of encoding concurrency.
	encodingWorkerNum int

	// Shared by file and memory backends for worker fanout sizing.
	flushWorkerNum int
	// Used only by the memory backend for file compression.
	compression string
	// Used only by the memory backend for flush concurrency.
	flushConcurrency int

	// Used only by the file backend as the local writer directory.
	dir string
}

// NewConfig builds the runtime writer config from an adjusted ConsistentConfig.
func NewConfig(changefeedID common.ChangeFeedID, consistentCfg *config.ConsistentConfig) (*Config, error) {
	storageURI := util.GetOrZero(consistentCfg.Storage)
	uri, err := storage.ParseRawURL(storageURI)
	if err != nil {
		return nil, errors.WrapError(errors.ErrStorageInitialize, err)
	}
	if !redo.IsValidConsistentStorage(uri.Scheme) {
		return nil, errors.ErrConsistentStorage.GenWithStackByArgs(uri.Scheme)
	}
	redo.FixLocalScheme(uri)

	cfg := &Config{
		captureID:         config.GetGlobalServerConfig().AdvertiseAddr,
		changefeedID:      changefeedID,
		uri:               uri,
		maxLogSizeInBytes: util.GetOrZero(consistentCfg.MaxLogSize) * redo.Megabyte,
		useFileBackend:    util.GetOrZero(consistentCfg.UseFileBackend),
		flushIntervalInMs: util.GetOrZero(consistentCfg.FlushIntervalInMs),
		encodingWorkerNum: util.GetOrZero(consistentCfg.EncodingWorkerNum),
		flushWorkerNum:    util.GetOrZero(consistentCfg.FlushWorkerNum),
		compression:       util.GetOrZero(consistentCfg.Compression),
		flushConcurrency:  util.GetOrZero(consistentCfg.FlushConcurrency),
	}
	cfg.dir = newWriterDir(cfg)
	return cfg, nil
}

func newWriterDir(cfg *Config) string {
	if cfg == nil || cfg.uri == nil {
		return ""
	}
	if !cfg.UseExternalStorage() {
		return cfg.uri.Path
	}
	if cfg.uri.Scheme == "file" {
		return cfg.uri.Path
	}
	if !cfg.useFileBackend {
		return ""
	}
	return filepath.Join(
		config.GetGlobalServerConfig().DataDir,
		config.DefaultRedoDir,
		cfg.changefeedID.Keyspace(),
		cfg.changefeedID.Name(),
	)
}

func (cfg Config) String() string {
	uri := ""
	if cfg.uri != nil {
		uri = cfg.uri.String()
	}
	return fmt.Sprintf("%s:%s:%s:%s:%d:%s:%t",
		cfg.changefeedID.Keyspace(), cfg.changefeedID.Name(), cfg.captureID,
		cfg.dir, cfg.maxLogSizeInBytes, uri, cfg.UseExternalStorage())
}

func (cfg *Config) CaptureID() config.CaptureID {
	return cfg.captureID
}

func (cfg *Config) ChangeFeedID() common.ChangeFeedID {
	return cfg.changefeedID
}

func (cfg *Config) URI() *url.URL {
	return cfg.uri
}

func (cfg *Config) UseExternalStorage() bool {
	return cfg.uri != nil && redo.IsExternalStorage(cfg.uri.Scheme)
}

func (cfg *Config) Dir() string {
	return cfg.dir
}

func (cfg *Config) MaxLogSizeInBytes() int64 {
	return cfg.maxLogSizeInBytes
}

func (cfg *Config) UseFileBackend() bool {
	return cfg.useFileBackend
}

func (cfg *Config) FlushIntervalInMs() int64 {
	return cfg.flushIntervalInMs
}

func (cfg *Config) EncodingWorkerNum() int {
	return cfg.encodingWorkerNum
}

func (cfg *Config) FlushWorkerNum() int {
	return cfg.flushWorkerNum
}

func (cfg *Config) Compression() string {
	return cfg.compression
}

func (cfg *Config) FlushConcurrency() int {
	return cfg.flushConcurrency
}
