// Copyright 2022 PingCAP, Inc.
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

package cloudstorage

import (
	"context"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin/binding"
	"github.com/imdario/mergo"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	// defaultWorkerCount is the default value of worker-count.
	defaultWorkerCount = 16
	// the upper limit of worker-count.
	maxWorkerCount = 512
	// defaultFlushInterval is the default value of flush-interval.
	defaultFlushInterval = 5 * time.Second
	// the lower limit of flush-interval.
	minFlushInterval = 100 * time.Millisecond
	// the upper limit of flush-interval.
	maxFlushInterval = 10 * time.Minute
	// defaultFlushConcurrency is the default value of flush-concurrency.
	defaultFlushConcurrency = 1
	// the lower limit of flush-concurrency.
	minFlushConcurrency = 1
	// the upper limit of flush-concurrency.
	maxFlushConcurrency = 512
	// defaultFileSize is the default value of file-size.
	defaultFileSize = 64 * 1024 * 1024
	// defaultSpoolDiskQuota is the default value of spool-disk-quota.
	defaultSpoolDiskQuota = int64(10 * 1024 * 1024 * 1024)
	// the lower limit of file size
	minFileSize = 1 * 1024
	// the upper limit of file size
	maxFileSize = 512 * 1024 * 1024

	// disable file cleanup by default
	defaultFileExpirationDays = 0
	// Second | Minute | Hour | Dom | Month | DowOptional
	// `0 0 2 * * ?` means 2:00:00 AM every day
	defaultFileCleanupCronSpec = "0 0 2 * * *"

	defaultEnableTableAcrossNodes = false
)

type urlConfig struct {
	WorkerCount      *int    `form:"worker-count"`
	FlushInterval    *string `form:"flush-interval"`
	FileSize         *int    `form:"file-size"`
	UseTableIDAsPath *bool   `form:"use-table-id-as-path"`
	SpoolDiskQuota   *int64  `form:"spool-disk-quota"`
	SpoolDir         *string `form:"spool-dir"`
}

// Config is the configuration for cloud storage sink.
type Config struct {
	WorkerCount              int
	FlushInterval            time.Duration
	FileSize                 int
	FileIndexWidth           int
	DateSeparator            string
	FileExpirationDays       int
	FileCleanupCronSpec      string
	EnablePartitionSeparator bool
	OutputColumnID           bool
	FlushConcurrency         int
	EnableTableAcrossNodes   bool
	UseTableIDAsPath         bool
	SpoolDiskQuota           int64
	SpoolDir                 string
}

// NewConfig returns the default cloud storage sink config.
func NewConfig() *Config {
	return &Config{
		WorkerCount:            defaultWorkerCount,
		FlushInterval:          defaultFlushInterval,
		FileSize:               defaultFileSize,
		FileExpirationDays:     defaultFileExpirationDays,
		FileCleanupCronSpec:    defaultFileCleanupCronSpec,
		EnableTableAcrossNodes: defaultEnableTableAcrossNodes,
		SpoolDiskQuota:         defaultSpoolDiskQuota,
	}
}

// Apply applies the sink URI parameters to the config.
func (c *Config) Apply(
	ctx context.Context,
	sinkURI *url.URL,
	sinkConfig *config.SinkConfig,
	enableTableAcrossNodes bool,
) (err error) {
	if sinkURI == nil {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"failed to open cloud storage sink, empty SinkURI")
	}

	scheme := strings.ToLower(sinkURI.Scheme)
	if !config.IsStorageScheme(scheme) {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"can't create cloud storage sink with unsupported scheme: %s", scheme)
	}
	req := &http.Request{URL: sinkURI}
	urlParameter := &urlConfig{}
	if err := binding.Query.Bind(req, urlParameter); err != nil {
		return errors.WrapError(errors.ErrStorageSinkInvalidConfig, err)
	}
	if urlParameter, err = mergeConfig(sinkConfig, urlParameter); err != nil {
		return err
	}
	if err = getWorkerCount(urlParameter, &c.WorkerCount); err != nil {
		return err
	}
	err = getFlushInterval(urlParameter, &c.FlushInterval)
	if err != nil {
		return err
	}
	err = getFileSize(urlParameter, &c.FileSize)
	if err != nil {
		return err
	}
	if err = getUseTableIDAsPath(urlParameter, &c.UseTableIDAsPath); err != nil {
		return err
	}
	if err = getSpoolDiskQuota(urlParameter, &c.SpoolDiskQuota); err != nil {
		return err
	}
	if err = getSpoolDir(urlParameter, &c.SpoolDir); err != nil {
		return err
	}

	c.DateSeparator = util.GetOrZero(sinkConfig.DateSeparator)
	c.EnablePartitionSeparator = util.GetOrZero(sinkConfig.EnablePartitionSeparator)
	c.FileIndexWidth = util.GetOrZero(sinkConfig.FileIndexWidth)
	if sinkConfig.CloudStorageConfig != nil {
		c.OutputColumnID = util.GetOrZero(sinkConfig.CloudStorageConfig.OutputColumnID)
		if sinkConfig.CloudStorageConfig.FileExpirationDays != nil {
			c.FileExpirationDays = *sinkConfig.CloudStorageConfig.FileExpirationDays
		}
		if sinkConfig.CloudStorageConfig.FileCleanupCronSpec != nil {
			c.FileCleanupCronSpec = *sinkConfig.CloudStorageConfig.FileCleanupCronSpec
		}
		c.FlushConcurrency = util.GetOrZero(sinkConfig.CloudStorageConfig.FlushConcurrency)
	}

	if c.FileIndexWidth < config.MinFileIndexWidth || c.FileIndexWidth > config.MaxFileIndexWidth {
		c.FileIndexWidth = config.DefaultFileIndexWidth
	}
	if c.FlushConcurrency < minFlushConcurrency || c.FlushConcurrency > maxFlushConcurrency {
		c.FlushConcurrency = defaultFlushConcurrency
	}

	c.EnableTableAcrossNodes = enableTableAcrossNodes
	return nil
}

func mergeConfig(
	sinkConfig *config.SinkConfig,
	urlParameters *urlConfig,
) (*urlConfig, error) {
	dest := &urlConfig{}
	if sinkConfig != nil && sinkConfig.CloudStorageConfig != nil {
		// Copy pointed values into urlConfig so URI overrides do not mutate the
		// caller's ReplicaConfig through shared pointers.
		if sinkConfig.CloudStorageConfig.WorkerCount != nil {
			dest.WorkerCount = util.AddressOf(*sinkConfig.CloudStorageConfig.WorkerCount)
		}
		if sinkConfig.CloudStorageConfig.FlushInterval != nil {
			dest.FlushInterval = util.AddressOf(*sinkConfig.CloudStorageConfig.FlushInterval)
		}
		if sinkConfig.CloudStorageConfig.FileSize != nil {
			dest.FileSize = util.AddressOf(*sinkConfig.CloudStorageConfig.FileSize)
		}
		if sinkConfig.CloudStorageConfig.UseTableIDAsPath != nil {
			dest.UseTableIDAsPath = util.AddressOf(*sinkConfig.CloudStorageConfig.UseTableIDAsPath)
		}
		if sinkConfig.CloudStorageConfig.SpoolDiskQuota != nil {
			dest.SpoolDiskQuota = util.AddressOf(*sinkConfig.CloudStorageConfig.SpoolDiskQuota)
		}
		if sinkConfig.CloudStorageConfig.SpoolDir != nil {
			dest.SpoolDir = util.AddressOf(*sinkConfig.CloudStorageConfig.SpoolDir)
		}
	}
	if err := mergo.Merge(dest, urlParameters, mergo.WithOverride); err != nil {
		return nil, errors.WrapError(errors.ErrStorageSinkInvalidConfig, err)
	}
	return dest, nil
}

func getWorkerCount(values *urlConfig, workerCount *int) error {
	if values.WorkerCount == nil {
		return nil
	}

	c := *values.WorkerCount
	if c <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"invalid worker-count %d, it must be greater than 0", c)
	}
	if c > maxWorkerCount {
		log.Warn("worker-count is too large",
			zap.Int("original", c), zap.Int("override", maxWorkerCount))
		c = maxWorkerCount
	}

	*workerCount = c
	return nil
}

func getFlushInterval(values *urlConfig, flushInterval *time.Duration) error {
	if values.FlushInterval == nil || len(*values.FlushInterval) == 0 {
		return nil
	}

	d, err := time.ParseDuration(*values.FlushInterval)
	if err != nil {
		return errors.WrapError(errors.ErrStorageSinkInvalidConfig, err)
	}

	if d > maxFlushInterval {
		log.Warn("flush-interval is too large", zap.Duration("original", d),
			zap.Duration("override", maxFlushInterval))
		d = maxFlushInterval
	}
	if d < minFlushInterval {
		log.Warn("flush-interval is too small", zap.Duration("original", d),
			zap.Duration("override", minFlushInterval))
		d = minFlushInterval
	}

	*flushInterval = d
	return nil
}

func getUseTableIDAsPath(values *urlConfig, useTableIDAsPath *bool) error {
	if values.UseTableIDAsPath == nil {
		return nil
	}

	*useTableIDAsPath = *values.UseTableIDAsPath
	return nil
}

func getFileSize(values *urlConfig, fileSize *int) error {
	if values.FileSize == nil {
		return nil
	}

	sz := *values.FileSize
	if sz > maxFileSize {
		log.Warn("file-size is too large",
			zap.Int("original", sz), zap.Int("override", maxFileSize))
		sz = maxFileSize
	}
	if sz < minFileSize {
		log.Warn("file-size is too small",
			zap.Int("original", sz), zap.Int("override", minFileSize))
		sz = minFileSize
	}
	*fileSize = sz
	return nil
}

func getSpoolDiskQuota(values *urlConfig, spoolDiskQuota *int64) error {
	if values.SpoolDiskQuota == nil {
		return nil
	}

	quota := *values.SpoolDiskQuota
	if quota <= 0 {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"invalid spool-disk-quota %d, it must be greater than 0", quota)
	}

	*spoolDiskQuota = quota
	return nil
}

func getSpoolDir(values *urlConfig, spoolDir *string) error {
	if values.SpoolDir == nil || len(*values.SpoolDir) == 0 {
		return nil
	}

	dir := *values.SpoolDir
	if !filepath.IsAbs(dir) {
		return errors.ErrStorageSinkInvalidConfig.GenWithStack(
			"invalid spool-dir %q, it must be an absolute path", dir)
	}

	*spoolDir = dir
	return nil
}
