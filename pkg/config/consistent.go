// Copyright 2021 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/ticdc/pkg/compression"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// ConsistentConfig represents replication consistency config for a changefeed.
// It is used by redo log functionality.
type ConsistentConfig struct {
	// Level is the consistency level, it can be `none` or `eventual`.
	// `eventual` means enable redo log.
	// Default is `none`.
	Level *string `toml:"level" json:"level,omitempty"`
	// MaxLogSize is the max size(MiB) of a log file written by redo log.
	// Default is 64MiB.
	MaxLogSize *int64 `toml:"max-log-size" json:"max-log-size,omitempty"`
	// FlushIntervalInMs is the flush interval(ms) of redo log to flush log to storage.
	// Default is 2000ms.
	FlushIntervalInMs *int64 `toml:"flush-interval" json:"flush-interval,omitempty"`
	// MetaFlushIntervalInMs is the flush interval(ms) of redo log to
	// flush meta(resolvedTs and checkpointTs) to storage.
	// Default is 200ms.
	MetaFlushIntervalInMs *int64 `toml:"meta-flush-interval" json:"meta-flush-interval,omitempty"`
	// EncodingWorkerNum is the number of workers to encode `RowChangeEvent`` to redo log.
	// Default is 16.
	EncodingWorkerNum *int `toml:"encoding-worker-num" json:"encoding-worker-num,omitempty"`
	// FlushWorkerNum is the number of workers to flush redo log to storage.
	// Default is 8.
	FlushWorkerNum *int `toml:"flush-worker-num" json:"flush-worker-num,omitempty"`
	// Storage is the storage path(uri) to store redo log.
	Storage *string `toml:"storage" json:"storage,omitempty"`
	// UseFileBackend is a flag to enable file backend for redo log.
	// file backend means before flush redo log to storage, it will be written to local file.
	// Default is false.
	UseFileBackend *bool `toml:"use-file-backend" json:"use-file-backend,omitempty"`
	// Compression is the compression algorithm used for redo log.
	// Default is "", it means no compression, equals to `none`.
	// Supported compression algorithms are `none` and `lz4`.
	Compression *string `toml:"compression" json:"compression,omitempty"`
	// FlushConcurrency is the concurrency of flushing a single log file.
	// Default is 1. It means a single log file will be flushed by only one worker.
	// The singe file concurrent flushing feature supports only `s3` storage.
	FlushConcurrency *int `toml:"flush-concurrency" json:"flush-concurrency,omitempty"`
	// MemoryUsage represents the percentage of ReplicaConfig.MemoryQuota
	// that can be utilized by the redo log module.
	MemoryUsage *ConsistentMemoryUsage `toml:"memory-usage" json:"memory-usage,omitempty"`
}

// ConsistentMemoryUsage represents memory usage of Consistent module.
type ConsistentMemoryUsage struct {
	// ReplicaConfig.MemoryQuota * MemoryQuotaPercentage / 100 will be used for redo events.
	MemoryQuotaPercentage uint64 `toml:"memory-quota-percentage" json:"memory-quota-percentage"`
}

// ValidateAndAdjust validates the consistency config and adjusts it if necessary.
func (c *ConsistentConfig) ValidateAndAdjust() error {
	if !redo.IsConsistentEnabled(util.GetOrZero(c.Level)) {
		return nil
	}

	if util.GetOrZero(c.MaxLogSize) == 0 {
		c.MaxLogSize = util.AddressOf(redo.DefaultMaxLogSize)
	}

	if util.GetOrZero(c.FlushIntervalInMs) == 0 {
		c.FlushIntervalInMs = util.AddressOf(int64(redo.DefaultFlushIntervalInMs))
	}
	if util.GetOrZero(c.FlushIntervalInMs) < redo.MinFlushIntervalInMs {
		return cerror.ErrInvalidReplicaConfig.FastGenByArgs(
			fmt.Sprintf("The consistent.flush-interval:%d must be equal or greater than %d",
				util.GetOrZero(c.FlushIntervalInMs), redo.MinFlushIntervalInMs))
	}

	if util.GetOrZero(c.MetaFlushIntervalInMs) == 0 {
		c.MetaFlushIntervalInMs = util.AddressOf(int64(redo.DefaultMetaFlushIntervalInMs))
	}
	if util.GetOrZero(c.MetaFlushIntervalInMs) < redo.MinFlushIntervalInMs {
		return cerror.ErrInvalidReplicaConfig.FastGenByArgs(
			fmt.Sprintf("The consistent.meta-flush-interval:%d must be equal or greater than %d",
				util.GetOrZero(c.MetaFlushIntervalInMs), redo.MinFlushIntervalInMs))
	}
	if len(util.GetOrZero(c.Compression)) > 0 &&
		util.GetOrZero(c.Compression) != compression.None && util.GetOrZero(c.Compression) != compression.LZ4 {
		return cerror.ErrInvalidReplicaConfig.FastGenByArgs(
			fmt.Sprintf("The consistent.compression:%s must be 'none' or 'lz4'", util.GetOrZero(c.Compression)))
	}

	if util.GetOrZero(c.EncodingWorkerNum) == 0 {
		c.EncodingWorkerNum = util.AddressOf(redo.DefaultEncodingWorkerNum)
	}
	if util.GetOrZero(c.FlushWorkerNum) == 0 {
		c.FlushWorkerNum = util.AddressOf(redo.DefaultFlushWorkerNum)
	}

	uri, err := storage.ParseRawURL(util.GetOrZero(c.Storage))
	if err != nil {
		return cerror.ErrInvalidReplicaConfig.GenWithStackByArgs(
			fmt.Sprintf("invalid storage uri: %s", util.GetOrZero(c.Storage)))
	}
	return redo.ValidateStorage(uri)
}

// MaskSensitiveData masks sensitive data in ConsistentConfig
func (c *ConsistentConfig) MaskSensitiveData() {
	c.Storage = util.AddressOf(util.MaskSensitiveDataInURI(util.GetOrZero(c.Storage)))
}
