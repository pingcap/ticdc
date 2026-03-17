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
	"context"
	"fmt"
	"net/url"
	"path/filepath"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/uuid"
	"github.com/pingcap/tidb/br/pkg/storage"
)

var (
	_ RedoEvent = (*commonEvent.RedoRowEvent)(nil)
	_ RedoEvent = (*commonEvent.DDLEvent)(nil)
)

// RedoEvent is the interface for redo event.
type RedoEvent interface {
	PostFlush()
	ToRedoLog() *commonEvent.RedoLog
}

// RedoLogWriter defines the interfaces used to write redo log, all operations are thread-safe.
type RedoLogWriter interface {
	// WriteEvents writes DDL/DML events to the redo log.
	WriteEvents(ctx context.Context, events ...RedoEvent) error

	Run(ctx context.Context) error
	// Close is used to close the writer.
	Close() error

	SetTableSchemaStore(*commonEvent.TableSchemaStore)
}

// Config is the config for redo log writer.
type Config struct {
	config.ConsistentConfig
	CaptureID    config.CaptureID
	ChangeFeedID common.ChangeFeedID

	URI                *url.URL
	UseExternalStorage bool
	Dir                string
	MaxLogSizeInBytes  int64
}

// ConfigOption applies optional overrides to redo writer config.
type ConfigOption func(*Config)

func NewConfig(changefeedID common.ChangeFeedID, consistentCfg *config.ConsistentConfig, opts ...ConfigOption) (*Config, error) {
	if consistentCfg == nil {
		return nil, errors.ErrRedoConfigInvalid.GenWithStack("consisten config is nil")
	}

	cfg := &Config{
		ChangeFeedID:      changefeedID,
		CaptureID:         config.GetGlobalServerConfig().AdvertiseAddr,
		ConsistentConfig:  *consistentCfg,
		MaxLogSizeInBytes: util.GetOrZero(consistentCfg.MaxLogSize) * redo.Megabyte,
	}

	if err := initStorageConfig(cfg); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if opt != nil {
			opt(cfg)
		}
	}
	return cfg, nil
}

func initStorageConfig(cfg *Config) error {
	storageURI := util.GetOrZero(cfg.Storage)
	if len(storageURI) == 0 {
		return errors.ErrRedoConfigInvalid.GenWithStack("storage uri is not set")
	}

	uri, err := storage.ParseRawURL(storageURI)
	if err != nil {
		return cerror.WrapError(cerror.ErrStorageInitialize, err)
	}
	if !redo.IsValidConsistentStorage(uri.Scheme) {
		return cerror.ErrConsistentStorage.GenWithStackByArgs(uri.Scheme)
	}
	redo.FixLocalScheme(uri)

	cfg.URI = uri
	cfg.UseExternalStorage = redo.IsExternalStorage(uri.Scheme)
	if cfg.UseExternalStorage {
		if uri.Scheme == "file" {
			cfg.Dir = uri.Path
			return nil
		}
		if util.GetOrZero(cfg.UseFileBackend) {
			cfg.Dir = filepath.Join(
				config.GetGlobalServerConfig().DataDir,
				config.DefaultRedoDir,
				cfg.ChangeFeedID.Keyspace(),
				cfg.ChangeFeedID.Name(),
			)
		}
		return nil
	}
	cfg.Dir = uri.Path
	return nil
}

func (cfg Config) String() string {
	return fmt.Sprintf("%s:%s:%s:%s:%d:%s:%t",
		cfg.ChangeFeedID.Keyspace(), cfg.ChangeFeedID.Name(), cfg.CaptureID,
		cfg.Dir, cfg.MaxLogSize, cfg.URI.String(), cfg.UseExternalStorage)
}

// Option define the writerOptions
type Option func(writer *LogWriterOptions)

// LogWriterOptions is the options for writer
type LogWriterOptions struct {
	GetLogFileName   func() string
	GetUUIDGenerator func() uuid.Generator
}

// WithLogFileName provide the Option for fileName
func WithLogFileName(f func() string) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetLogFileName = f
		}
	}
}

func WithCaptureID(captureID config.CaptureID) ConfigOption {
	return func(cfg *Config) {
		cfg.CaptureID = captureID
	}
}

func WithDir(dir string) ConfigOption {
	return func(cfg *Config) {
		cfg.Dir = dir
	}
}

func WithMaxLogSizeInBytes(maxLogSizeInBytes int64) ConfigOption {
	return func(cfg *Config) {
		cfg.MaxLogSizeInBytes = maxLogSizeInBytes
	}
}

// WithUUIDGenerator provides the Option for uuid generator
func WithUUIDGenerator(f func() uuid.Generator) Option {
	return func(o *LogWriterOptions) {
		if f != nil {
			o.GetUUIDGenerator = f
		}
	}
}

// EncodeFrameSize encodes the frame size for etcd wal which uses code
// from etcd wal/encoder.go. Ref: https://github.com/etcd-io/etcd/pull/5250
func EncodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)
	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return lenField, padBytes
}
