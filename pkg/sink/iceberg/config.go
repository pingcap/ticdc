// Copyright 2025 PingCAP, Inc.
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

// Package iceberg implements an Apache Iceberg sink for TiCDC.
package iceberg

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
)

const (
	defaultNamespace      = "default"
	defaultCommitInterval = 30 * time.Second
	minCommitInterval     = 1 * time.Second
	maxCommitInterval     = 10 * time.Minute

	defaultTargetFileSizeBytes = 64 * 1024 * 1024
	minTargetFileSizeBytes     = 1 * 1024 * 1024
	maxTargetFileSizeBytes     = 512 * 1024 * 1024

	defaultPartitioning = "days(_tidb_commit_time)"
)

// Mode defines the write semantics for an Iceberg table.
type Mode string

const (
	// ModeAppend writes append-only changelog rows.
	ModeAppend Mode = "append"
	// ModeUpsert writes merge-on-read rows using equality deletes.
	ModeUpsert Mode = "upsert"
)

// CatalogType defines the Iceberg catalog backend.
type CatalogType string

const (
	// CatalogHadoop uses the Hadoop catalog (file/warehouse based).
	CatalogHadoop CatalogType = "hadoop"
	// CatalogGlue uses the AWS Glue catalog.
	CatalogGlue CatalogType = "glue"
	// CatalogRest uses the REST catalog (reserved).
	CatalogRest CatalogType = "rest"
)

// SchemaMode controls how schema evolution is handled.
type SchemaMode string

const (
	// SchemaModeStrict rejects incompatible schema changes.
	SchemaModeStrict SchemaMode = "strict"
	// SchemaModeEvolve allows compatible schema evolution.
	SchemaModeEvolve SchemaMode = "evolve"
)

// Config stores Iceberg sink configuration values.
type Config struct {
	WarehouseURI      string
	WarehouseLocation string
	Namespace         string
	Catalog           CatalogType
	CatalogURI        string
	AWSRegion         string
	GlueDatabase      string
	Mode              Mode

	CommitInterval      time.Duration
	TargetFileSizeBytes int64
	AutoTuneFileSize    bool
	Partitioning        string

	SchemaMode SchemaMode

	EmitMetadataColumns bool

	EnableCheckpointTable bool

	EnableGlobalCheckpointTable bool

	AllowTakeover bool

	MaxBufferedRows          int64
	MaxBufferedBytes         int64
	MaxBufferedRowsPerTable  int64
	MaxBufferedBytesPerTable int64
}

// NewConfig returns a Config initialized with default values.
func NewConfig() *Config {
	return &Config{
		Namespace:                   defaultNamespace,
		Catalog:                     CatalogHadoop,
		Mode:                        ModeAppend,
		CommitInterval:              defaultCommitInterval,
		TargetFileSizeBytes:         defaultTargetFileSizeBytes,
		AutoTuneFileSize:            false,
		Partitioning:                "",
		SchemaMode:                  SchemaModeStrict,
		EmitMetadataColumns:         true,
		EnableCheckpointTable:       false,
		EnableGlobalCheckpointTable: false,
		AllowTakeover:               false,
		MaxBufferedRows:             0,
		MaxBufferedBytes:            0,
		MaxBufferedRowsPerTable:     0,
		MaxBufferedBytesPerTable:    0,
	}
}

// Apply parses the sink URI and applies Iceberg-specific options.
func (c *Config) Apply(_ context.Context, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	if sinkURI == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("sink uri is empty")
	}
	scheme := config.GetScheme(sinkURI)
	if scheme != config.IcebergScheme {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported scheme: %s", scheme))
	}

	query := sinkURI.Query()
	catalogSpecified := false
	autoTuneSpecified := false
	partitioningSpecified := false

	if sinkConfig != nil && sinkConfig.IcebergConfig != nil {
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Warehouse)); v != "" {
			if err := c.setWarehouse(v); err != nil {
				return err
			}
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Namespace)); v != "" {
			c.Namespace = v
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Catalog)); v != "" {
			if err := c.setCatalog(v); err != nil {
				return err
			}
			catalogSpecified = true
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.CatalogURI)); v != "" {
			c.CatalogURI = v
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Region)); v != "" {
			c.AWSRegion = v
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Database)); v != "" {
			c.GlueDatabase = v
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.Mode)); v != "" {
			if err := c.setMode(v); err != nil {
				return err
			}
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.CommitInterval)); v != "" {
			if err := c.setCommitInterval(v); err != nil {
				return err
			}
		}
		if sinkConfig.IcebergConfig.TargetFileSize != nil {
			if err := c.setTargetFileSize(*sinkConfig.IcebergConfig.TargetFileSize); err != nil {
				return err
			}
		}
		if sinkConfig.IcebergConfig.AutoTuneFileSize != nil {
			c.AutoTuneFileSize = *sinkConfig.IcebergConfig.AutoTuneFileSize
			autoTuneSpecified = true
		}
		if sinkConfig.IcebergConfig.Partitioning != nil {
			c.Partitioning = strings.TrimSpace(*sinkConfig.IcebergConfig.Partitioning)
			partitioningSpecified = true
		}
		if sinkConfig.IcebergConfig.EmitMetadataColumns != nil {
			c.EmitMetadataColumns = *sinkConfig.IcebergConfig.EmitMetadataColumns
		}
		if sinkConfig.IcebergConfig.EnableCheckpointTable != nil {
			c.EnableCheckpointTable = *sinkConfig.IcebergConfig.EnableCheckpointTable
		}
		if sinkConfig.IcebergConfig.EnableGlobalCheckpointTable != nil {
			c.EnableGlobalCheckpointTable = *sinkConfig.IcebergConfig.EnableGlobalCheckpointTable
		}
		if sinkConfig.IcebergConfig.AllowTakeover != nil {
			c.AllowTakeover = *sinkConfig.IcebergConfig.AllowTakeover
		}
		if v := strings.TrimSpace(getOrEmpty(sinkConfig.IcebergConfig.SchemaMode)); v != "" {
			if err := c.setSchemaMode(v); err != nil {
				return err
			}
		}
		if sinkConfig.IcebergConfig.MaxBufferedRows != nil {
			c.MaxBufferedRows = *sinkConfig.IcebergConfig.MaxBufferedRows
		}
		if sinkConfig.IcebergConfig.MaxBufferedBytes != nil {
			c.MaxBufferedBytes = *sinkConfig.IcebergConfig.MaxBufferedBytes
		}
		if sinkConfig.IcebergConfig.MaxBufferedRowsPerTable != nil {
			c.MaxBufferedRowsPerTable = *sinkConfig.IcebergConfig.MaxBufferedRowsPerTable
		}
		if sinkConfig.IcebergConfig.MaxBufferedBytesPerTable != nil {
			c.MaxBufferedBytesPerTable = *sinkConfig.IcebergConfig.MaxBufferedBytesPerTable
		}
	}

	if v := strings.TrimSpace(query.Get("warehouse")); v != "" {
		if err := c.setWarehouse(v); err != nil {
			return err
		}
	}
	if v := strings.TrimSpace(query.Get("namespace")); v != "" {
		c.Namespace = v
	}
	if v := strings.TrimSpace(query.Get("catalog")); v != "" {
		if err := c.setCatalog(v); err != nil {
			return err
		}
		catalogSpecified = true
	}
	if v := strings.TrimSpace(query.Get("catalog-uri")); v != "" {
		c.CatalogURI = v
	}
	if v := strings.TrimSpace(query.Get("region")); v != "" {
		c.AWSRegion = v
	}
	if v := strings.TrimSpace(query.Get("database")); v != "" {
		c.GlueDatabase = v
	}

	if v := strings.TrimSpace(query.Get("mode")); v != "" {
		if err := c.setMode(v); err != nil {
			return err
		}
	}

	if v := strings.TrimSpace(query.Get("commit-interval")); v != "" {
		if err := c.setCommitInterval(v); err != nil {
			return err
		}
	}

	if v := strings.TrimSpace(query.Get("target-file-size")); v != "" {
		fileSizeBytes, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		if err := c.setTargetFileSize(fileSizeBytes); err != nil {
			return err
		}
	}
	if v := strings.TrimSpace(query.Get("auto-tune-file-size")); v != "" {
		autoTune, err := strconv.ParseBool(v)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.AutoTuneFileSize = autoTune
		autoTuneSpecified = true
	}

	if _, ok := query["partitioning"]; ok {
		c.Partitioning = strings.TrimSpace(query.Get("partitioning"))
		partitioningSpecified = true
	}

	if emitMetaStr := strings.TrimSpace(query.Get("emit-metadata-columns")); emitMetaStr != "" {
		v, err := strconv.ParseBool(emitMetaStr)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.EmitMetadataColumns = v
	}
	if checkpointStr := strings.TrimSpace(query.Get("enable-checkpoint-table")); checkpointStr != "" {
		v, err := strconv.ParseBool(checkpointStr)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.EnableCheckpointTable = v
	}
	if checkpointStr := strings.TrimSpace(query.Get("enable-global-checkpoint-table")); checkpointStr != "" {
		v, err := strconv.ParseBool(checkpointStr)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.EnableGlobalCheckpointTable = v
	}
	if takeoverStr := strings.TrimSpace(query.Get("allow-takeover")); takeoverStr != "" {
		v, err := strconv.ParseBool(takeoverStr)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.AllowTakeover = v
	}

	if v := strings.TrimSpace(query.Get("schema-mode")); v != "" {
		if err := c.setSchemaMode(v); err != nil {
			return err
		}
	}
	if v := strings.TrimSpace(query.Get("schema_mode")); v != "" {
		if err := c.setSchemaMode(v); err != nil {
			return err
		}
	}

	if v := strings.TrimSpace(query.Get("max-buffered-rows")); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.MaxBufferedRows = parsed
	}
	if v := strings.TrimSpace(query.Get("max-buffered-bytes")); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.MaxBufferedBytes = parsed
	}
	if v := strings.TrimSpace(query.Get("max-buffered-rows-per-table")); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.MaxBufferedRowsPerTable = parsed
	}
	if v := strings.TrimSpace(query.Get("max-buffered-bytes-per-table")); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
		}
		c.MaxBufferedBytesPerTable = parsed
	}

	if c.WarehouseURI == "" {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("missing required parameter: warehouse")
	}
	if !catalogSpecified && isS3Warehouse(c.WarehouseURI) {
		c.Catalog = CatalogGlue
	}
	if c.Catalog == CatalogRest && strings.TrimSpace(c.CatalogURI) == "" {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("missing required parameter: catalog-uri")
	}

	if !partitioningSpecified && strings.TrimSpace(c.Partitioning) == "" && c.EmitMetadataColumns && c.Mode == ModeAppend {
		c.Partitioning = defaultPartitioning
	}
	if !c.EmitMetadataColumns && partitioningUsesMetadataColumns(c.Partitioning) {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("partitioning requires emit-metadata-columns=true")
	}
	if !autoTuneSpecified && c.Mode == ModeUpsert {
		c.AutoTuneFileSize = true
	}

	if c.MaxBufferedRows < 0 || c.MaxBufferedBytes < 0 || c.MaxBufferedRowsPerTable < 0 || c.MaxBufferedBytesPerTable < 0 {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg buffer limits must be non negative")
	}

	return nil
}

func getOrEmpty(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (c *Config) setWarehouse(warehouseURI string) error {
	warehouseLocation, err := sanitizeLocationURI(warehouseURI)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	c.WarehouseURI = warehouseURI
	c.WarehouseLocation = warehouseLocation
	return nil
}

func (c *Config) setCatalog(catalog string) error {
	switch CatalogType(strings.ToLower(catalog)) {
	case CatalogHadoop:
		c.Catalog = CatalogHadoop
	case CatalogGlue:
		c.Catalog = CatalogGlue
	case CatalogRest:
		c.Catalog = CatalogRest
	default:
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported iceberg catalog: %s", catalog))
	}
	return nil
}

func (c *Config) setMode(mode string) error {
	switch Mode(strings.ToLower(mode)) {
	case ModeAppend:
		c.Mode = ModeAppend
	case ModeUpsert:
		c.Mode = ModeUpsert
	default:
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported iceberg mode: %s", mode))
	}
	return nil
}

func (c *Config) setSchemaMode(mode string) error {
	switch SchemaMode(strings.ToLower(strings.TrimSpace(mode))) {
	case SchemaModeStrict:
		c.SchemaMode = SchemaModeStrict
	case SchemaModeEvolve:
		c.SchemaMode = SchemaModeEvolve
	default:
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(fmt.Sprintf("unsupported iceberg schema mode: %s", mode))
	}
	return nil
}

func (c *Config) setCommitInterval(intervalStr string) error {
	interval, err := time.ParseDuration(intervalStr)
	if err != nil {
		return cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	if interval < minCommitInterval {
		interval = minCommitInterval
	}
	if interval > maxCommitInterval {
		interval = maxCommitInterval
	}
	c.CommitInterval = interval
	return nil
}

func (c *Config) setTargetFileSize(fileSizeBytes int64) error {
	if fileSizeBytes < minTargetFileSizeBytes {
		fileSizeBytes = minTargetFileSizeBytes
	}
	if fileSizeBytes > maxTargetFileSizeBytes {
		fileSizeBytes = maxTargetFileSizeBytes
	}
	c.TargetFileSizeBytes = fileSizeBytes
	return nil
}

func sanitizeLocationURI(rawURI string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return "", err
	}
	parsed.User = nil
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func isS3Warehouse(warehouseURI string) bool {
	parsed, err := url.Parse(warehouseURI)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Scheme, "s3")
}

func partitioningUsesMetadataColumns(expr string) bool {
	s := strings.ToLower(strings.TrimSpace(expr))
	if s == "" {
		return false
	}
	return strings.Contains(s, "_tidb_commit_time") ||
		strings.Contains(s, "_tidb_commit_ts") ||
		strings.Contains(s, "_tidb_op")
}
