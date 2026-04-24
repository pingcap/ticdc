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

package main

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
)

const (
	ddlModeFixed  = "fixed"
	ddlModeRandom = "random"
)

var defaultInlineDDLRate = DDLRatePerMinute{
	AddColumn:     2,
	DropColumn:    1,
	AddIndex:      4,
	DropIndex:     2,
	TruncateTable: 0,
}

type DDLConfig struct {
	Mode          string           `toml:"mode"`
	RatePerMinute DDLRatePerMinute `toml:"rate_per_minute"`
	Tables        []string         `toml:"tables"`
}

type DDLRatePerMinute struct {
	AddColumn     int `toml:"add_column"`
	DropColumn    int `toml:"drop_column"`
	AddIndex      int `toml:"add_index"`
	DropIndex     int `toml:"drop_index"`
	TruncateTable int `toml:"truncate_table"`
}

func LoadDDLConfig(path string) (*DDLConfig, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("ddl config path is empty")
	}
	if filepath.Ext(path) != ".toml" {
		return nil, errors.Errorf("ddl config must be a .toml file: %s", path)
	}

	var cfg DDLConfig
	meta, err := toml.DecodeFile(path, &cfg)
	if err != nil {
		return nil, errors.Annotate(err, "decode ddl config failed")
	}
	if undecoded := meta.Undecoded(); len(undecoded) > 0 {
		return nil, errors.Errorf("unknown keys in ddl config: %v", undecoded)
	}

	cfg.normalize()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func LoadDDLConfigFromWorkloadConfig(cfg *WorkloadConfig) (*DDLConfig, error) {
	if cfg == nil {
		return nil, errors.New("workload config is nil")
	}

	path := strings.TrimSpace(cfg.DDLConfigPath)
	if path != "" {
		if cfg.HasInlineDDLConfig() {
			return nil, errors.New("ddl-config cannot be used together with inline ddl flags")
		}
		return LoadDDLConfig(path)
	}

	inlineCfg := &DDLConfig{
		Mode:          chooseInlineDDLMode(cfg),
		RatePerMinute: cfg.DDLRate,
	}
	if inlineCfg.RatePerMinute.totalRate() == 0 {
		inlineCfg.RatePerMinute = defaultInlineDDLRate
	}
	inlineCfg.Tables = inferDDLTables(cfg, inlineCfg.Mode)
	inlineCfg.normalize()
	if err := inlineCfg.validate(); err != nil {
		return nil, err
	}
	return inlineCfg, nil
}

func (c *DDLConfig) normalize() {
	c.Mode = strings.ToLower(strings.TrimSpace(c.Mode))
	if c.Mode == "" {
		if len(c.Tables) > 0 {
			c.Mode = ddlModeFixed
		} else {
			c.Mode = ddlModeRandom
		}
	}

	// Trim and drop empty entries.
	tables := make([]string, 0, len(c.Tables))
	for _, t := range c.Tables {
		t = strings.TrimSpace(t)
		if t != "" {
			tables = append(tables, t)
		}
	}
	c.Tables = tables
}

func (c *DDLConfig) validate() error {
	if c.Mode != ddlModeFixed && c.Mode != ddlModeRandom {
		return errors.Errorf("unsupported ddl mode: %s", c.Mode)
	}
	if c.Mode == ddlModeFixed && len(c.Tables) == 0 {
		return errors.New("ddl mode fixed requires tables")
	}

	if err := validateRate("add_column", c.RatePerMinute.AddColumn); err != nil {
		return err
	}
	if err := validateRate("drop_column", c.RatePerMinute.DropColumn); err != nil {
		return err
	}
	if err := validateRate("add_index", c.RatePerMinute.AddIndex); err != nil {
		return err
	}
	if err := validateRate("drop_index", c.RatePerMinute.DropIndex); err != nil {
		return err
	}
	if err := validateRate("truncate_table", c.RatePerMinute.TruncateTable); err != nil {
		return err
	}

	if c.totalRate() == 0 {
		return errors.New("ddl config has no enabled ddl types")
	}
	return nil
}

func validateRate(name string, v int) error {
	if v < 0 {
		return errors.Errorf("ddl rate must be >= 0: %s=%d", name, v)
	}
	return nil
}

func (c *DDLConfig) totalRate() int {
	return c.RatePerMinute.AddColumn +
		c.RatePerMinute.DropColumn +
		c.RatePerMinute.AddIndex +
		c.RatePerMinute.DropIndex +
		c.RatePerMinute.TruncateTable
}

func (r DDLRatePerMinute) totalRate() int {
	return r.AddColumn + r.DropColumn + r.AddIndex + r.DropIndex + r.TruncateTable
}

func inferDDLTables(cfg *WorkloadConfig, mode string) []string {
	if cfg == nil {
		return nil
	}
	if strings.ToLower(strings.TrimSpace(mode)) != ddlModeFixed {
		return nil
	}
	if cfg.WorkloadType != fastSlow {
		return nil
	}

	fastTableCount := cfg.TableCount / 2
	if fastTableCount == 0 && cfg.TableCount > 1 {
		fastTableCount = 1
	}

	slowStart := cfg.TableStartIndex + fastTableCount
	slowCount := cfg.TableCount - fastTableCount
	if slowCount <= 0 {
		return nil
	}

	tables := make([]string, 0, slowCount)
	for i := 0; i < slowCount; i++ {
		tableIndex := slowStart + i
		tables = append(tables, fmt.Sprintf("%s.slow_table_%d", strings.TrimSpace(cfg.DBName), tableIndex))
	}
	return tables
}

func chooseInlineDDLMode(cfg *WorkloadConfig) string {
	if cfg == nil {
		return ddlModeRandom
	}
	if cfg.WorkloadType == fastSlow {
		return ddlModeFixed
	}
	return ddlModeRandom
}

type TableName struct {
	Schema string
	Name   string
}

func ParseTableName(raw string, defaultSchema string) (TableName, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return TableName{}, errors.New("table name is empty")
	}

	parts := strings.Split(raw, ".")
	switch len(parts) {
	case 1:
		name := strings.TrimSpace(parts[0])
		if name == "" {
			return TableName{}, errors.Errorf("invalid table name: %s", raw)
		}
		if strings.TrimSpace(defaultSchema) == "" {
			return TableName{}, errors.Errorf("table %s missing schema", raw)
		}
		return TableName{Schema: strings.TrimSpace(defaultSchema), Name: name}, nil
	case 2:
		schema := strings.TrimSpace(parts[0])
		name := strings.TrimSpace(parts[1])
		if schema == "" || name == "" {
			return TableName{}, errors.Errorf("invalid table name: %s", raw)
		}
		return TableName{Schema: schema, Name: name}, nil
	default:
		return TableName{}, errors.Errorf("invalid table name: %s", raw)
	}
}

func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}
