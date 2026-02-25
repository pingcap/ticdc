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
	"os"
	"path/filepath"
	"testing"
)

func TestParseTableName(t *testing.T) {
	t.Parallel()

	t.Run("schema qualified", func(t *testing.T) {
		table, err := ParseTableName("test.sbtest1", "ignored")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if table.Schema != "test" || table.Name != "sbtest1" {
			t.Fatalf("unexpected table: %+v", table)
		}
	})

	t.Run("default schema", func(t *testing.T) {
		table, err := ParseTableName("sbtest1", "test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if table.Schema != "test" || table.Name != "sbtest1" {
			t.Fatalf("unexpected table: %+v", table)
		}
	})

	t.Run("missing schema", func(t *testing.T) {
		_, err := ParseTableName("sbtest1", "")
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("invalid", func(t *testing.T) {
		_, err := ParseTableName("a.b.c", "test")
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}

func TestLoadDDLConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	t.Run("extension check", func(t *testing.T) {
		path := filepath.Join(dir, "ddl.txt")
		if err := os.WriteFile(path, []byte("mode = \"fixed\""), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
		_, err := LoadDDLConfig(path)
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("default mode fixed", func(t *testing.T) {
		path := filepath.Join(dir, "ddl_fixed.toml")
		content := `
tables = ["test.sbtest1"]

[rate_per_minute]
add_column = 1
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
		cfg, err := LoadDDLConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Mode != ddlModeFixed {
			t.Fatalf("expected mode fixed, got %s", cfg.Mode)
		}
	})

	t.Run("default mode random", func(t *testing.T) {
		path := filepath.Join(dir, "ddl_random.toml")
		content := `
[rate_per_minute]
add_column = 1
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
		cfg, err := LoadDDLConfig(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cfg.Mode != ddlModeRandom {
			t.Fatalf("expected mode random, got %s", cfg.Mode)
		}
	})

	t.Run("no enabled ddl types", func(t *testing.T) {
		path := filepath.Join(dir, "ddl_empty.toml")
		content := `
mode = "fixed"

tables = ["test.sbtest1"]

[rate_per_minute]
add_column = 0
drop_column = 0
add_index = 0
drop_index = 0
truncate_table = 0
`
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("write file failed: %v", err)
		}
		_, err := LoadDDLConfig(path)
		if err == nil {
			t.Fatalf("expected error")
		}
	})
}
