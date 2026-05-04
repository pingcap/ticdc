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

package hotspot

import (
	"strings"
	"testing"

	"workload/schema"
)

func TestBuildUpdateSqlUsesHotRows(t *testing.T) {
	t.Parallel()

	workload := NewHotspotWorkload(3)
	sql := workload.BuildUpdateSql(schema.UpdateOption{
		TableIndex: 0,
		Batch:      10,
	})

	if !strings.Contains(sql, "INSERT INTO hotspot_0") {
		t.Fatalf("expected hotspot table insert, got %s", sql)
	}
	if !strings.Contains(sql, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("expected upsert update, got %s", sql)
	}
	for _, key := range []string{"(1,1,1", "(2,1,1", "(3,1,1"} {
		if !strings.Contains(sql, key) {
			t.Fatalf("expected hot key %s in sql: %s", key, sql)
		}
	}
	if strings.Contains(sql, "(4,1,1") {
		t.Fatalf("unexpected key outside hot row count: %s", sql)
	}
}

func TestBuildDeleteSqlUsesHotRows(t *testing.T) {
	t.Parallel()

	workload := NewHotspotWorkload(1)
	sql := workload.BuildDeleteSql(schema.DeleteOption{
		TableIndex: 2,
		Batch:      8,
	})

	if sql != "DELETE FROM hotspot_2 WHERE id IN (1)" {
		t.Fatalf("unexpected delete sql: %s", sql)
	}
}
