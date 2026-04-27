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

package batchdelete

import (
	"strings"
	"testing"

	"workload/schema"
)

func TestBuildCreateTableStatement(t *testing.T) {
	t.Parallel()

	workload := NewBatchDeleteWorkload(16)
	sql := workload.BuildCreateTableStatement(3)
	if !strings.Contains(sql, "CREATE TABLE if not exists batch_delete3") {
		t.Fatalf("unexpected create table sql: %s", sql)
	}
	if !strings.Contains(sql, "PRIMARY KEY (id)") {
		t.Fatalf("create table sql should define id primary key: %s", sql)
	}
}

func TestBuildInsertSql(t *testing.T) {
	t.Parallel()

	workload := NewBatchDeleteWorkload(8)
	sql := workload.BuildInsertSql(1, 3)
	expected := "INSERT INTO batch_delete1 (k, payload) VALUES (1, 'xxxxxxxx'),(2, 'xxxxxxxx'),(3, 'xxxxxxxx')"
	if sql != expected {
		t.Fatalf("unexpected insert sql, expected %s, got %s", expected, sql)
	}
}

func TestBuildDeleteSql(t *testing.T) {
	t.Parallel()

	workload := NewBatchDeleteWorkload(16)
	sql := workload.BuildDeleteSql(schema.DeleteOption{
		TableIndex: 2,
		Batch:      128,
	})
	expected := "DELETE FROM batch_delete2 ORDER BY id LIMIT 128"
	if sql != expected {
		t.Fatalf("unexpected delete sql, expected %s, got %s", expected, sql)
	}
}
