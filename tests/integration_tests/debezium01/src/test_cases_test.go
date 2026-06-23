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
	"path/filepath"
	"strings"
	"testing"
)

func TestShouldSkipTestCase(t *testing.T) {
	reason, ok := shouldSkipTestCase("sql", filepath.Join("sql", "debezium", "mysql-dbz-193.ddl"))
	if !ok {
		t.Fatal("shouldSkipTestCase(mysql-dbz-193.ddl) = false, want true")
	}
	if !strings.Contains(reason, "FULLTEXT") {
		t.Fatalf("shouldSkipTestCase(mysql-dbz-193.ddl) reason = %q, want mention FULLTEXT", reason)
	}

	_, ok = shouldSkipTestCase("sql", filepath.Join("sql", "debezium", "mysql-dbz-123.ddl"))
	if ok {
		t.Fatal("shouldSkipTestCase(mysql-dbz-123.ddl) = true, want false")
	}
}
