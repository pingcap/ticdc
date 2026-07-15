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

package bank

import (
	"regexp"
	"strings"
	"testing"
)

func TestUniformWriteSchemaAndInsert(t *testing.T) {
	workload := NewBankWorkload(false, true, 0)

	createSQL := workload.BuildCreateTableStatement(7)
	for _, expected := range []string{
		"auto_id bigint(20) NOT NULL,",
		"PRIMARY KEY (auto_id) /*T![clustered_index] CLUSTERED */",
	} {
		if !strings.Contains(createSQL, expected) {
			t.Fatalf("create SQL does not contain %q: %s", expected, createSQL)
		}
	}
	for _, unexpected := range []string{"AUTO_INCREMENT", "NONCLUSTERED"} {
		if strings.Contains(createSQL, unexpected) {
			t.Fatalf("create SQL unexpectedly contains %q: %s", unexpected, createSQL)
		}
	}
	if strings.Contains(createSQL, "KEY idx") {
		t.Fatalf("uniform schema unexpectedly contains a secondary index: %s", createSQL)
	}

	insertSQL := workload.BuildInsertSql(7, 2)
	for _, pattern := range []string{
		`^insert into bank_7 \(auto_id,col1,.*\) values\([0-9]+,`,
		`\),\([0-9]+,`,
	} {
		if !regexp.MustCompile(pattern).MatchString(insertSQL) {
			t.Fatalf("insert SQL does not match %q: %s", pattern, insertSQL)
		}
	}
}

func TestDefaultSchemaAndInsertRemainUnchanged(t *testing.T) {
	workload := NewBankWorkload(false, false, 0)

	createSQL := workload.BuildCreateTableStatement(7)
	for _, expected := range []string{
		"auto_id bigint(20) NOT NULL AUTO_INCREMENT,",
		"PRIMARY KEY (auto_id,col3,col4) /*T![clustered_index] NONCLUSTERED */",
		"KEY idx6 (col3)",
	} {
		if !strings.Contains(createSQL, expected) {
			t.Fatalf("create SQL does not contain %q: %s", expected, createSQL)
		}
	}

	insertSQL := workload.BuildInsertSql(7, 1)
	if !strings.Contains(insertSQL, "insert into bank_7 (col1,col2") {
		t.Fatalf("default insert SQL has unexpected columns: %s", insertSQL)
	}
	if strings.Contains(insertSQL, "(auto_id,col1") {
		t.Fatalf("default insert SQL unexpectedly supplies auto_id: %s", insertSQL)
	}
}

func TestUniformWritePayload(t *testing.T) {
	workload := NewBankWorkload(false, true, 32)

	createSQL := workload.BuildCreateTableStatement(7)
	if !strings.Contains(createSQL, "payload VARBINARY(4096) DEFAULT NULL") {
		t.Fatalf("payload schema is missing: %s", createSQL)
	}

	insertSQL := workload.BuildInsertSql(7, 2)
	if !strings.Contains(insertSQL, "col29,col30,payload) values") {
		t.Fatalf("payload insert column is missing: %s", insertSQL)
	}
	if count := strings.Count(insertSQL, "'"+strings.Repeat("x", 32)+"'"); count != 2 {
		t.Fatalf("payload value count = %d, want 2: %s", count, insertSQL)
	}
}
