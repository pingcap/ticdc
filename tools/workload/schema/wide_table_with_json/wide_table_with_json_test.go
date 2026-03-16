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

package widetablewithjson

import (
	"bytes"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"workload/schema"
)

func TestWideTableWithJSONWorkloadRowSizingAndClamping(t *testing.T) {
	w := NewWideTableWithJSONWorkload(10240, 4, 0, 1000, "const").(*WideTableWithJSONWorkload)

	if w.entityMediaSize != maxEntityMediaMetadataSize {
		t.Fatalf("entityMediaSize mismatch: got %d, want %d", w.entityMediaSize, maxEntityMediaMetadataSize)
	}
	if w.batchAuxSize != 1280 {
		t.Fatalf("batchAuxSize mismatch: got %d, want %d", w.batchAuxSize, 1280)
	}
	if w.batchCbSize != 1280 {
		t.Fatalf("batchCbSize mismatch: got %d, want %d", w.batchCbSize, 1280)
	}
	if w.batchMetaSize != 7680 {
		t.Fatalf("batchMetaSize mismatch: got %d, want %d", w.batchMetaSize, 7680)
	}
	if w.perTableUpdateKeySpace != 250 {
		t.Fatalf("perTableUpdateKeySpace mismatch: got %d, want %d", w.perTableUpdateKeySpace, 250)
	}

	w2 := NewWideTableWithJSONWorkload(maxBatchMetadataSize+100000, 1, 0, 0, "const").(*WideTableWithJSONWorkload)
	if w2.batchMetaSize != maxBatchMetadataSize {
		t.Fatalf("batchMetaSize should be clamped: got %d, want %d", w2.batchMetaSize, maxBatchMetadataSize)
	}
	if w2.entityMediaSize != maxEntityMediaMetadataSize {
		t.Fatalf("entityMediaSize should be clamped: got %d, want %d", w2.entityMediaSize, maxEntityMediaMetadataSize)
	}
}

func TestWideTableWithJSONWorkloadBuildCreateTableStatement(t *testing.T) {
	w := NewWideTableWithJSONWorkload(1024, 2, 0, 100, "const").(*WideTableWithJSONWorkload)

	sql0 := w.BuildCreateTableStatement(0)
	if !strings.Contains(sql0, "CREATE TABLE IF NOT EXISTS `wide_table_with_json_primary`") {
		t.Fatalf("unexpected primary table ddl: %s", sql0)
	}
	if !strings.Contains(sql0, "CREATE TABLE IF NOT EXISTS `wide_table_with_json_secondary`") {
		t.Fatalf("unexpected secondary table ddl: %s", sql0)
	}
	if strings.Contains(sql0, "entity_metadata") || strings.Contains(sql0, "batch_metadata") || strings.Contains(sql0, "user_id") {
		t.Fatalf("ddl should be anonymized: %s", sql0)
	}

	sql1 := w.BuildCreateTableStatement(1)
	if !strings.Contains(sql1, "CREATE TABLE IF NOT EXISTS `wide_table_with_json_primary_1`") {
		t.Fatalf("unexpected primary table ddl for shard: %s", sql1)
	}
	if !strings.Contains(sql1, "CREATE TABLE IF NOT EXISTS `wide_table_with_json_secondary_1`") {
		t.Fatalf("unexpected secondary table ddl for shard: %s", sql1)
	}
}

func TestWideTableWithJSONWorkloadBuildInsertSqlWithValues(t *testing.T) {
	w := NewWideTableWithJSONWorkload(4096, 1, 0, 100, "const").(*WideTableWithJSONWorkload)
	w.randPool = sync.Pool{New: func() any { return rand.New(rand.NewSource(1)) }}

	var (
		foundEntity bool
		foundBatch  bool
	)

	for i := 0; i < 128 && (!foundEntity || !foundBatch); i++ {
		sql, values := w.BuildInsertSqlWithValues(0, 3)

		switch {
		case strings.Contains(sql, "INSERT INTO `wide_table_with_json_primary`"):
			foundEntity = true
			colCount := 10
			if strings.Contains(sql, "sync_at") {
				colCount = 11
			}
			if len(values) != 3*colCount {
				t.Fatalf("primary insert values len mismatch: got %d, want %d", len(values), 3*colCount)
			}
			media, ok := values[colCount-1].(string)
			if !ok {
				t.Fatalf("primary payload_text type mismatch: %T", values[colCount-1])
			}
			if len(media) != w.entityMediaSize {
				t.Fatalf("primary payload_text len mismatch: got %d, want %d", len(media), w.entityMediaSize)
			}
		case strings.Contains(sql, "INSERT INTO `wide_table_with_json_secondary`"):
			foundBatch = true
			colCount := 9
			if strings.Contains(sql, "`payload_aux`,") {
				colCount = 12
			}
			if len(values) != 3*colCount {
				t.Fatalf("secondary insert values len mismatch: got %d, want %d", len(values), 3*colCount)
			}
			metaBytes, ok := values[6].([]byte)
			if !ok {
				t.Fatalf("secondary payload_blob type mismatch: %T", values[6])
			}
			if len(metaBytes) != w.batchMetaSize {
				t.Fatalf("secondary payload_blob len mismatch: got %d, want %d", len(metaBytes), w.batchMetaSize)
			}
			if colCount == 12 {
				aux, ok := values[8].(string)
				if !ok {
					t.Fatalf("secondary payload_aux type mismatch: %T", values[8])
				}
				if len(aux) != w.batchAuxSize {
					t.Fatalf("secondary payload_aux len mismatch: got %d, want %d", len(aux), w.batchAuxSize)
				}
			}
		default:
			t.Fatalf("unexpected insert sql: %s", sql)
		}
	}

	if !foundEntity {
		t.Fatalf("expected at least one primary insert")
	}
	if !foundBatch {
		t.Fatalf("expected at least one secondary insert")
	}
}

func TestWideTableWithJSONWorkloadBuildUpdateSqlWithValues(t *testing.T) {
	w := NewWideTableWithJSONWorkload(1024, 1, 0, 100, "const").(*WideTableWithJSONWorkload)
	w.randPool = sync.Pool{New: func() any { return rand.New(rand.NewSource(2)) }}

	// Seed some rows so updates have a chance to match.
	_, _ = w.BuildInsertSqlWithValues(0, 8)
	_, _ = w.BuildInsertSqlWithValues(0, 8)

	var (
		foundEntity bool
		foundBatch  bool
	)

	for i := 0; i < 256 && (!foundEntity || !foundBatch); i++ {
		sql, values := w.BuildUpdateSqlWithValues(schema.UpdateOption{TableIndex: 0, Batch: 4})

		switch {
		case strings.Contains(sql, "UPDATE `wide_table_with_json_primary`"):
			foundEntity = true
			switch {
			case strings.Contains(sql, "WHERE (`lookup_key` = ?)"):
				if len(values) != 4 {
					t.Fatalf("primary update by lookup_key values len mismatch: got %d, want %d", len(values), 4)
				}
			case strings.Contains(sql, "WHERE (`attr_key_2` = ?)"):
				if len(values) != 8 {
					t.Fatalf("primary update by composite values len mismatch: got %d, want %d", len(values), 8)
				}
			case strings.Contains(sql, "SET `id` = ?"):
				if len(values) != 4 {
					t.Fatalf("primary update by id values len mismatch: got %d, want %d", len(values), 4)
				}
			default:
				t.Fatalf("unexpected primary update sql: %s", sql)
			}
		case strings.Contains(sql, "UPDATE `wide_table_with_json_secondary`"):
			foundBatch = true
			switch {
			case strings.Contains(sql, "SET `updated_at` = ?, `state` = ?"):
				if len(values) != 3 {
					t.Fatalf("secondary state update values len mismatch: got %d, want %d", len(values), 3)
				}
			case strings.Contains(sql, "SET `payload_blob` = ?, `updated_at` = ?, `state` = ?"):
				if len(values) != 4 {
					t.Fatalf("secondary payload update values len mismatch: got %d, want %d", len(values), 4)
				}
			case strings.Contains(sql, "SET `payload_blob` = ?, `updated_at` = ?, `owner_key` = ?"):
				if len(values) != 6 {
					t.Fatalf("secondary payload and key update values len mismatch: got %d, want %d", len(values), 6)
				}
			case strings.Contains(sql, "SET `event_time_2` = ?, `payload_blob` = ?"):
				if len(values) != 8 {
					t.Fatalf("secondary time update values len mismatch: got %d, want %d", len(values), 8)
				}
			default:
				t.Fatalf("unexpected secondary update sql: %s", sql)
			}

			// Validate metadata payload when present.
			for _, v := range values {
				b, ok := v.([]byte)
				if !ok {
					continue
				}
				if len(b) != w.batchMetaSize {
					t.Fatalf("secondary payload len mismatch: got %d, want %d", len(b), w.batchMetaSize)
				}
			}
		default:
			t.Fatalf("unexpected update sql: %s", sql)
		}
	}

	if !foundEntity {
		t.Fatalf("expected at least one primary update")
	}
	if !foundBatch {
		t.Fatalf("expected at least one secondary update")
	}
}

func TestWideTableWithJSONWorkloadZstdPayloadMode(t *testing.T) {
	w := NewWideTableWithJSONWorkload(2048, 1, 0, 100, "zstd").(*WideTableWithJSONWorkload)
	r := rand.New(rand.NewSource(3))

	_, entityValues := w.buildPrimaryInsertWithValues(0, 1, r)
	media, ok := entityValues[9].(string)
	if !ok {
		t.Fatalf("primary payload_text type mismatch: %T", entityValues[9])
	}
	if len(media) != w.entityMediaSize {
		t.Fatalf("primary payload_text len mismatch: got %d, want %d", len(media), w.entityMediaSize)
	}
	if !strings.Contains(media, `{"key":"`) {
		t.Fatalf("expected zstd payload signature in primary payload_text")
	}

	_, batchValues := w.buildSecondaryInsertWithValues(0, 1, r)
	metaBytes, ok := batchValues[6].([]byte)
	if !ok {
		t.Fatalf("secondary payload_blob type mismatch: %T", batchValues[6])
	}
	if len(metaBytes) != w.batchMetaSize {
		t.Fatalf("secondary payload_blob len mismatch: got %d, want %d", len(metaBytes), w.batchMetaSize)
	}
	if !bytes.Contains(metaBytes, []byte(`{"key":"`)) {
		t.Fatalf("expected zstd payload signature in secondary payload_blob")
	}
}
