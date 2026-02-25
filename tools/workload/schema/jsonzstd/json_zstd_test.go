package jsonzstd

import (
	"bytes"
	"math/rand"
	"strings"
	"sync"
	"testing"

	"workload/schema"
)

func TestJSONZstdWorkloadRowSizingAndClamping(t *testing.T) {
	w := NewJSONZstdWorkload(10240, 4, 0, 1000, "const").(*JSONZstdWorkload)

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

	w2 := NewJSONZstdWorkload(maxBatchMetadataSize+100000, 1, 0, 0, "const").(*JSONZstdWorkload)
	if w2.batchMetaSize != maxBatchMetadataSize {
		t.Fatalf("batchMetaSize should be clamped: got %d, want %d", w2.batchMetaSize, maxBatchMetadataSize)
	}
	if w2.entityMediaSize != maxEntityMediaMetadataSize {
		t.Fatalf("entityMediaSize should be clamped: got %d, want %d", w2.entityMediaSize, maxEntityMediaMetadataSize)
	}
}

func TestJSONZstdWorkloadBuildCreateTableStatement(t *testing.T) {
	w := NewJSONZstdWorkload(1024, 2, 0, 100, "const").(*JSONZstdWorkload)

	sql0 := w.BuildCreateTableStatement(0)
	if !strings.Contains(sql0, "CREATE TABLE IF NOT EXISTS `json_zstd_entity_metadata`") {
		t.Fatalf("unexpected entity table ddl: %s", sql0)
	}
	if !strings.Contains(sql0, "CREATE TABLE IF NOT EXISTS `json_zstd_batch_metadata`") {
		t.Fatalf("unexpected batch table ddl: %s", sql0)
	}

	sql1 := w.BuildCreateTableStatement(1)
	if !strings.Contains(sql1, "CREATE TABLE IF NOT EXISTS `json_zstd_entity_metadata_1`") {
		t.Fatalf("unexpected entity table ddl for shard: %s", sql1)
	}
	if !strings.Contains(sql1, "CREATE TABLE IF NOT EXISTS `json_zstd_batch_metadata_1`") {
		t.Fatalf("unexpected batch table ddl for shard: %s", sql1)
	}
}

func TestJSONZstdWorkloadBuildInsertSqlWithValues(t *testing.T) {
	w := NewJSONZstdWorkload(4096, 1, 0, 100, "const").(*JSONZstdWorkload)
	w.randPool = sync.Pool{New: func() any { return rand.New(rand.NewSource(1)) }}

	var (
		foundEntity bool
		foundBatch  bool
	)

	for i := 0; i < 128 && (!foundEntity || !foundBatch); i++ {
		sql, values := w.BuildInsertSqlWithValues(0, 3)

		switch {
		case strings.Contains(sql, "INSERT INTO `json_zstd_entity_metadata`"):
			foundEntity = true
			colCount := 10
			if strings.Contains(sql, "migrated_at") {
				colCount = 11
			}
			if len(values) != 3*colCount {
				t.Fatalf("entity insert values len mismatch: got %d, want %d", len(values), 3*colCount)
			}
			media, ok := values[colCount-1].(string)
			if !ok {
				t.Fatalf("entity media_metadata type mismatch: %T", values[colCount-1])
			}
			if len(media) != w.entityMediaSize {
				t.Fatalf("entity media_metadata len mismatch: got %d, want %d", len(media), w.entityMediaSize)
			}
		case strings.Contains(sql, "INSERT INTO `json_zstd_batch_metadata`"):
			foundBatch = true
			colCount := 9
			if strings.Contains(sql, "aux_data") {
				colCount = 12
			}
			if len(values) != 3*colCount {
				t.Fatalf("batch insert values len mismatch: got %d, want %d", len(values), 3*colCount)
			}
			metaBytes, ok := values[6].([]byte)
			if !ok {
				t.Fatalf("batch metadata type mismatch: %T", values[6])
			}
			if len(metaBytes) != w.batchMetaSize {
				t.Fatalf("batch metadata len mismatch: got %d, want %d", len(metaBytes), w.batchMetaSize)
			}
			if colCount == 12 {
				aux, ok := values[8].(string)
				if !ok {
					t.Fatalf("batch aux_data type mismatch: %T", values[8])
				}
				if len(aux) != w.batchAuxSize {
					t.Fatalf("batch aux_data len mismatch: got %d, want %d", len(aux), w.batchAuxSize)
				}
			}
		default:
			t.Fatalf("unexpected insert sql: %s", sql)
		}
	}

	if !foundEntity {
		t.Fatalf("expected at least one entity insert")
	}
	if !foundBatch {
		t.Fatalf("expected at least one batch insert")
	}
}

func TestJSONZstdWorkloadBuildUpdateSqlWithValues(t *testing.T) {
	w := NewJSONZstdWorkload(1024, 1, 0, 100, "const").(*JSONZstdWorkload)
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
		case strings.Contains(sql, "UPDATE `json_zstd_entity_metadata`"):
			foundEntity = true
			switch {
			case strings.Contains(sql, "WHERE (`secondary_id` = ?)"):
				if len(values) != 4 {
					t.Fatalf("entity update by secondary_id values len mismatch: got %d, want %d", len(values), 4)
				}
			case strings.Contains(sql, "WHERE (`entity_id` = ?)"):
				if len(values) != 8 {
					t.Fatalf("entity update by composite values len mismatch: got %d, want %d", len(values), 8)
				}
			case strings.Contains(sql, "SET `id` = ?"):
				if len(values) != 4 {
					t.Fatalf("entity update by id values len mismatch: got %d, want %d", len(values), 4)
				}
			default:
				t.Fatalf("unexpected entity update sql: %s", sql)
			}
		case strings.Contains(sql, "UPDATE `json_zstd_batch_metadata`"):
			foundBatch = true
			switch {
			case strings.Contains(sql, "SET `updated_at` = ?, `status` = ?"):
				if len(values) != 3 {
					t.Fatalf("batch status update values len mismatch: got %d, want %d", len(values), 3)
				}
			case strings.Contains(sql, "SET `metadata` = ?, `updated_at` = ?, `status` = ?"):
				if len(values) != 4 {
					t.Fatalf("batch metadata update values len mismatch: got %d, want %d", len(values), 4)
				}
			case strings.Contains(sql, "SET `metadata` = ?, `updated_at` = ?, `user_id` = ?"):
				if len(values) != 6 {
					t.Fatalf("batch metadata and keys update values len mismatch: got %d, want %d", len(values), 6)
				}
			case strings.Contains(sql, "SET `job_end_timestamp` = ?, `metadata` = ?"):
				if len(values) != 8 {
					t.Fatalf("batch timestamp update values len mismatch: got %d, want %d", len(values), 8)
				}
			default:
				t.Fatalf("unexpected batch update sql: %s", sql)
			}

			// Validate metadata payload when present.
			for _, v := range values {
				b, ok := v.([]byte)
				if !ok {
					continue
				}
				if len(b) != w.batchMetaSize {
					t.Fatalf("batch metadata len mismatch: got %d, want %d", len(b), w.batchMetaSize)
				}
			}
		default:
			t.Fatalf("unexpected update sql: %s", sql)
		}
	}

	if !foundEntity {
		t.Fatalf("expected at least one entity update")
	}
	if !foundBatch {
		t.Fatalf("expected at least one batch update")
	}
}

func TestJSONZstdWorkloadZstdPayloadMode(t *testing.T) {
	w := NewJSONZstdWorkload(2048, 1, 0, 100, "zstd").(*JSONZstdWorkload)
	r := rand.New(rand.NewSource(3))

	_, entityValues := w.buildEntityInsertWithValues(0, 1, r)
	media, ok := entityValues[9].(string)
	if !ok {
		t.Fatalf("entity media_metadata type mismatch: %T", entityValues[9])
	}
	if len(media) != w.entityMediaSize {
		t.Fatalf("entity media_metadata len mismatch: got %d, want %d", len(media), w.entityMediaSize)
	}
	if !strings.Contains(media, `{"key":"`) {
		t.Fatalf("expected zstd payload signature in entity media_metadata")
	}

	_, batchValues := w.buildBatchInsertWithValues(0, 1, r)
	metaBytes, ok := batchValues[6].([]byte)
	if !ok {
		t.Fatalf("batch metadata type mismatch: %T", batchValues[6])
	}
	if len(metaBytes) != w.batchMetaSize {
		t.Fatalf("batch metadata len mismatch: got %d, want %d", len(metaBytes), w.batchMetaSize)
	}
	if !bytes.Contains(metaBytes, []byte(`{"key":"`)) {
		t.Fatalf("expected zstd payload signature in batch metadata")
	}
}
