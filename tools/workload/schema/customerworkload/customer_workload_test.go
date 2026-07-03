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

package customerworkload

import (
	"strings"
	"testing"

	"workload/schema"
)

func TestCustomerWorkloadBuildsStatementsForAllModels(t *testing.T) {
	t.Parallel()

	for _, model := range []string{"A", "B", "C", "D"} {
		model := model
		t.Run(model, func(t *testing.T) {
			t.Parallel()

			workload := NewCustomerWorkload(Options{
				Model:           model,
				TableCount:      4,
				TableStartIndex: 0,
				TotalRowCount:   10_000,
			}).(*CustomerWorkload)

			createSQL := workload.BuildCreateTableStatement(0)
			assertContains(t, createSQL, "CREATE TABLE IF NOT EXISTS")
			assertContains(t, createSQL, "payload_blob")

			insertSQL, insertValues := workload.BuildInsertSqlWithValues(0, 3)
			assertContains(t, insertSQL, "INSERT INTO")
			assertContains(t, insertSQL, "VALUES")
			if got, want := len(insertValues), 39; got != want {
				t.Fatalf("unexpected insert values len: got %d, want %d", got, want)
			}

			updateSQL, updateValues := workload.BuildUpdateSqlWithValues(schema.UpdateOption{TableIndex: 0, Batch: 2})
			assertContains(t, updateSQL, "UPDATE")
			if len(updateValues) == 0 {
				t.Fatalf("expected update values")
			}

			deleteSQL, deleteValues := workload.BuildDeleteSqlWithValues(schema.DeleteOption{TableIndex: 0, Batch: 2})
			assertContains(t, deleteSQL, "DELETE FROM")
			if len(deleteValues) == 0 {
				t.Fatalf("expected delete values")
			}

			assertSanitized(t, createSQL, insertSQL, updateSQL, deleteSQL)
		})
	}
}

func TestCustomerWorkloadCompositeDeleteUsesTupleKey(t *testing.T) {
	t.Parallel()

	workload := NewCustomerWorkload(Options{
		Model:         "D",
		TableCount:    1,
		TotalRowCount: 10_000,
	}).(*CustomerWorkload)

	deleteSQL, deleteValues := workload.BuildDeleteSqlWithValues(schema.DeleteOption{TableIndex: 0, Batch: 2})
	assertContains(t, deleteSQL, "(`entity_id`,`bucket_id`,`sequence_no`) IN")
	if got, want := len(deleteValues), 6; got != want {
		t.Fatalf("unexpected delete values len: got %d, want %d", got, want)
	}
}

func TestCustomerWorkloadModelATableMix(t *testing.T) {
	t.Parallel()

	workload := NewCustomerWorkload(Options{
		Model:      "A",
		TableCount: 27,
	}).(*CustomerWorkload)

	countByPrefix := make(map[string]int)
	for i := 0; i < 27; i++ {
		countByPrefix[workload.specForTable(i).prefix]++
	}

	expected := map[string]int{
		"catalog_large":     9,
		"catalog_blended":   9,
		"catalog_json":      7,
		"catalog_compact_a": 1,
		"catalog_compact_b": 1,
	}
	for prefix, want := range expected {
		if got := countByPrefix[prefix]; got != want {
			t.Fatalf("unexpected model A table mix for %s: got %d, want %d", prefix, got, want)
		}
	}
}

func TestCustomerWorkloadInitialSeqSeedsInsertContinuation(t *testing.T) {
	t.Parallel()

	workload := NewCustomerWorkload(Options{
		Model:         "A",
		TableCount:    1,
		KeyspaceSize:  100,
		InitialSeq:    100,
		TotalRowCount: 100,
	}).(*CustomerWorkload)

	_, values := workload.BuildInsertSqlWithValues(0, 1)
	entityID, ok := values[0].(uint64)
	if !ok {
		t.Fatalf("unexpected entity id type %T", values[0])
	}
	if entityID != 101 {
		t.Fatalf("expected insert to continue after initial seq, got entity id %d", entityID)
	}

	_, updateValues := workload.BuildUpdateSqlWithValues(schema.UpdateOption{TableIndex: 0, Batch: 1})
	updateEntityID, ok := updateValues[len(updateValues)-1].(uint64)
	if !ok {
		t.Fatalf("unexpected update entity id type %T", updateValues[len(updateValues)-1])
	}
	if updateEntityID == 0 || updateEntityID > 101 {
		t.Fatalf("expected update entity id inside seeded keyspace, got %d", updateEntityID)
	}
}

func TestCustomerWorkloadRandomizedInsertUsesPreparedKeyspace(t *testing.T) {
	t.Parallel()

	workload := NewCustomerWorkload(Options{
		Model:           "C",
		TableCount:      1,
		KeyspaceSize:    1000,
		InitialSeq:      1000,
		RandomizeInsert: true,
	}).(*CustomerWorkload)

	_, values := workload.BuildInsertSqlWithValues(0, 16)
	seen := make(map[uint64]struct{}, 16)
	for i := 0; i < len(values); i += 13 {
		entityID, ok := values[i].(uint64)
		if !ok {
			t.Fatalf("unexpected entity id type %T", values[i])
		}
		if entityID == 0 || entityID > 1000 {
			t.Fatalf("expected randomized insert entity id inside keyspace, got %d", entityID)
		}
		if _, ok := seen[entityID]; ok {
			t.Fatalf("expected unique entity id inside one batch, got duplicate %d", entityID)
		}
		seen[entityID] = struct{}{}
	}

	if workload.tableSeq[0].Load() != 1000 {
		t.Fatalf("randomized insert should not append table sequence, got %d", workload.tableSeq[0].Load())
	}
}

func TestCustomerWorkloadRandomizedInsertDefaultsToComputedKeyspace(t *testing.T) {
	t.Parallel()

	workload := NewCustomerWorkload(Options{
		Model:           "B",
		TableCount:      2,
		TotalRowCount:   200,
		RandomizeInsert: true,
	}).(*CustomerWorkload)

	_, values := workload.BuildInsertSqlWithValues(0, 4)
	for i := 0; i < len(values); i += 13 {
		entityID, ok := values[i].(uint64)
		if !ok {
			t.Fatalf("unexpected entity id type %T", values[i])
		}
		if entityID == 0 || entityID > 100 {
			t.Fatalf("expected entity id inside computed per-table keyspace, got %d", entityID)
		}
	}
}

func assertContains(t *testing.T, s string, expected string) {
	t.Helper()
	if !strings.Contains(s, expected) {
		t.Fatalf("expected %q to contain %q", s, expected)
	}
}

func assertSanitized(t *testing.T, statements ...string) {
	t.Helper()

	joined := strings.ToLower(strings.Join(statements, "\n"))
	if !strings.Contains(joined, "payload_") {
		t.Fatalf("expected anonymized payload columns: %s", joined)
	}
	if strings.Contains(joined, ".com") || strings.Contains(joined, "://") {
		t.Fatalf("statement should not contain external identifiers: %s", joined)
	}
}
