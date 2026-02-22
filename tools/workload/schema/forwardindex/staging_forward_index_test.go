package forwardindex

import (
	"encoding/binary"
	"strings"
	"testing"
)

func TestStagingForwardIndexWorkloadRowSizing512K(t *testing.T) {
	w := NewStagingForwardIndexWorkload(512*1024, 1000).(*StagingForwardIndexWorkload)

	if w.debugInfoSize != maxBlobSize {
		t.Fatalf("debugInfoSize mismatch: got %d, want %d", w.debugInfoSize, maxBlobSize)
	}
	if w.debugInfoHistorySize != maxBlobSize {
		t.Fatalf("debugInfoHistorySize mismatch: got %d, want %d", w.debugInfoHistorySize, maxBlobSize)
	}

	wantAdDocSize := 512*1024 - 2*maxBlobSize
	if w.adDocSize != wantAdDocSize {
		t.Fatalf("adDocSize mismatch: got %d, want %d", w.adDocSize, wantAdDocSize)
	}
}

func TestStagingForwardIndexWorkloadRowSizingClamped(t *testing.T) {
	maxRowSize := maxMediumBlobSize + 2*maxBlobSize
	w := NewStagingForwardIndexWorkload(maxRowSize+1, 1000).(*StagingForwardIndexWorkload)

	if w.debugInfoSize != maxBlobSize {
		t.Fatalf("debugInfoSize mismatch: got %d, want %d", w.debugInfoSize, maxBlobSize)
	}
	if w.debugInfoHistorySize != maxBlobSize {
		t.Fatalf("debugInfoHistorySize mismatch: got %d, want %d", w.debugInfoHistorySize, maxBlobSize)
	}
	if w.adDocSize != maxMediumBlobSize {
		t.Fatalf("adDocSize mismatch: got %d, want %d", w.adDocSize, maxMediumBlobSize)
	}
}

func TestStagingForwardIndexWorkloadBuildInsertSqlWithValues(t *testing.T) {
	w := NewStagingForwardIndexWorkload(512*1024, 1000).(*StagingForwardIndexWorkload)
	sql, values := w.BuildInsertSqlWithValues(0, 3)

	if !strings.Contains(sql, "INSERT INTO staging_forward_index") {
		t.Fatalf("unexpected insert sql: %s", sql)
	}
	if !strings.Contains(sql, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("missing upsert clause: %s", sql)
	}

	if len(values) != 3*4 {
		t.Fatalf("values len mismatch: got %d, want %d", len(values), 3*4)
	}

	debugInfo, ok := values[0].([]byte)
	if !ok {
		t.Fatalf("debugInfo type mismatch: %T", values[0])
	}
	if len(debugInfo) != w.debugInfoSize {
		t.Fatalf("debugInfo len mismatch: got %d, want %d", len(debugInfo), w.debugInfoSize)
	}

	debugInfoHistory, ok := values[1].([]byte)
	if !ok {
		t.Fatalf("debugInfoHistory type mismatch: %T", values[1])
	}
	if len(debugInfoHistory) != w.debugInfoHistorySize {
		t.Fatalf("debugInfoHistory len mismatch: got %d, want %d", len(debugInfoHistory), w.debugInfoHistorySize)
	}

	adDoc, ok := values[3].([]byte)
	if !ok {
		t.Fatalf("adDoc type mismatch: %T", values[3])
	}
	if len(adDoc) != w.adDocSize {
		t.Fatalf("adDoc len mismatch: got %d, want %d", len(adDoc), w.adDocSize)
	}

	id1, ok := values[2].([]byte)
	if !ok {
		t.Fatalf("id type mismatch: %T", values[2])
	}
	if len(id1) != pinPromotionIDSize {
		t.Fatalf("id len mismatch: got %d, want %d", len(id1), pinPromotionIDSize)
	}
	if seq := binary.BigEndian.Uint64(id1[pinPromotionIDSeed:]); seq != 1 {
		t.Fatalf("id seq mismatch: got %d, want %d", seq, 1)
	}
}
