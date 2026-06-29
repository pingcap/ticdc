package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanLogsForPatternsIgnoresPayloadSubstrings(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`
[2026/06/16 13:31:34.281 +08:00] [DEBUG] [basic_dispatcher.go:600] ["dispatcher receive all event"] [event="Rows: Insert: Row: 1, Bb8bdTFTEIN9i3spwifGjZj3AmFAtalR;"]
[2026/06/16 13:36:34.814 +08:00] [DEBUG] [basic_dispatcher.go:600] ["dispatcher receive all event"] [event="Rows: Insert: Row: 2, 1YCs3x0WFrKYaheC3jpXpAnicxBqG3pe;"]
`)
	if err := os.WriteFile(filepath.Join(dir, "cdc.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"panic", "fatal", "DATA RACE"}, true, nil); err != nil {
		t.Fatalf("scanLogsForPatterns() unexpected error = %v", err)
	}
}

func TestScanLogsForPatternsDetectsFatalLogLevel(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`[2026/06/16 13:31:34.281 +08:00] [FATAL] [server.go:1] ["failed"]`)
	if err := os.WriteFile(filepath.Join(dir, "cdc.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"fatal"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected fatal log level match")
	}
}

func TestScanLogsForPatternsDetectsPanicLogLevel(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`[2026/06/16 13:31:34.281 +08:00] [PANIC] [server.go:1] ["failed"]`)
	if err := os.WriteFile(filepath.Join(dir, "cdc.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"panic"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected panic log level match")
	}
}

func TestScanLogsForPatternsDetectsPanicPrefix(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`panic: runtime error: invalid memory address`)
	if err := os.WriteFile(filepath.Join(dir, "stdout.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"panic"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected panic prefix match")
	}
}

func TestScanLogsForPatternsDetectsFatalErrorPrefix(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`fatal error: concurrent map writes`)
	if err := os.WriteFile(filepath.Join(dir, "stdout.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"fatal"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected fatal error prefix match")
	}
}

func TestScanLogsForPatternsDetectsDataRaceWarning(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`WARNING: DATA RACE`)
	if err := os.WriteFile(filepath.Join(dir, "stdout.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"DATA RACE"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected data race warning match")
	}
}

func TestScanLogsForPatternsKeepsCustomSubstringMatch(t *testing.T) {
	dir := t.TempDir()
	content := []byte(`[INFO] custom marker appeared`)
	if err := os.WriteFile(filepath.Join(dir, "cdc.log"), content, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := scanLogsForPatterns(dir, []string{"custom marker"}, true, nil); err == nil {
		t.Fatalf("scanLogsForPatterns() expected custom substring match")
	}
}
