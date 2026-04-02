package utils_test

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNextGenUpstreamTiKVWalSyncDirIsPerInstance(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to get test file path")
	}

	scriptPath := filepath.Join(filepath.Dir(currentFile), "start_tidb_cluster_nextgen")
	content, err := os.ReadFile(scriptPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", scriptPath, err)
	}

	script := string(content)

	sharedWalDir := `wal-sync-dir = "$TEST_DATA_DIR/wal-sync/upstream/tikv/raft-wal"`
	if strings.Contains(script, sharedWalDir) {
		t.Fatalf("upstream TiKV instances must not share rfengine wal-sync-dir: %q", sharedWalDir)
	}

	perInstanceWalDir := `wal-sync-dir = "$TEST_DATA_DIR/wal-sync/upstream/tikv$idx/raft-wal"`
	if !strings.Contains(script, perInstanceWalDir) {
		t.Fatalf("expected per-instance rfengine wal-sync-dir snippet: %q", perInstanceWalDir)
	}
}
