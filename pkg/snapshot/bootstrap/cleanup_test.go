// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/stretchr/testify/require"
)

func TestRemoveAppliedChunksOnlyInsideJobAttempts(t *testing.T) {
	root := t.TempDir()
	attempt := filepath.Join(root, "br", "job", "attempts", "a1")
	require.NoError(t, os.MkdirAll(attempt, 0o700))
	chunk := filepath.Join(attempt, "chunk-000000.skv")
	require.NoError(t, os.WriteFile(chunk, []byte("data"), 0o600))
	export := protocol.Export{JobID: "job", Chunks: []protocol.Chunk{{ObjectRef: protocol.ObjectRef{URI: chunk}}}}
	require.NoError(t, removeExportChunks(root, "job", export))
	require.NoFileExists(t, chunk)
	require.NoError(t, removeExportChunks(root, "job", export))

	outside := filepath.Join(t.TempDir(), "chunk-000000.skv")
	require.NoError(t, os.WriteFile(outside, []byte("keep"), 0o600))
	export.Chunks[0].URI = outside
	require.ErrorContains(t, removeExportChunks(root, "job", export), "outside job attempts")
	require.FileExists(t, outside)
}
