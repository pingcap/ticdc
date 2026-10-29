// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package bootstrap

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pingcap/ticdc/pkg/snapshot/protocol"
	"github.com/pingcap/ticdc/pkg/snapshot/store"
)

// Applied task pages are the durable permission to remove their local chunks.
// Repeating this sweep after restart closes the receipt-then-cleanup crash gap.
func cleanupAppliedChunks(root, job string, tasks store.Tasks) error {
	all, err := tasks.List()
	if err != nil {
		return err
	}
	for _, task := range all {
		if task.State != "APPLIED" {
			continue
		}
		var export protocol.Export
		if err := protocol.Read(task.Export, &export); err != nil {
			return err
		}
		if err := removeExportChunks(root, job, export); err != nil {
			return err
		}
	}
	return nil
}

func removeExportChunks(root, job string, export protocol.Export) error {
	if export.JobID != job {
		return protocol.Invalid("chunk cleanup job mismatch")
	}
	if len(export.Chunks) == 0 {
		return nil
	}
	allowed, err := filepath.EvalSymlinks(filepath.Join(root, "br", job, "attempts"))
	if err != nil {
		return protocol.Wrap(err)
	}
	for _, chunk := range export.Chunks {
		name := filepath.Base(chunk.URI)
		if !filepath.IsAbs(chunk.URI) || !strings.HasPrefix(name, "chunk-") || !strings.HasSuffix(name, ".skv") {
			return protocol.Invalid("invalid local chunk cleanup path")
		}
		parent, err := filepath.EvalSymlinks(filepath.Dir(chunk.URI))
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return protocol.Wrap(err)
		}
		rel, err := filepath.Rel(allowed, parent)
		if err != nil {
			return protocol.Wrap(err)
		}
		if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
			return protocol.Invalid("chunk cleanup path outside job attempts")
		}
		if err := os.Remove(chunk.URI); err != nil && !os.IsNotExist(err) {
			return protocol.Wrap(err)
		}
	}
	return nil
}
