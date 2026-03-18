// Copyright 2023 PingCAP, Inc.
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

package factory

import (
	"context"
	"strings"

	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/blackhole"
	"github.com/pingcap/ticdc/pkg/redo/writer/file"
	"github.com/pingcap/ticdc/pkg/redo/writer/memory"
)

// NewRedoLogWriter creates a new RedoLogWriter.
func NewRedoLogWriter(
	ctx context.Context, cfg *writer.Config, fileType string,
) (writer.RedoLogWriter, error) {
	uri := cfg.URI()
	if redo.IsBlackholeStorage(uri.Scheme) {
		invalid := strings.HasSuffix(uri.Scheme, "invalid")
		return blackhole.NewLogWriter(invalid), nil
	}

	if cfg.UseFileBackend() {
		return file.NewLogWriter(ctx, cfg, fileType)
	}
	return memory.NewLogWriter(ctx, cfg, fileType)
}
