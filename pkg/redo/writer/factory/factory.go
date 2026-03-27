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

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/pingcap/ticdc/pkg/redo/writer"
	"github.com/pingcap/ticdc/pkg/redo/writer/blackhole"
	"github.com/pingcap/ticdc/pkg/redo/writer/file"
	"github.com/pingcap/ticdc/pkg/redo/writer/memory"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
)

<<<<<<< HEAD
// NewRedoLogWriter creates a new RedoLogWriter.
func NewRedoLogWriter(
	ctx context.Context, lwCfg *writer.LogWriterConfig, fileType string,
) (writer.RedoLogWriter, error) {
	uri, err := storage.ParseRawURL(util.GetOrZero(lwCfg.Storage))
	if err != nil {
		return nil, err
	}

	if !redo.IsValidConsistentStorage(uri.Scheme) {
		return nil, errors.ErrConsistentStorage.GenWithStackByArgs(uri.Scheme)
	}

	lwCfg.URI = uri
	lwCfg.UseExternalStorage = redo.IsExternalStorage(uri.Scheme)

=======
// NewRedoDMLWriter creates a new RedoDMLWriter.
func NewRedoDMLWriter(
	ctx context.Context, cfg *writer.Config,
) (writer.RedoDMLWriter, error) {
	uri := cfg.URI()
>>>>>>> 7b6e554bb (redo: split the redo writer interface to ddl writer and dml writer (#4580))
	if redo.IsBlackholeStorage(uri.Scheme) {
		invalid := strings.HasSuffix(uri.Scheme, "invalid")
		return blackhole.NewDMLWriter(invalid), nil
	}

<<<<<<< HEAD
	if util.GetOrZero(lwCfg.UseFileBackend) {
		return file.NewLogWriter(ctx, lwCfg, fileType)
	}
	return memory.NewLogWriter(ctx, lwCfg, fileType)
=======
	if cfg.UseFileBackend() {
		return file.NewDMLWriter(ctx, cfg)
	}
	return memory.NewDMLWriter(ctx, cfg)
}

// NewRedoDDLWriter creates a new RedoDDLWriter.
func NewRedoDDLWriter(
	ctx context.Context, cfg *writer.Config,
) (writer.RedoDDLWriter, error) {
	uri := cfg.URI()
	if redo.IsBlackholeStorage(uri.Scheme) {
		invalid := strings.HasSuffix(uri.Scheme, "invalid")
		return blackhole.NewDDLWriter(invalid), nil
	}

	if cfg.UseFileBackend() {
		return file.NewDDLWriter(ctx, cfg)
	}
	return memory.NewDDLWriter(ctx, cfg)
>>>>>>> 7b6e554bb (redo: split the redo writer interface to ddl writer and dml writer (#4580))
}
