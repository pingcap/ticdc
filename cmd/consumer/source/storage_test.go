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

package source

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

// TestStorageReaderReportsScanCheckpoint covers the barrier that integration
// tests rely on: a scan without new files reports the metadata checkpoint, so a
// later read proves every file below it has been dispatched.
func TestStorageReaderReportsScanCheckpoint(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "metadata"), []byte(`{"checkpoint-ts":100}`), 0o644))

	upstreamURI, err := url.Parse("file://" + dir)
	require.NoError(t, err)
	reader := newStorageReader(t.Context(), Options{
		UpstreamURI:   upstreamURI,
		ReplicaConfig: config.GetDefaultReplicaConfig(),
		CodecConfig:   common.NewConfig(config.ProtocolCanalJSON),
	})

	payloads, err := reader.Read(t.Context())
	require.NoError(t, err)
	require.Len(t, payloads, 1)
	require.Equal(t, uint64(100), payloads[0].Watermark)
	require.Equal(t, uint64(100), reader.watermark)
}
