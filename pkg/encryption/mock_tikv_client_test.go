// Copyright 2025 PingCAP, Inc.
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

package encryption

import (
	"context"
	"testing"

	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMockTiKVClientNotFound(t *testing.T) {
	cli := NewMockTiKVClient()
	meta, err := cli.GetKeyspaceEncryptionMeta(context.Background(), 999)
	require.Nil(t, meta)
	require.True(t, cerrors.ErrEncryptionMetaNotFound.Equal(err))
}

func TestMockTiKVClientEnabledKeyspace(t *testing.T) {
	cli := NewMockTiKVClient()
	meta, err := cli.GetKeyspaceEncryptionMeta(context.Background(), 1)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, uint32(1), meta.KeyspaceId)
	require.NotNil(t, meta.Current)
	require.NotZero(t, meta.Current.DataKeyId)
	require.NotEmpty(t, meta.DataKeys)
}
