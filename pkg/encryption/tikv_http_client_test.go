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

package encryption

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
)

type mockTiKVMetaPDClient struct {
	pd.Client
	stores []*metapb.Store
}

func (m *mockTiKVMetaPDClient) GetAllStores(ctx context.Context, opts ...pdopt.GetStoreOption) ([]*metapb.Store, error) {
	return m.stores, nil
}

func TestTiKVEncryptionHTTPClientGetKeyspaceEncryptionMeta(t *testing.T) {
	t.Parallel()

	const keyspaceID = uint32(1)
	const dataKeyID = uint32(0x010203) // 24-bit big-endian -> [0x01 0x02 0x03]

	handler := http.NewServeMux()
	handler.HandleFunc("/encryption_meta", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		qs := r.URL.Query()
		if qs.Get("keyspace_id") != "1" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
  "keyspace_id": 1,
  "current": {"file_id": 1, "data_key_id": 66051, "created_at": 0},
  "master_key": {"vendor": "aws-kms", "cmek_id": "cmek-1", "region": "us-west-1", "endpoint": "", "ciphertext": [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]},
  "data_keys": {
    "66051": {"ciphertext": [31,30,29,28,27,26,25,24,23,22,21,20,19,18,17,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1,0]}
  },
  "history": []
}`))
	})

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	srvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	statusAddr := srvURL.Host

	pdCli := &mockTiKVMetaPDClient{
		stores: []*metapb.Store{
			{Id: 1, StatusAddress: statusAddr, Address: "unused"},
		},
	}

	client, err := NewTiKVEncryptionHTTPClient(pdCli, nil)
	require.NoError(t, err)

	meta, err := client.GetKeyspaceEncryptionMeta(context.Background(), keyspaceID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, keyspaceID, meta.KeyspaceId)
	require.NotNil(t, meta.Current)

	expectedKeyID := string([]byte{0x01, 0x02, 0x03})
	currentKeyID, err := encodeDataKeyID24BE(meta.Current.DataKeyId)
	require.NoError(t, err)
	require.Equal(t, expectedKeyID, currentKeyID)
	require.Equal(t, dataKeyID, meta.Current.DataKeyId)

	dk, ok := meta.DataKeys[dataKeyID]
	require.True(t, ok)
	require.Len(t, dk.Ciphertext, 32)
}

func TestTiKVEncryptionHTTPClientNotFoundReturnsErrEncryptionMetaNotFound(t *testing.T) {
	t.Parallel()

	handler := http.NewServeMux()
	handler.HandleFunc("/encryption_meta", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("not found"))
	})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	srvURL, err := url.Parse(srv.URL)
	require.NoError(t, err)
	statusAddr := srvURL.Host

	pdCli := &mockTiKVMetaPDClient{
		stores: []*metapb.Store{
			{Id: 1, StatusAddress: statusAddr},
		},
	}

	client, err := NewTiKVEncryptionHTTPClient(pdCli, nil)
	require.NoError(t, err)

	_, err = client.GetKeyspaceEncryptionMeta(context.Background(), 1)
	require.True(t, cerrors.ErrEncryptionMetaNotFound.Equal(err), "err=%v", err)
}

func TestByteArrayUnmarshalSupportsUint8Array(t *testing.T) {
	t.Parallel()

	var b ByteArray
	err := b.UnmarshalJSON([]byte(`[0, 1, 2, 255]`))
	require.NoError(t, err)
	require.Equal(t, []byte{0, 1, 2, 255}, []byte(b))

	var bad ByteArray
	err = bad.UnmarshalJSON([]byte(`[256]`))
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "out of range"))
}
