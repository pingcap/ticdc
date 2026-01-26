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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
)

type tikvEncryptionHTTPClient struct {
	pdClient    pd.Client
	httpClient  *httputil.Client
	httpScheme  string
	httpTimeout time.Duration
}

func NewTiKVEncryptionHTTPClient(pdClient pd.Client, credential *security.Credential) (TiKVEncryptionClient, error) {
	httpClient, err := httputil.NewClient(credential)
	if err != nil {
		return nil, err
	}

	httpScheme := "http"
	if credential != nil && credential.IsTLSEnabled() {
		httpScheme = "https"
	}

	return &tikvEncryptionHTTPClient{
		pdClient:    pdClient,
		httpClient:  httpClient,
		httpScheme:  httpScheme,
		httpTimeout: 5 * time.Second,
	}, nil
}

func (c *tikvEncryptionHTTPClient) GetKeyspaceEncryptionMeta(ctx context.Context, keyspaceID uint32) (*KeyspaceEncryptionMeta, error) {
	stores, err := c.pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return nil, errors.Trace(err)
	}

	var lastErr error
	for _, store := range stores {
		if engine.IsTiFlash(store) {
			continue
		}

		statusAddr := store.GetStatusAddress()
		if statusAddr == "" {
			continue
		}

		meta, err := c.getEncryptionMetaFromStore(ctx, statusAddr, keyspaceID)
		if err == nil {
			return meta, nil
		}
		if cerrors.ErrEncryptionMetaNotFound.Equal(err) {
			lastErr = err
			continue
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = cerrors.ErrEncryptionMetaNotFound
	}
	return nil, lastErr
}

func (c *tikvEncryptionHTTPClient) getEncryptionMetaFromStore(ctx context.Context, statusAddr string, keyspaceID uint32) (*KeyspaceEncryptionMeta, error) {
	storeURL := c.buildStatusURL(statusAddr, keyspaceID)

	reqCtx, cancel := context.WithTimeout(ctx, c.httpTimeout)
	defer cancel()

	resp, err := c.httpClient.Get(reqCtx, storeURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, cerrors.ErrEncryptionMetaNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, body)
	}

	var metaResp encryptionMetaResponse
	if err := json.Unmarshal(body, &metaResp); err != nil {
		return nil, errors.Trace(err)
	}

	return metaResp.toKeyspaceEncryptionMeta()
}

func (c *tikvEncryptionHTTPClient) buildStatusURL(statusAddr string, keyspaceID uint32) string {
	if strings.Contains(statusAddr, "://") {
		return fmt.Sprintf("%s/encryption_meta?keyspace_id=%d", strings.TrimRight(statusAddr, "/"), keyspaceID)
	}
	return fmt.Sprintf("%s://%s/encryption_meta?keyspace_id=%d", c.httpScheme, statusAddr, keyspaceID)
}

type encryptionMetaResponse struct {
	KeyspaceID uint32                     `json:"keyspace_id"`
	Current    encryptionEpochResponse    `json:"current"`
	MasterKey  masterKeyResponse          `json:"master_key"`
	DataKeys   map[uint32]dataKeyResponse `json:"data_keys"`
	History    []encryptionEpochResponse  `json:"history"`
}

type encryptionEpochResponse struct {
	FileID    uint64 `json:"file_id"`
	DataKeyID uint32 `json:"data_key_id"`
	CreatedAt uint64 `json:"created_at"`
}

type masterKeyResponse struct {
	Vendor     string    `json:"vendor"`
	CMEKID     string    `json:"cmek_id"`
	Region     string    `json:"region"`
	Endpoint   string    `json:"endpoint"`
	Ciphertext ByteArray `json:"ciphertext"`
}

type dataKeyResponse struct {
	Ciphertext ByteArray `json:"ciphertext"`
}

func (r *encryptionMetaResponse) toKeyspaceEncryptionMeta() (*KeyspaceEncryptionMeta, error) {
	if r.Current.DataKeyID == 0 {
		return nil, cerrors.ErrEncryptionMetaNotFound
	}

	currentKeyID, err := encodeDataKeyID24BE(r.Current.DataKeyID)
	if err != nil {
		return nil, err
	}

	version := byte(r.Current.DataKeyID & 0xFF)
	if version == VersionUnencrypted {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("version must be non-zero")
	}

	dataKeyMap := make(map[string]*DataKey, len(r.DataKeys))
	for id, dk := range r.DataKeys {
		keyID, err := encodeDataKeyID24BE(id)
		if err != nil {
			return nil, err
		}
		dataKeyMap[keyID] = &DataKey{
			Ciphertext:          []byte(dk.Ciphertext),
			EncryptionAlgorithm: AES256CTR,
		}
	}

	if _, ok := dataKeyMap[currentKeyID]; !ok {
		return nil, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key not found")
	}

	return &KeyspaceEncryptionMeta{
		Enabled:          true,
		Version:          version,
		MasterKey:        r.MasterKey.toMasterKey(),
		CurrentDataKeyID: currentKeyID,
		DataKeyMap:       dataKeyMap,
	}, nil
}

func (r *masterKeyResponse) toMasterKey() *MasterKey {
	return &MasterKey{
		Vendor:     KMSVendor(r.Vendor),
		CMEKID:     r.CMEKID,
		Region:     r.Region,
		Endpoint:   r.Endpoint,
		Ciphertext: []byte(r.Ciphertext),
	}
}

func encodeDataKeyID24BE(id uint32) (string, error) {
	if id > 0xFFFFFF {
		return "", cerrors.ErrInvalidDataKeyID.GenWithStackByArgs("data key ID exceeds 24-bit range")
	}
	b := [3]byte{byte(id >> 16), byte(id >> 8), byte(id)}
	return string(b[:]), nil
}
