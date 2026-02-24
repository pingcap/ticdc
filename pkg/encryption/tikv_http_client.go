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
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/tidb/pkg/util/engine"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"go.uber.org/zap"
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

func (c *tikvEncryptionHTTPClient) GetKeyspaceEncryptionMeta(ctx context.Context, keyspaceID uint32) (*EncryptionMeta, error) {
	stores, err := c.pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		log.Warn("failed to list TiKV stores",
			zap.Uint32("keyspaceID", keyspaceID),
			zap.Error(err))
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

		meta, err := c.getEncryptionMetaFromStore(ctx, store.GetId(), statusAddr, keyspaceID)
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

func (c *tikvEncryptionHTTPClient) getEncryptionMetaFromStore(ctx context.Context, storeID uint64, statusAddr string, keyspaceID uint32) (*EncryptionMeta, error) {
	storeURL := c.buildStatusURL(statusAddr, keyspaceID)

	reqCtx, cancel := context.WithTimeout(ctx, c.httpTimeout)
	defer cancel()

	resp, err := c.httpClient.Get(reqCtx, storeURL)
	if err != nil {
		log.Warn("failed to fetch encryption meta from TiKV store",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.Error(err))
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Warn("failed to read encryption meta response body",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	if resp.StatusCode == http.StatusNotFound {
		log.Debug("encryption meta not found on TiKV store",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL))
		return nil, cerrors.ErrEncryptionMetaNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Warn("unexpected encryption meta response status",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.Int("statusCode", resp.StatusCode),
			zap.Int("bodySize", len(body)),
			zap.String("body", truncateBytesForLog(body, 256)))
		return nil, errors.Errorf("[%d] %s", resp.StatusCode, body)
	}

	var metaResp encryptionMetaResponse
	if err := json.Unmarshal(body, &metaResp); err != nil {
		log.Warn("failed to decode encryption meta response",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.Int("bodySize", len(body)),
			zap.String("body", truncateBytesForLog(body, 256)),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	meta, err := metaResp.toEncryptionMeta()
	if err != nil {
		log.Warn("failed to convert encryption meta response",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.Error(err))
		return nil, err
	}
	return meta, nil
}

func (c *tikvEncryptionHTTPClient) buildStatusURL(statusAddr string, keyspaceID uint32) string {
	if strings.Contains(statusAddr, "://") {
		return fmt.Sprintf("%s/encryption_meta?keyspace_id=%d", strings.TrimRight(statusAddr, "/"), keyspaceID)
	}
	return fmt.Sprintf("%s://%s/encryption_meta?keyspace_id=%d", c.httpScheme, statusAddr, keyspaceID)
}

type encryptionMetaResponse struct {
	KeyspaceId uint32                     `json:"keyspace_id"`
	Current    encryptionEpochResponse    `json:"current"`
	MasterKey  masterKeyResponse          `json:"master_key"`
	DataKeys   map[uint32]dataKeyResponse `json:"data_keys"`
	History    []encryptionEpochResponse  `json:"history"`
}

type encryptionEpochResponse struct {
	FileId    uint64 `json:"file_id"`
	DataKeyId uint32 `json:"data_key_id"`
	CreatedAt uint64 `json:"created_at"`
}

type masterKeyResponse struct {
	Vendor     string    `json:"vendor"`
	CmekId     string    `json:"cmek_id"`
	Region     string    `json:"region"`
	Endpoint   string    `json:"endpoint"`
	Ciphertext ByteArray `json:"ciphertext"`
}

type dataKeyResponse struct {
	Ciphertext ByteArray `json:"ciphertext"`
}

func (r *encryptionMetaResponse) toEncryptionMeta() (*EncryptionMeta, error) {
	if r.Current.DataKeyId == 0 {
		return nil, cerrors.ErrEncryptionMetaNotFound
	}

	version := byte(r.Current.DataKeyId & 0xFF)
	if version == VersionUnencrypted {
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("version must be non-zero")
	}

	dataKeys := make(map[uint32]*DataKey, len(r.DataKeys))
	for id, dk := range r.DataKeys {
		dataKeys[id] = &DataKey{Ciphertext: []byte(dk.Ciphertext)}
	}

	if _, ok := dataKeys[r.Current.DataKeyId]; !ok {
		return nil, cerrors.ErrDataKeyNotFound.GenWithStackByArgs("current data key not found")
	}

	history := make([]*EncryptionEpoch, 0, len(r.History))
	for _, epoch := range r.History {
		history = append(history, &EncryptionEpoch{
			FileId:    epoch.FileId,
			DataKeyId: epoch.DataKeyId,
			CreatedAt: epoch.CreatedAt,
		})
	}

	return &EncryptionMeta{
		KeyspaceId: r.KeyspaceId,
		Current: &EncryptionEpoch{
			FileId:    r.Current.FileId,
			DataKeyId: r.Current.DataKeyId,
			CreatedAt: r.Current.CreatedAt,
		},
		MasterKey: r.MasterKey.toMasterKey(),
		DataKeys:  dataKeys,
		History:   history,
	}, nil
}

func (r *masterKeyResponse) toMasterKey() *MasterKey {
	return &MasterKey{
		Vendor:     r.Vendor,
		CmekId:     r.CmekId,
		Region:     r.Region,
		Endpoint:   r.Endpoint,
		Ciphertext: []byte(r.Ciphertext),
	}
}

func truncateBytesForLog(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return fmt.Sprintf("%s...(truncated, %d bytes total)", string(b[:max]), len(b))
}
