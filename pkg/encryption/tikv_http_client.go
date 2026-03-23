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

	oldproto "github.com/gogo/protobuf/proto"
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
	contentType := ""

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
	contentType = resp.Header.Get("Content-Type")

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

	metaResp := &encryptionMetaResponse{}
	responseFormat := "json"
	if jsonErr := json.Unmarshal(body, metaResp); jsonErr != nil {
		log.Debug("failed to decode encryption meta response as json, fallback to protobuf",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("keyspaceID", keyspaceID),
			zap.String("url", storeURL),
			zap.String("contentType", contentType),
			zap.Int("bodySize", len(body)),
			zap.Error(jsonErr))

		metaResp, err = decodeEncryptionMetaResponseFromProtobuf(body)
		if err != nil {
			decodeErr := errors.Annotatef(err, "json decode failed: %v", jsonErr)
			log.Warn("failed to decode encryption meta response",
				zap.Uint64("storeID", storeID),
				zap.String("statusAddr", statusAddr),
				zap.Uint32("keyspaceID", keyspaceID),
				zap.String("url", storeURL),
				zap.String("contentType", contentType),
				zap.Int("bodySize", len(body)),
				zap.String("body", truncateBytesForLog(body, 256)),
				zap.Error(decodeErr))
			return nil, errors.Trace(decodeErr)
		}
		responseFormat = "protobuf"
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

	if meta.KeyspaceId != 0 && meta.KeyspaceId != keyspaceID {
		log.Warn("encryption meta keyspace ID mismatch",
			zap.Uint64("storeID", storeID),
			zap.String("statusAddr", statusAddr),
			zap.Uint32("requestedKeyspaceID", keyspaceID),
			zap.Uint32("metaKeyspaceID", meta.KeyspaceId),
			zap.String("url", storeURL))
	}

	masterKeyCiphertextLen := 0
	if meta.MasterKey != nil {
		masterKeyCiphertextLen = len(meta.MasterKey.Ciphertext)
	}

	log.Info("fetched valid encryption meta from TiKV store",
		zap.Uint64("storeID", storeID),
		zap.String("statusAddr", statusAddr),
		zap.String("responseFormat", responseFormat),
		zap.String("contentType", contentType),
		zap.Uint32("requestedKeyspaceID", keyspaceID),
		zap.Uint32("metaKeyspaceID", meta.KeyspaceId),
		zap.Uint32("currentDataKeyID", meta.Current.DataKeyId),
		zap.Uint8("version", byte(meta.Current.DataKeyId&0xFF)),
		zap.Int("dataKeyCount", len(meta.DataKeys)),
		zap.Int("historyCount", len(meta.History)),
		zap.String("kmsVendor", safeKMSVendor(meta.MasterKey)),
		zap.String("cmekID", safeCMEKID(meta.MasterKey)),
		zap.Int("masterKeyCiphertextLen", masterKeyCiphertextLen))

	return meta, nil
}

func (c *tikvEncryptionHTTPClient) buildStatusURL(statusAddr string, keyspaceID uint32) string {
	if strings.Contains(statusAddr, "://") {
		return fmt.Sprintf("%s/encryption/get-meta?keyspace_id=%d", strings.TrimRight(statusAddr, "/"), keyspaceID)
	}
	return fmt.Sprintf("%s://%s/encryption/get-meta?keyspace_id=%d", c.httpScheme, statusAddr, keyspaceID)
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

type keyspaceEncryptionMetaPB struct {
	KeyspaceId uint32                        `protobuf:"varint,1,opt,name=keyspace_id,json=keyspaceId,proto3"`
	Current    *keyspaceEncryptionEpochPB    `protobuf:"bytes,2,opt,name=current,proto3"`
	MasterKey  *keyspaceMasterKeyPB          `protobuf:"bytes,3,opt,name=master_key,json=masterKey,proto3"`
	DataKeys   map[uint32]*keyspaceDataKeyPB `protobuf:"bytes,4,rep,name=data_keys,json=dataKeys,proto3" protobuf_key:"varint,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	History    []*keyspaceEncryptionEpochPB  `protobuf:"bytes,5,rep,name=history,proto3"`
}

func (m *keyspaceEncryptionMetaPB) Reset()         { *m = keyspaceEncryptionMetaPB{} }
func (m *keyspaceEncryptionMetaPB) String() string { return "" }
func (*keyspaceEncryptionMetaPB) ProtoMessage()    {}

type keyspaceEncryptionEpochPB struct {
	FileId    uint64 `protobuf:"varint,1,opt,name=file_id,json=fileId,proto3"`
	DataKeyId uint32 `protobuf:"varint,2,opt,name=data_key_id,json=dataKeyId,proto3"`
	CreatedAt uint64 `protobuf:"varint,3,opt,name=created_at,json=createdAt,proto3"`
}

func (m *keyspaceEncryptionEpochPB) Reset()         { *m = keyspaceEncryptionEpochPB{} }
func (m *keyspaceEncryptionEpochPB) String() string { return "" }
func (*keyspaceEncryptionEpochPB) ProtoMessage()    {}

type keyspaceMasterKeyPB struct {
	Vendor     string `protobuf:"bytes,1,opt,name=vendor,proto3"`
	CmekId     string `protobuf:"bytes,2,opt,name=cmek_id,json=cmekId,proto3"`
	Region     string `protobuf:"bytes,3,opt,name=region,proto3"`
	Endpoint   string `protobuf:"bytes,4,opt,name=endpoint,proto3"`
	Ciphertext []byte `protobuf:"bytes,5,opt,name=ciphertext,proto3"`
}

func (m *keyspaceMasterKeyPB) Reset()         { *m = keyspaceMasterKeyPB{} }
func (m *keyspaceMasterKeyPB) String() string { return "" }
func (*keyspaceMasterKeyPB) ProtoMessage()    {}

type keyspaceDataKeyPB struct {
	Ciphertext []byte `protobuf:"bytes,1,opt,name=ciphertext,proto3"`
}

func (m *keyspaceDataKeyPB) Reset()         { *m = keyspaceDataKeyPB{} }
func (m *keyspaceDataKeyPB) String() string { return "" }
func (*keyspaceDataKeyPB) ProtoMessage()    {}

func decodeEncryptionMetaResponseFromProtobuf(body []byte) (*encryptionMetaResponse, error) {
	metaPB := &keyspaceEncryptionMetaPB{}
	if err := oldproto.Unmarshal(body, metaPB); err != nil {
		return nil, errors.Trace(err)
	}
	if metaPB.Current == nil && metaPB.MasterKey == nil && len(metaPB.DataKeys) == 0 && len(metaPB.History) == 0 && metaPB.KeyspaceId == 0 {
		return nil, errors.New("protobuf payload does not contain encryption meta fields")
	}
	return metaPB.toEncryptionMetaResponse(), nil
}

func (m *keyspaceEncryptionMetaPB) toEncryptionMetaResponse() *encryptionMetaResponse {
	resp := &encryptionMetaResponse{
		KeyspaceId: m.KeyspaceId,
		DataKeys:   make(map[uint32]dataKeyResponse, len(m.DataKeys)),
		History:    make([]encryptionEpochResponse, 0, len(m.History)),
	}

	if m.Current != nil {
		resp.Current = encryptionEpochResponse{
			FileId:    m.Current.FileId,
			DataKeyId: m.Current.DataKeyId,
			CreatedAt: m.Current.CreatedAt,
		}
	}

	if m.MasterKey != nil {
		resp.MasterKey = masterKeyResponse{
			Vendor:     m.MasterKey.Vendor,
			CmekId:     m.MasterKey.CmekId,
			Region:     m.MasterKey.Region,
			Endpoint:   m.MasterKey.Endpoint,
			Ciphertext: ByteArray(m.MasterKey.Ciphertext),
		}
	}

	for id, dataKey := range m.DataKeys {
		if dataKey == nil {
			continue
		}
		resp.DataKeys[id] = dataKeyResponse{
			Ciphertext: ByteArray(dataKey.Ciphertext),
		}
	}

	for _, epoch := range m.History {
		if epoch == nil {
			continue
		}
		resp.History = append(resp.History, encryptionEpochResponse{
			FileId:    epoch.FileId,
			DataKeyId: epoch.DataKeyId,
			CreatedAt: epoch.CreatedAt,
		})
	}

	return resp
}

func (r *encryptionMetaResponse) toEncryptionMeta() (*EncryptionMeta, error) {
	if r.Current.DataKeyId == 0 {
		log.Warn("invalid encryption meta from TiKV: current data key ID is empty",
			zap.Uint32("metaKeyspaceID", r.KeyspaceId))
		return nil, cerrors.ErrEncryptionMetaNotFound
	}

	version := byte(r.Current.DataKeyId & 0xFF)
	if version == VersionUnencrypted {
		log.Warn("invalid encryption meta from TiKV: version must be non-zero",
			zap.Uint32("metaKeyspaceID", r.KeyspaceId),
			zap.Uint32("currentDataKeyID", r.Current.DataKeyId))
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("version must be non-zero")
	}

	dataKeys := make(map[uint32]*DataKey, len(r.DataKeys))
	for id, dk := range r.DataKeys {
		dataKeys[id] = &DataKey{Ciphertext: []byte(dk.Ciphertext)}
	}

	if _, ok := dataKeys[r.Current.DataKeyId]; !ok {
		log.Warn("invalid encryption meta from TiKV: current data key missing",
			zap.Uint32("metaKeyspaceID", r.KeyspaceId),
			zap.Uint32("currentDataKeyID", r.Current.DataKeyId),
			zap.Int("dataKeyCount", len(dataKeys)))
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
