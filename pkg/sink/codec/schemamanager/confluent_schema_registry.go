// Copyright 2020 PingCAP, Inc.
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

package schemamanager

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/httputil"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

// confluent avro wire format, the first byte is always 0
// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
const (
	ConfluentMagicByte           = uint8(0)
	schemaRegistryRequestTimeout = 30 * time.Second
)

// confluentSchemaManager is used to register Avro Schemas to the confluent Registry server,
// look up local cache according to the table's name, and fetch from the Registry
// in cache the local cache entry is missing.
type confluentSchemaManager struct {
	registryURL string

	credential *security.Credential // placeholder, currently always nil

	cache        *schemaCache
	registryType string
}

type registerRequest struct {
	Schema string `json:"schema"`
	// Commented out for compatibility with Confluent 5.4.x
	// SchemaType string `json:"schemaType"`
}

type registerResponse struct {
	SchemaID int `json:"id"`
}

type lookupResponse struct {
	Name     string `json:"name"`
	SchemaID int    `json:"id"`
	Schema   string `json:"schema"`
}

// NewConfluentSchemaManager create schema managers,
// and test connectivity to the schema registry
func NewConfluentSchemaManager(
	ctx context.Context,
	registryURL string,
	credential *security.Credential,
) (SchemaManager, error) {
	registryURL = strings.TrimRight(registryURL, "/")
	httpCli, err := httputil.NewClient(credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	httpCli.SetTimeout(schemaRegistryRequestTimeout)
	ctx, cancel := context.WithTimeout(ctx, schemaRegistryRequestTimeout)
	defer cancel()
	resp, err := httpCli.Get(ctx, registryURL)
	if err != nil {
		err = util.MaskSensitiveDataInURLError(err)
		log.Error("Test connection to Schema Registry failed", zap.Error(err))
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	defer resp.Body.Close()

	text, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Reading response from Schema Registry failed", zap.Error(err))
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	if string(text[:]) != "{}" {
		log.Error("Unexpected response from Schema Registry", zap.ByteString("response", text))
		return nil, errors.ErrAvroSchemaAPIError.GenWithStack(
			"Unexpected response from Schema Registry",
		)
	}

	log.Info("Successfully tested connectivity to Schema Registry")

	return &confluentSchemaManager{
		registryURL:  registryURL,
		cache:        newSchemaCache(),
		registryType: common.SchemaRegistryTypeConfluent,
	}, nil
}

// Register a schema in schema registry, no cache
func (m *confluentSchemaManager) Register(
	ctx context.Context,
	schemaName string,
	schemaDefinition string,
) (SchemaID, error) {
	id := SchemaID{}
	log.Info("confluentSchemaManager", zap.String("schemaDefinition", schemaDefinition), zap.String("schemaName", schemaName))

	reqBody := registerRequest{
		Schema: schemaDefinition,
	}
	payload, err := json.Marshal(&reqBody)
	if err != nil {
		log.Error("Could not marshal request to the Registry", zap.Error(err))
		return id, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	uri := m.registryURL + "/subjects/" + url.QueryEscape(schemaName) + "/versions"
	log.Info("Registering schema", zap.ByteString("payload", payload))

	req, err := http.NewRequestWithContext(ctx, "POST", uri, bytes.NewReader(payload))
	if err != nil {
		err = util.MaskSensitiveDataInURLError(err)
		log.Error("Failed to NewRequestWithContext", zap.Error(err))
		return id, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)
	req.Header.Add("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return id, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to read response from Registry", zap.Error(err))
		return id, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	if resp.StatusCode != 200 {
		// https://docs.confluent.io/platform/current/schema-registry/develop/api.html \
		// #post--subjects-(string-%20subject)-versions
		// 409 for incompatible schema
		log.Error(
			"Failed to register schema to the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.ByteString("requestBody", payload),
			zap.ByteString("responseBody", body),
		)
		return id, errors.ErrAvroSchemaAPIError.GenWithStackByArgs(
			"register schema failed with status " + strconv.Itoa(resp.StatusCode),
		)
	}

	var jsonResp registerResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return id, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	if jsonResp.SchemaID == 0 {
		return id, errors.ErrAvroSchemaAPIError.GenWithStack(
			"Illegal schema ID returned from Registry %d",
			jsonResp.SchemaID,
		)
	}

	log.Info("Registered schema successfully",
		zap.Int("schemaID", jsonResp.SchemaID),
		zap.ByteString("body", body))

	id.confluentSchemaID = jsonResp.SchemaID
	return id, nil
}

// Lookup the cached schema entry first, if not found, fetch from the Registry server.
func (m *confluentSchemaManager) Lookup(
	ctx context.Context,
	schemaName string,
	schemaID SchemaID,
) (string, error) {
	cacheKey := newDecodeCacheKey(schemaName)
	entry, exists := m.cache.load(cacheKey)
	if exists && entry.schemaID.confluentSchemaID == schemaID.confluentSchemaID {
		return entry.schemaDefinition, nil
	}

	uri := m.registryURL + "/schemas/ids/" + strconv.Itoa(schemaID.confluentSchemaID)

	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		err = util.MaskSensitiveDataInURLError(err)
		log.Error("Error constructing request for Registry lookup", zap.Error(err))
		return "", errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)

	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return "", errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		log.Error("Failed to query schema from the Registry, HTTP error",
			zap.Int("status", resp.StatusCode),
			zap.ByteString("responseBody", body))
		return "", errors.ErrAvroSchemaAPIError.GenWithStack(
			"Failed to query schema from the Registry, HTTP error",
		)
	}

	if resp.StatusCode == 404 {
		log.Warn("Specified schema not found in Registry",
			zap.String("key", schemaName),
			zap.Int("schemaID", schemaID.confluentSchemaID))
		return "", errors.ErrAvroSchemaAPIError.GenWithStackByArgs(
			"Schema not found in Registry",
		)
	}

	var jsonResp lookupResponse
	err = json.Unmarshal(body, &jsonResp)
	if err != nil {
		log.Error("Failed to parse result from Registry", zap.Error(err))
		return "", errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}

	cacheEntry := new(schemaCacheEntry)
	cacheEntry.schemaDefinition = jsonResp.Schema
	cacheEntry.schemaID.confluentSchemaID = schemaID.confluentSchemaID
	cacheEntry.header, err = BuildConfluentWireHeader(schemaID.confluentSchemaID)
	if err != nil {
		return "", err
	}

	m.cache.store(cacheKey, cacheEntry)
	return cacheEntry.schemaDefinition, nil
}

// GetCachedOrRegister checks if the suitable Avro schema has been cached.
// If not, a new schema is generated, registered and cached.
// Re-registering an existing schema shall return the same id(and version), so even if the
// cache is out-of-sync with schema registry, we could reload it.
func (m *confluentSchemaManager) GetCachedOrRegister(
	ctx context.Context,
	schemaSubject string,
	schemaIdentity string,
	schemaVersion uint64,
	schemaDefinition string,
) ([]byte, error) {
	entry, cached, err := m.cache.getOrCreate(
		schemaSubject, schemaIdentity, schemaVersion,
		func() (*schemaCacheEntry, error) {
			log.Info("Schema lookup cache miss",
				zap.String("key", schemaSubject),
				zap.String("schemaIdentity", schemaIdentity),
				zap.Uint64("schemaVersion", schemaVersion))

			id, err := m.Register(ctx, schemaSubject, schemaDefinition)
			if err != nil {
				log.Error("GetCachedOrRegister: Could not register schema", zap.Error(err))
				return nil, errors.Trace(err)
			}

			header, err := BuildConfluentWireHeader(id.confluentSchemaID)
			if err != nil {
				return nil, err
			}

			return &schemaCacheEntry{
				schemaVersion:    schemaVersion,
				schemaID:         id,
				schemaDefinition: schemaDefinition,
				header:           header,
			}, nil
		},
	)
	if err != nil {
		return nil, err
	}

	if cached {
		log.Debug("Avro schema GetCachedOrRegister cache hit",
			zap.String("key", schemaSubject),
			zap.String("schemaIdentity", schemaIdentity),
			zap.Uint64("schemaVersion", schemaVersion),
			zap.Int("schemaID", entry.schemaID.confluentSchemaID))
	} else {
		log.Info("Avro schema GetCachedOrRegister successful with cache miss",
			zap.Uint64("schemaVersion", entry.schemaVersion),
			zap.Int("schemaID", entry.schemaID.confluentSchemaID),
			zap.String("schema", entry.schemaDefinition))
	}

	return entry.header, nil
}

// ClearRegistry clears the Registry subject for the given table. Should be idempotent.
// Exported for testing.
// NOT USED for now, reserved for future use.
func (m *confluentSchemaManager) ClearRegistry(ctx context.Context, schemaSubject string) error {
	uri := m.registryURL + "/subjects/" + url.QueryEscape(schemaSubject)
	req, err := http.NewRequestWithContext(ctx, "DELETE", uri, nil)
	if err != nil {
		err = util.MaskSensitiveDataInURLError(err)
		log.Error("Could not construct request for clearRegistry", zap.Error(err))
		return errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	req.Header.Add(
		"Accept",
		"application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, "+
			"application/json",
	)
	resp, err := httpRetry(ctx, m.credential, req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == 200 {
		log.Info("Clearing Registry successful")
		return nil
	}

	if resp.StatusCode == 404 {
		log.Info("Registry already cleaned")
		return nil
	}

	log.Error("Error when clearing Registry", zap.Int("status", resp.StatusCode))
	return errors.ErrAvroSchemaAPIError.GenWithStack(
		"Error when clearing Registry, status = %d",
		resp.StatusCode,
	)
}

func (m *confluentSchemaManager) RegistryType() string {
	return m.registryType
}

// BuildConfluentWireHeader builds a Confluent Avro wire header.
func BuildConfluentWireHeader(schemaID int) ([]byte, error) {
	head := new(bytes.Buffer)
	err := head.WriteByte(ConfluentMagicByte)
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	err = binary.Write(head, binary.BigEndian, int32(schemaID))
	if err != nil {
		return nil, errors.WrapError(errors.ErrEncodeFailed, err)
	}
	return head.Bytes(), nil
}

func httpRetry(
	ctx context.Context,
	credential *security.Credential,
	r *http.Request,
) (*http.Response, error) {
	var (
		err  error
		resp *http.Response
		data []byte
	)

	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.MaxInterval = time.Second * 30
	expBackoff.MaxElapsedTime = schemaRegistryRequestTimeout
	httpCli, err := httputil.NewClient(credential)
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	httpCli.SetTimeout(schemaRegistryRequestTimeout)

	if r.Body != nil {
		data, err = io.ReadAll(r.Body)
		_ = r.Body.Close()
	}

	if err != nil {
		log.Error("Failed to parse response", zap.Error(err))
		return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
	}
	for {
		if data != nil {
			r.Body = io.NopCloser(bytes.NewReader(data))
		}
		resp, err = httpCli.Do(r)
		if err != nil {
			err = util.MaskSensitiveDataInURLError(err)
			log.Warn("HTTP request failed", zap.Error(err))
			goto checkCtx
		}

		// Return HTTP responses to callers so they can report registry status
		// codes. Retrying 5xx here can hide downstream failures indefinitely.
		if resp.StatusCode >= 200 {
			break
		}
		log.Warn("HTTP server returned with error", zap.Int("status", resp.StatusCode))
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()

	checkCtx:
		select {
		case <-ctx.Done():
			return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, ctx.Err())
		default:
		}

		sleepTime := expBackoff.NextBackOff()
		if sleepTime == backoff.Stop {
			if err != nil {
				return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
			}
			return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs("HTTP retry stopped")
		}
		timer := time.NewTimer(sleepTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, ctx.Err())
		case <-timer.C:
		}
	}

	return resp, nil
}

// GetConfluentSchemaIDFromHeader extracts a schema ID from a Confluent wire header.
func GetConfluentSchemaIDFromHeader(header []byte) (uint32, error) {
	if len(header) < 5 {
		return 0, errors.ErrDecodeFailed.GenWithStackByArgs("header too short")
	}
	return binary.BigEndian.Uint32(header[1:5]), nil
}
