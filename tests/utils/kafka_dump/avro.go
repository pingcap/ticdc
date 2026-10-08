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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/errors"
)

type avroMessageDecoder struct {
	registryURL string
	client      *http.Client
	codecs      map[uint32]*goavro.Codec
}

// Decode using the wire schema independently of TiCDC's Debezium decoder.
// JSON preserves bytes as Base64 strings and decimals as exact rational strings.
func (d *avroMessageDecoder) decode(ctx context.Context, data []byte) (map[string]any, error) {
	if len(data) < 5 || data[0] != 0 {
		return nil, errors.ErrAvroInvalidMessage.GenWithStackByArgs("invalid Confluent header")
	}
	schemaID := binary.BigEndian.Uint32(data[1:5])
	codec, ok := d.codecs[schemaID]
	if !ok {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			d.registryURL+"/schemas/ids/"+strconv.FormatUint(uint64(schemaID), 10), nil)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode != http.StatusOK {
			return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs(resp.Status)
		}
		var registered struct {
			Schema string `json:"schema"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&registered); err != nil {
			return nil, errors.WrapError(errors.ErrAvroSchemaAPIError, err)
		}
		codec, err = goavro.NewCodec(registered.Schema)
		if err != nil {
			return nil, errors.WrapError(errors.ErrAvroInvalidMessage, err)
		}
		d.codecs[schemaID] = codec
	}
	payload, remaining, err := codec.NativeFromBinary(data[5:])
	if err != nil {
		return nil, errors.WrapError(errors.ErrAvroInvalidMessage, err)
	}
	if len(remaining) != 0 {
		return nil, errors.ErrAvroInvalidMessage.GenWithStackByArgs("trailing Avro payload bytes")
	}
	return map[string]any{
		"schema":  json.RawMessage(codec.Schema()),
		"payload": payload,
	}, nil
}
