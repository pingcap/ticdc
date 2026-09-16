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

package messaging

import (
	"encoding/json"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
)

// SchemaStoreTableInfosRequest is used to query table infos from schema store.
// It is mainly used for changefeed bootstrap in downstream adapter.
type SchemaStoreTableInfosRequest struct {
	RequestID    uint64  `json:"request_id"`
	KeyspaceID   uint32  `json:"keyspace_id"`
	KeyspaceName string  `json:"keyspace_name"`
	TableIDs     []int64 `json:"table_ids"`
	Ts           uint64  `json:"ts"`
}

func (r *SchemaStoreTableInfosRequest) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *SchemaStoreTableInfosRequest) Unmarshal(data []byte) error {
	return json.Unmarshal(data, r)
}

// SchemaStoreTableInfosResponse is a streamed response for SchemaStoreTableInfosRequest.
//
// For each requested table, schema store sends one response with:
// - RequestID + TableID + TableInfo (or Error)
//
// Then it sends a final response with:
// - RequestID + Done=true (and optional Error for request-level failures).
type SchemaStoreTableInfosResponse struct {
	RequestID uint64 `json:"request_id"`
	TableID   int64  `json:"table_id"`
	// TableInfo is the marshaled bytes of `common.TableInfo`.
	TableInfo []byte `json:"table_info,omitempty"`
	Error     string `json:"error,omitempty"`
	Done      bool   `json:"done,omitempty"`
}

func (r *SchemaStoreTableInfosResponse) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *SchemaStoreTableInfosResponse) Unmarshal(data []byte) error {
	return json.Unmarshal(data, r)
}

// SchemaStoreOperation identifies a non-streaming schema store request.
type SchemaStoreOperation int

const (
	SchemaStoreRegisterKeyspace SchemaStoreOperation = iota + 1
	SchemaStoreGetAllPhysicalTables
)

// SchemaStoreRequest registers a keyspace or queries its physical tables.
type SchemaStoreRequest struct {
	RequestID      uint64               `json:"request_id"`
	Operation      SchemaStoreOperation `json:"operation"`
	Keyspace       common.KeyspaceMeta  `json:"keyspace"`
	Ts             uint64               `json:"ts,omitempty"`
	Filter         *config.FilterConfig `json:"filter,omitempty"`
	CaseSensitive  bool                 `json:"case_sensitive,omitempty"`
	ForceReplicate bool                 `json:"force_replicate,omitempty"`
}

func (r *SchemaStoreRequest) Marshal() ([]byte, error)    { return json.Marshal(r) }
func (r *SchemaStoreRequest) Unmarshal(data []byte) error { return json.Unmarshal(data, r) }

// SchemaStoreResponse contains a request result and preserves the original error code.
type SchemaStoreResponse struct {
	RequestID uint64              `json:"request_id"`
	Tables    []commonEvent.Table `json:"tables,omitempty"`
	Error     string              `json:"error,omitempty"`
	ErrorCode string              `json:"error_code,omitempty"`
}

func (r *SchemaStoreResponse) Marshal() ([]byte, error)    { return json.Marshal(r) }
func (r *SchemaStoreResponse) Unmarshal(data []byte) error { return json.Unmarshal(data, r) }
