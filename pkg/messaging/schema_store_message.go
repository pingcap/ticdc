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
	"time"

	"github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
)

const (
	SchemaStoreRequestTimeout  = 10 * time.Minute
	SchemaStoreTableBatchSize  = 128
	SchemaStoreTableBatchBytes = 4 << 20
)

type SchemaStoreOperation int

const (
	SchemaStoreRegisterKeyspace SchemaStoreOperation = iota + 1
	SchemaStoreGetAllPhysicalTables
	SchemaStoreGetTableInfos
	SchemaStoreCancelRequest
)

// SchemaStoreRequest has one response. Deadline is an absolute Unix timestamp in
// nanoseconds, including time spent waiting in the server's queue. Cancellation
// uses the original RequestID and does not produce another response.
type SchemaStoreRequest struct {
	RequestID      uint64               `json:"request_id"`
	Operation      SchemaStoreOperation `json:"operation"`
	Deadline       int64                `json:"deadline"`
	Keyspace       common.KeyspaceMeta  `json:"keyspace"`
	Ts             uint64               `json:"ts,omitempty"`
	TableIDs       []int64              `json:"table_ids,omitempty"`
	Filter         *config.FilterConfig `json:"filter,omitempty"`
	CaseSensitive  bool                 `json:"case_sensitive,omitempty"`
	ForceReplicate bool                 `json:"force_replicate,omitempty"`
}

func (r *SchemaStoreRequest) Marshal() ([]byte, error)    { return json.Marshal(r) }
func (r *SchemaStoreRequest) Unmarshal(data []byte) error { return json.Unmarshal(data, r) }

// SchemaStoreTableInfo accounts for one table, including explicit table errors.
type SchemaStoreTableInfo struct {
	TableID   int64  `json:"table_id"`
	TableInfo []byte `json:"table_info,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SchemaStoreResponse preserves error codes. TableInfos contains results in the
// requested order. More is set only when the byte limit stops the batch early;
// the client requests the remaining IDs in a new request.
type SchemaStoreResponse struct {
	RequestID  uint64                 `json:"request_id"`
	Tables     []commonEvent.Table    `json:"tables,omitempty"`
	TableInfos []SchemaStoreTableInfo `json:"table_infos,omitempty"`
	More       bool                   `json:"more,omitempty"`
	Error      string                 `json:"error,omitempty"`
	ErrorCode  string                 `json:"error_code,omitempty"`
}

func (r *SchemaStoreResponse) Marshal() ([]byte, error)    { return json.Marshal(r) }
func (r *SchemaStoreResponse) Unmarshal(data []byte) error { return json.Unmarshal(data, r) }
