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

// Package failpointrecord provides a lightweight utility that records failpoint
// triggered events (PK, commit_ts, etc.) to a JSONL file so that external tools
// (e.g. the multi-cluster-consistency-checker) can easily consume them.
//
// The file path is controlled by the environment variable
// TICDC_FAILPOINT_RECORD_FILE. When the variable is empty or unset the
// recorder is a silent no-op, introducing zero overhead in production.
//
// Each line written to the file is a self-contained JSON object:
//
//	{"time":"…","failpoint":"cloudStorageSinkDropMessage","rows":[{"commitTs":123,"primaryKeys":{"id":1}}]}
package failpointrecord

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// envKey is the environment variable that controls the output file path.
const envKey = "TICDC_FAILPOINT_RECORD_FILE"

// RowRecord captures the essential identity of a single affected row.
type RowRecord struct {
	CommitTs    uint64         `json:"commitTs"`
	OriginTs    uint64         `json:"originTs"`
	PrimaryKeys map[string]any `json:"primaryKeys"`
}

// NormalizeOriginTs converts `_tidb_origin_ts` values from row payloads into uint64.
// It returns 0 for nil/invalid values.
func NormalizeOriginTs(v any) uint64 {
	switch value := v.(type) {
	case nil:
		return 0
	case uint64:
		return value
	case uint:
		return uint64(value)
	case uint32:
		return uint64(value)
	case uint16:
		return uint64(value)
	case uint8:
		return uint64(value)
	case int64:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int32:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int16:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case int8:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case float64:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case json.Number:
		i, err := value.Int64()
		if err != nil || i < 0 {
			return 0
		}
		return uint64(i)
	case string:
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return 0
		}
		return parsed
	default:
		return 0
	}
}

// Record is one line written to the JSONL file.
type Record struct {
	Time      string      `json:"time"`
	Failpoint string      `json:"failpoint"`
	Rows      []RowRecord `json:"rows"`
}

var (
	initOnce sync.Once
	mu       sync.Mutex
	file     *os.File
	disabled bool
)

func ensureFile() {
	initOnce.Do(func() {
		path := os.Getenv(envKey)
		if path == "" {
			disabled = true
			return
		}
		var err error
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			log.Warn("failed to open failpoint record file, recording disabled",
				zap.String("path", path), zap.Error(err))
			disabled = true
			return
		}
		log.Info("failpoint record file opened", zap.String("path", path))
	})
}

// Write persists one failpoint event to the JSONL file.
// It is safe for concurrent use.
// If the env var is not set the call is a no-op (zero allocation).
func Write(failpoint string, rows []RowRecord) {
	if disabled {
		return
	}
	ensureFile()
	if file == nil {
		return
	}

	rec := Record{
		Time:      time.Now().UTC().Format(time.RFC3339Nano),
		Failpoint: failpoint,
		Rows:      rows,
	}
	data, err := json.Marshal(rec)
	if err != nil {
		log.Warn("failed to marshal failpoint record", zap.Error(err))
		return
	}
	data = append(data, '\n')

	mu.Lock()
	defer mu.Unlock()
	if _, err := file.Write(data); err != nil {
		log.Warn("failed to write failpoint record", zap.Error(err))
	}
}
