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

package checker

import (
	"encoding/json"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// envKey is the environment variable that controls the output file path.
const envKey = "TICDC_MULTI_CLUSTER_CONSISTENCY_CHECKER_FAILPOINT_RECORD_FILE"

// RowRecord captures the essential identity of a single affected row.
type RowRecord struct {
	CommitTs    uint64         `json:"commitTs"`
	PrimaryKeys map[string]any `json:"primaryKeys"`
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
	disabled atomic.Bool
)

func ensureFile() {
	initOnce.Do(func() {
		path := os.Getenv(envKey)
		if path == "" {
			disabled.Store(true)
			return
		}
		var err error
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Warn("failed to open failpoint record file, recording disabled",
				zap.String("path", path), zap.Error(err))
			disabled.Store(true)
			return
		}
		log.Info("failpoint record file opened", zap.String("path", path))
	})
}

// Write persists one failpoint event to the JSONL file.
// It is safe for concurrent use.
// If the env var is not set the call is a no-op (zero allocation).
func Write(failpoint string, rows []RowRecord) {
	if disabled.Load() {
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
