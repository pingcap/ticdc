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

package hotspot

import (
	"bytes"
	"fmt"
	"math/rand"

	"workload/schema"
)

const createHotspotTable = `
CREATE TABLE IF NOT EXISTS hotspot_%d (
  id BIGINT NOT NULL,
  balance BIGINT NOT NULL DEFAULT 0,
  version BIGINT NOT NULL DEFAULT 0,
  payload VARCHAR(256) NOT NULL DEFAULT '',
  updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (id),
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`

type HotspotWorkload struct {
	hotRowCount int
}

func NewHotspotWorkload(hotRowCount int) schema.Workload {
	if hotRowCount <= 0 {
		hotRowCount = 1
	}
	return &HotspotWorkload{hotRowCount: hotRowCount}
}

func (w *HotspotWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createHotspotTable, n)
}

func (w *HotspotWorkload) BuildInsertSql(tableN int, batchSize int) string {
	return w.buildUpsertSQL(tableN, batchSize)
}

func (w *HotspotWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	return w.buildUpsertSQL(opts.TableIndex, opts.Batch)
}

func (w *HotspotWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	if opts.Batch <= 0 {
		return ""
	}

	ids := w.pickHotIDs(opts.Batch)
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("DELETE FROM hotspot_%d WHERE id IN (", opts.TableIndex))
	for i, id := range ids {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%d", id))
	}
	buf.WriteString(")")
	return buf.String()
}

func (w *HotspotWorkload) buildUpsertSQL(tableN int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}

	ids := w.pickHotIDs(batchSize)
	payload := rand.Int63()

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("INSERT INTO hotspot_%d (id, balance, version, payload) VALUES ", tableN))
	for i, id := range ids {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("(%d,1,1,'payload-%d-%d')", id, payload, i))
	}
	buf.WriteString(" ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance), version = version + 1, payload = VALUES(payload)")
	return buf.String()
}

func (w *HotspotWorkload) pickHotIDs(batchSize int) []int {
	if batchSize > w.hotRowCount {
		batchSize = w.hotRowCount
	}

	ids := make([]int, 0, batchSize)
	start := rand.Intn(w.hotRowCount)
	for i := 0; i < batchSize; i++ {
		ids = append(ids, start%w.hotRowCount+1)
		start++
	}
	return ids
}
