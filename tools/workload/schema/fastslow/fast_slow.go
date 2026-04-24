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

package fastslow

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"workload/schema"
)

const (
	defaultSlowPayloadSize = 4096
	fastPayloadSize        = 96
)

const createFastTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  touch_count BIGINT NOT NULL DEFAULT 0,
  payload VARCHAR(128) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_account_id (account_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

const createSlowTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  id BIGINT NOT NULL,
  account_id BIGINT NOT NULL,
  status BIGINT NOT NULL DEFAULT 0,
  hot_key BIGINT NOT NULL,
  touch_count BIGINT NOT NULL DEFAULT 0,
  payload MEDIUMTEXT NOT NULL,
  payload_shadow MEDIUMTEXT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_account_status (account_id, status),
  KEY idx_hot_key (hot_key),
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type FastSlowWorkload struct {
	tableCount      int
	tableStartIndex int
	fastTableCount  int

	fastPayload       string
	fastUpdatePayload string
	slowPayload       string
	slowShadowPayload string
	slowUpdatePayload string
	slowUpdateShadow  string

	nextID []atomic.Uint64
}

func NewFastSlowWorkload(rowSize int, tableCount int, tableStartIndex int) schema.Workload {
	if tableCount <= 0 {
		tableCount = 1
	}

	slowPayloadSize := rowSize
	if slowPayloadSize <= 0 {
		slowPayloadSize = defaultSlowPayloadSize
	}

	fastTableCount := tableCount / 2
	if fastTableCount == 0 && tableCount > 1 {
		fastTableCount = 1
	}

	return &FastSlowWorkload{
		tableCount:        tableCount,
		tableStartIndex:   tableStartIndex,
		fastTableCount:    fastTableCount,
		fastPayload:       buildPayload("fast_insert", fastPayloadSize),
		fastUpdatePayload: buildPayload("fast_update", fastPayloadSize),
		slowPayload:       buildPayload("slow_insert", slowPayloadSize),
		slowShadowPayload: buildPayload("slow_shadow", max(256, slowPayloadSize/2)),
		slowUpdatePayload: buildPayload("slow_update", slowPayloadSize),
		slowUpdateShadow:  buildPayload("slow_shadow_update", max(256, slowPayloadSize/2)),
		nextID:            make([]atomic.Uint64, tableCount),
	}
}

func buildPayload(label string, targetSize int) string {
	if targetSize <= 0 {
		return label
	}

	chunk := fmt.Sprintf(`{"kind":"%s","value":"0123456789abcdefghijklmnopqrstuvwxyz"}`, label)
	var builder strings.Builder
	for builder.Len() < targetSize {
		builder.WriteString(chunk)
	}
	payload := builder.String()
	return payload[:targetSize]
}

func (w *FastSlowWorkload) BuildCreateTableStatement(n int) string {
	if w.isSlowTable(n) {
		return fmt.Sprintf(createSlowTableFormat, w.tableName(n))
	}
	return fmt.Sprintf(createFastTableFormat, w.tableName(n))
}

func (w *FastSlowWorkload) BuildInsertSql(tableN int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}
	if w.isSlowTable(tableN) {
		return w.buildSlowInsert(tableN, batchSize)
	}
	return w.buildFastInsert(tableN, batchSize)
}

func (w *FastSlowWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	if w.maxID(opt.TableIndex) == 0 {
		return ""
	}
	if w.isSlowTable(opt.TableIndex) {
		return w.buildSlowUpdate(opt)
	}
	return w.buildFastUpdate(opt)
}

func (w *FastSlowWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	maxID := w.maxID(opt.TableIndex)
	if maxID == 0 {
		return ""
	}

	batch := opt.Batch
	if batch <= 0 {
		batch = 1
	}
	if w.isSlowTable(opt.TableIndex) {
		batch = min(batch, 2)
	}

	start, end := w.randomIDRange(maxID, batch)
	return fmt.Sprintf("DELETE FROM %s WHERE id BETWEEN %d AND %d",
		w.tableName(opt.TableIndex), start, end)
}

func (w *FastSlowWorkload) PickTable(op schema.OperationType) int {
	switch op {
	case schema.OperationInsert:
		return w.pickTableByWeight(8, 2)
	case schema.OperationUpdate:
		return w.pickTableByWeight(3, 7)
	case schema.OperationDelete:
		return w.pickTableByWeight(4, 6)
	default:
		return w.pickRandomTable()
	}
}

func (w *FastSlowWorkload) buildFastInsert(tableN int, batchSize int) string {
	var buf bytes.Buffer
	tableName := w.tableName(tableN)
	buf.WriteString(fmt.Sprintf(
		"INSERT INTO %s (id, account_id, touch_count, payload) VALUES",
		tableName,
	))

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			buf.WriteString(",")
		}

		id := w.nextTableID(tableN)
		buf.WriteString(fmt.Sprintf("(%d,%d,0,'%s')",
			id,
			id%2048,
			w.fastPayload,
		))
	}
	return buf.String()
}

func (w *FastSlowWorkload) buildSlowInsert(tableN int, batchSize int) string {
	var buf bytes.Buffer
	tableName := w.tableName(tableN)
	buf.WriteString(fmt.Sprintf(
		"INSERT INTO %s (id, account_id, status, hot_key, touch_count, payload, payload_shadow) VALUES",
		tableName,
	))

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			buf.WriteString(",")
		}

		id := w.nextTableID(tableN)
		buf.WriteString(fmt.Sprintf("(%d,%d,%d,%d,0,'%s','%s')",
			id,
			id%1024,
			id%16,
			id%4096,
			w.slowPayload,
			w.slowShadowPayload,
		))
	}
	return buf.String()
}

func (w *FastSlowWorkload) buildFastUpdate(opt schema.UpdateOption) string {
	batch := opt.Batch
	if batch <= 0 {
		batch = 1
	}

	start, end := w.randomIDRange(w.maxID(opt.TableIndex), batch)
	return fmt.Sprintf(
		"UPDATE %s SET touch_count = touch_count + 1, payload = '%s' WHERE id BETWEEN %d AND %d",
		w.tableName(opt.TableIndex),
		w.fastUpdatePayload,
		start,
		end,
	)
}

func (w *FastSlowWorkload) buildSlowUpdate(opt schema.UpdateOption) string {
	id := w.randomExistingID(opt.TableIndex)
	return fmt.Sprintf(
		"UPDATE %s SET touch_count = touch_count + 1, status = %d, hot_key = %d, payload = '%s', payload_shadow = '%s' WHERE id = %d",
		w.tableName(opt.TableIndex),
		rand.Intn(64),
		rand.Int63n(1_000_000),
		w.slowUpdatePayload,
		w.slowUpdateShadow,
		id,
	)
}

func (w *FastSlowWorkload) pickTableByWeight(fastWeight int, slowWeight int) int {
	if w.fastTableCount == 0 {
		return w.pickRandomSlowTable()
	}
	if w.fastTableCount == w.tableCount {
		return w.pickRandomFastTable()
	}

	totalWeight := fastWeight + slowWeight
	if totalWeight <= 0 {
		return w.pickRandomTable()
	}
	if rand.Intn(totalWeight) < fastWeight {
		return w.pickRandomFastTable()
	}
	return w.pickRandomSlowTable()
}

func (w *FastSlowWorkload) pickRandomTable() int {
	return rand.Intn(w.tableCount) + w.tableStartIndex
}

func (w *FastSlowWorkload) pickRandomFastTable() int {
	if w.fastTableCount == 0 {
		return w.pickRandomSlowTable()
	}
	return rand.Intn(w.fastTableCount) + w.tableStartIndex
}

func (w *FastSlowWorkload) pickRandomSlowTable() int {
	slowCount := w.tableCount - w.fastTableCount
	if slowCount <= 0 {
		return w.pickRandomFastTable()
	}
	return rand.Intn(slowCount) + w.tableStartIndex + w.fastTableCount
}

func (w *FastSlowWorkload) isSlowTable(tableN int) bool {
	return w.tableSlot(tableN) >= w.fastTableCount
}

func (w *FastSlowWorkload) tableName(tableN int) string {
	if w.isSlowTable(tableN) {
		return fmt.Sprintf("slow_table_%d", tableN)
	}
	return fmt.Sprintf("fast_table_%d", tableN)
}

func (w *FastSlowWorkload) tableSlot(tableN int) int {
	return tableN - w.tableStartIndex
}

func (w *FastSlowWorkload) nextTableID(tableN int) uint64 {
	return w.nextID[w.tableSlot(tableN)].Add(1)
}

func (w *FastSlowWorkload) maxID(tableN int) uint64 {
	return w.nextID[w.tableSlot(tableN)].Load()
}

func (w *FastSlowWorkload) randomExistingID(tableN int) uint64 {
	maxID := w.maxID(tableN)
	if maxID == 0 {
		return 0
	}
	return uint64(rand.Int63n(int64(maxID))) + 1
}

func (w *FastSlowWorkload) randomIDRange(maxID uint64, batch int) (uint64, uint64) {
	if maxID == 0 {
		return 0, 0
	}
	if batch <= 0 {
		batch = 1
	}

	start := uint64(rand.Int63n(int64(maxID))) + 1
	end := start + uint64(batch) - 1
	if end > maxID {
		end = maxID
	}
	return start, end
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
