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

package bisbatchmetadata

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
)

const (
	maxMediumBlobSize = 16777215
	maxAuxDataSize    = 3072

	defaultUpdateKeySpace = 1_000_000

	statusOnlyUpdateWeight      = 0.45
	metadataUpdateWeight        = 0.31
	metadataAndKeysUpdateWeight = 0.24
)

const createTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  ` + "`id`" + ` varchar(36) NOT NULL,
  ` + "`user_id`" + ` bigint(20) NOT NULL,
  ` + "`entity_set_id`" + ` varchar(255) NOT NULL,
  ` + "`status`" + ` smallint(6) DEFAULT NULL,
  ` + "`metadata`" + ` mediumblob DEFAULT NULL,
  ` + "`created_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`client_id`" + ` varchar(255) DEFAULT NULL,
  ` + "`aux_data`" + ` varchar(3072) DEFAULT NULL,
  ` + "`callback_job_metadata`" + ` varchar(3072) DEFAULT NULL,
  ` + "`bmd_image_pipeline_id`" + ` varchar(255) DEFAULT NULL,
  ` + "`bmd_client_name`" + ` varchar(255) DEFAULT NULL,
  ` + "`skip_pin_operations`" + ` tinyint(1) DEFAULT NULL,
  ` + "`bmd_start_timestamp`" + ` timestamp NULL DEFAULT NULL,
  ` + "`bmd_end_timestamp`" + ` timestamp NULL DEFAULT NULL,
  ` + "`pinshot_start_timestamp`" + ` timestamp NULL DEFAULT NULL,
  ` + "`pinshot_end_timestamp`" + ` timestamp NULL DEFAULT NULL,
  KEY ` + "`idx_uid_esetid`" + ` (` + "`user_id`" + `,` + "`entity_set_id`" + `),
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
/*T! SHARD_ROW_ID_BITS=4 */
/*T![ttl] TTL=` + "`updated_at`" + ` + INTERVAL 1 HOUR */
/*T![ttl] TTL_ENABLE='ON' */
/*T![ttl] TTL_JOB_INTERVAL='1h' */
`

// BISBatchMetadataWorkload generates the bis_batch_metadata business DML pattern.
type BISBatchMetadataWorkload struct {
	metadataInsert []byte
	metadataUpdate []byte
	auxData        string
	callbackData   string

	tableStartIndex        int
	rowSeq                 []atomic.Uint64
	perTableUpdateKeySpace uint64

	seed     atomic.Int64
	randPool sync.Pool
}

func NewBISBatchMetadataWorkload(rowSize int, tableCount int, tableStartIndex int, totalRowCount uint64) schema.Workload {
	if rowSize < 0 {
		rowSize = 0
	}
	if rowSize > maxMediumBlobSize {
		plog.Warn("row size too large for bis_batch_metadata metadata, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxSize", maxMediumBlobSize))
		rowSize = maxMediumBlobSize
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	perTableUpdateKeySpace := uint64(defaultUpdateKeySpace)
	if totalRowCount > 0 {
		perTableUpdateKeySpace = maxUint64(1, totalRowCount/uint64(tableCount))
	}

	w := &BISBatchMetadataWorkload{
		tableStartIndex:        tableStartIndex,
		rowSeq:                 make([]atomic.Uint64, tableCount),
		perTableUpdateKeySpace: perTableUpdateKeySpace,
	}
	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}

	r := w.getRand()
	w.metadataInsert = newPayloadBytes(rowSize, r)
	w.metadataUpdate = newPayloadBytes(rowSize, r)
	w.auxData = newPayloadString(min(rowSize/8, maxAuxDataSize), r)
	w.callbackData = newPayloadString(min(rowSize/8, maxAuxDataSize), r)
	w.putRand(r)

	plog.Info("bis_batch_metadata workload initialized",
		zap.Int("rowSize", rowSize),
		zap.Int("metadataSize", len(w.metadataInsert)),
		zap.Int("auxDataSize", len(w.auxData)),
		zap.Int("callbackJobMetadataSize", len(w.callbackData)),
		zap.Uint64("perTableUpdateKeySpace", w.perTableUpdateKeySpace),
		zap.Int("tableStartIndex", w.tableStartIndex),
		zap.Int("tableSlots", len(w.rowSeq)))
	return w
}

func (w *BISBatchMetadataWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createTableFormat, getTableName(n))
}

func (w *BISBatchMetadataWorkload) BuildInsertSql(tableIndex int, batchSize int) string {
	tableName := getTableName(tableIndex)
	metadataExpr := fmt.Sprintf("REPEAT('m',%d)", len(w.metadataInsert))
	return fmt.Sprintf("INSERT INTO %s (`created_at`, `updated_at`, `id`, `user_id`, `entity_set_id`, `status`, `metadata`, `client_id`, `aux_data`, `callback_job_metadata`, `bmd_image_pipeline_id`, `bmd_client_name`) VALUES (NOW(), NOW(), UUID(), 1, 'entity-set-0', 0, %s, 'client-0', '', '', 'pipeline-0', 'client-name-0')", tableName, metadataExpr)
}

func (w *BISBatchMetadataWorkload) BuildInsertSqlWithValues(tableIndex int, batchSize int) (string, []interface{}) {
	tableName := getTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (`created_at`, `updated_at`, `id`, `user_id`, `entity_set_id`, `status`, `metadata`, `client_id`, `aux_data`, `callback_job_metadata`, `bmd_image_pipeline_id`, `bmd_client_name`) VALUES ")

	placeholders := make([]string, 0, batchSize)
	values := make([]interface{}, 0, batchSize*12)
	for range batchSize {
		seq := w.rowSeq[slot].Add(1)
		placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?,?)")
		values = append(values,
			now,
			now,
			rowID(tableIndex, seq),
			userID(tableIndex, seq),
			entitySetID(seq),
			status(seq),
			w.metadataInsert,
			clientID(seq),
			w.auxData,
			w.callbackData,
			pipelineID(seq),
			clientName(seq),
		)
	}

	sb.WriteString(strings.Join(placeholders, ","))
	return sb.String(), values
}

func (w *BISBatchMetadataWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	tableName := getTableName(opt.TableIndex)
	return fmt.Sprintf("UPDATE %s SET `updated_at` = NOW(), `status` = 7 WHERE (`id` = (SELECT `id` FROM %s LIMIT 1))", tableName, tableName)
}

func (w *BISBatchMetadataWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	tableName := getTableName(opt.TableIndex)
	now := time.Now()
	seq := w.randExistingSeq(opt.TableIndex, r)
	id := rowID(opt.TableIndex, seq)
	nextSeq := uint64(now.UnixNano())
	state := status(nextSeq)

	p := r.Float64()
	switch {
	case p < statusOnlyUpdateWeight:
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{now, state, id}
	case p < statusOnlyUpdateWeight+metadataUpdateWeight:
		sql := fmt.Sprintf("UPDATE %s SET `metadata` = ?, `updated_at` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.metadataUpdate, now, state, id}
	case p < statusOnlyUpdateWeight+metadataUpdateWeight+metadataAndKeysUpdateWeight:
		sql := fmt.Sprintf("UPDATE %s SET `metadata` = ?, `updated_at` = ?, `user_id` = ?, `entity_set_id` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.metadataUpdate, now, userID(opt.TableIndex, nextSeq), entitySetID(nextSeq), state, id}
	default:
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{now, state, id}
	}
}

func (w *BISBatchMetadataWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	return ""
}

func (w *BISBatchMetadataWorkload) slot(tableIndex int) int {
	if len(w.rowSeq) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return slot % len(w.rowSeq)
}

func (w *BISBatchMetadataWorkload) randExistingSeq(tableIndex int, r *rand.Rand) uint64 {
	slot := w.slot(tableIndex)
	upper := w.rowSeq[slot].Load()
	if upper == 0 {
		upper = w.perTableUpdateKeySpace
	}
	return randSeq(r, upper)
}

func (w *BISBatchMetadataWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *BISBatchMetadataWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func getTableName(n int) string {
	if n == 0 {
		return "`bis_batch_metadata`"
	}
	return fmt.Sprintf("`bis_batch_metadata_%d`", n)
}

func rowID(tableIndex int, seq uint64) string {
	return fmt.Sprintf("%08x%024x", uint32(tableIndex), seq)
}

func userID(tableIndex int, seq uint64) int64 {
	return int64(uint64(tableIndex)*1_000_000 + seq%1_000_000)
}

func entitySetID(seq uint64) string {
	return fmt.Sprintf("entity-set-%06d", seq%10_000)
}

func status(seq uint64) int16 {
	return int16(seq % 10)
}

func clientID(seq uint64) string {
	return fmt.Sprintf("client-%04d", seq%1000)
}

func pipelineID(seq uint64) string {
	return fmt.Sprintf("pipeline-%04d", seq%1000)
}

func clientName(seq uint64) string {
	return fmt.Sprintf("client-name-%04d", seq%1000)
}

func newPayloadString(size int, r *rand.Rand) string {
	if size <= 0 {
		return ""
	}
	buf := make([]byte, size)
	fillPayload(buf, r)
	return string(buf)
}

func newPayloadBytes(size int, r *rand.Rand) []byte {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	fillPayload(buf, r)
	return buf
}

func fillPayload(dst []byte, r *rand.Rand) {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	if len(dst) == 0 {
		return
	}
	_, _ = r.Read(dst)
	for i, b := range dst {
		dst[i] = alphabet[int(b)%len(alphabet)]
	}
}

func randSeq(r *rand.Rand, upper uint64) uint64 {
	if upper <= 1 {
		return 1
	}
	if upper <= uint64(math.MaxInt64) {
		return uint64(r.Int63n(int64(upper))) + 1
	}
	return (r.Uint64() % upper) + 1
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
