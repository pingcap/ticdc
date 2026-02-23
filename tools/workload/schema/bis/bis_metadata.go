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

package bis

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
)

const (
	maxEntityMediaMetadataSize = 6144
	maxBatchAuxDataSize        = 3072
	maxBatchCallbackJobSize    = 3072
	maxBatchMetadataSize       = 16777215 // MEDIUMBLOB

	defaultUpdateKeySpace = 1_000_000

	entityInsertRatio = 0.77
	entityUpdateRatio = 0.51

	entityUpdateByPinIDWeight        = 0.66
	entityUpdateByCompositeKeyWeight = 0.25
	// remaining is entityUpdateByIDWeight

	batchUpdateStatusOnlyWeight      = 0.45
	batchUpdateMetadataWeight        = 0.31
	batchUpdateMetadataAndKeysWeight = 0.21
	// remaining is batchUpdateTimestampWeight

	entityMigratedInsertWeight = 0.01
	batchInsertWithAuxWeight   = 0.46

	entitySetSpace = 10_000
)

const createEntityMetadataTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  ` + "`id`" + ` varchar(36) NOT NULL,
  ` + "`user_id`" + ` bigint(20) NOT NULL,
  ` + "`entity_set_id`" + ` varchar(255) NOT NULL,
  ` + "`entity_id`" + ` varchar(255) NOT NULL,
  ` + "`board_id`" + ` bigint(20) DEFAULT NULL,
  ` + "`content_hash`" + ` char(32) DEFAULT NULL,
  ` + "`pin_id`" + ` bigint(20) DEFAULT NULL,
  ` + "`media_metadata`" + ` varchar(6144) DEFAULT NULL,
  ` + "`delete_after`" + ` timestamp NULL DEFAULT NULL,
  ` + "`created_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`migrated_from_hyperloop_at`" + ` timestamp NULL DEFAULT NULL,
  KEY ` + "`idx_uid_esetid_eid`" + ` (` + "`user_id`" + `,` + "`entity_set_id`" + `,` + "`entity_id`" + `),
  KEY ` + "`idx_delafter`" + ` (` + "`delete_after`" + `),
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] NONCLUSTERED */,
  KEY ` + "`idx_pinid`" + ` (` + "`pin_id`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 */ /*T![ttl] TTL=` + "`delete_after`" + ` + INTERVAL 1 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */
`

const createBatchMetadataTableFormat = `
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 */ /*T![ttl] TTL=` + "`updated_at`" + ` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */
`

type BISMetadataWorkload struct {
	entityMediaSize int
	batchAuxSize    int
	batchCjSize     int
	batchMetaSize   int

	entityMediaInsert string
	entityMediaUpdate string
	batchAuxData      string
	batchCjData       string
	batchMetaInsert   []byte
	batchMetaUpdate   []byte

	tableStartIndex int

	entitySeq []atomic.Uint64
	batchSeq  []atomic.Uint64

	perTableUpdateKeySpace uint64

	idSuffix [4]byte

	seed     atomic.Int64
	randPool sync.Pool
}

func NewBISMetadataWorkload(rowSize int, tableCount int, tableStartIndex int, totalRowCount uint64) schema.Workload {
	if rowSize < 0 {
		rowSize = 0
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	entityMediaSize := min(rowSize, maxEntityMediaMetadataSize)
	if rowSize > maxEntityMediaMetadataSize {
		plog.Warn("row size too large for bis entity media_metadata, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxSize", maxEntityMediaMetadataSize))
	}

	auxSize := min(rowSize/8, maxBatchAuxDataSize)
	cjSize := min(rowSize/8, maxBatchCallbackJobSize)
	metaSize := rowSize - auxSize - cjSize
	if metaSize < 0 {
		metaSize = 0
	}
	if metaSize > maxBatchMetadataSize {
		plog.Warn("row size too large for bis batch metadata, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxSize", maxBatchMetadataSize+maxBatchAuxDataSize+maxBatchCallbackJobSize))
		metaSize = maxBatchMetadataSize
	}

	perTableUpdateKeySpace := uint64(defaultUpdateKeySpace)
	if totalRowCount > 0 {
		perTableUpdateKeySpace = maxUint64(1, totalRowCount/uint64(tableCount))
	}

	w := &BISMetadataWorkload{
		entityMediaSize:        entityMediaSize,
		batchAuxSize:           auxSize,
		batchCjSize:            cjSize,
		batchMetaSize:          metaSize,
		entityMediaInsert:      strings.Repeat("a", entityMediaSize),
		entityMediaUpdate:      strings.Repeat("b", entityMediaSize),
		batchAuxData:           strings.Repeat("c", auxSize),
		batchCjData:            strings.Repeat("d", cjSize),
		batchMetaInsert:        newConstBytes(metaSize, 'e'),
		batchMetaUpdate:        newConstBytes(metaSize, 'f'),
		tableStartIndex:        tableStartIndex,
		entitySeq:              make([]atomic.Uint64, tableCount),
		batchSeq:               make([]atomic.Uint64, tableCount),
		perTableUpdateKeySpace: perTableUpdateKeySpace,
	}

	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}

	r := w.getRand()
	binary.BigEndian.PutUint32(w.idSuffix[:], r.Uint32())
	w.putRand(r)

	plog.Info("bis metadata workload initialized",
		zap.Int("rowSize", rowSize),
		zap.Int("entityMediaSize", w.entityMediaSize),
		zap.Int("batchAuxDataSize", w.batchAuxSize),
		zap.Int("batchCallbackJobMetadataSize", w.batchCjSize),
		zap.Int("batchMetadataSize", w.batchMetaSize),
		zap.Uint64("perTableUpdateKeySpace", w.perTableUpdateKeySpace),
		zap.Int("tableStartIndex", w.tableStartIndex),
		zap.Int("tableSlots", len(w.entitySeq)))
	return w
}

func (w *BISMetadataWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *BISMetadataWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func getEntityTableName(n int) string {
	if n == 0 {
		return "`bis_entity_metadata`"
	}
	return fmt.Sprintf("`bis_entity_metadata_%d`", n)
}

func getBatchTableName(n int) string {
	if n == 0 {
		return "`bis_batch_metadata`"
	}
	return fmt.Sprintf("`bis_batch_metadata_%d`", n)
}

func (w *BISMetadataWorkload) slot(tableIndex int) int {
	if len(w.entitySeq) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return slot % len(w.entitySeq)
}

func (w *BISMetadataWorkload) newID(kind byte, tableIndex int, seq uint64) string {
	var id uuid.UUID
	id[0] = kind
	binary.BigEndian.PutUint32(id[1:5], uint32(tableIndex))
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], seq)
	copy(id[5:12], seqBuf[1:])
	copy(id[12:], w.idSuffix[:])
	return id.String()
}

func (w *BISMetadataWorkload) userID(tableIndex int, seq uint64) int64 {
	return int64(uint64(tableIndex)*1_000_000 + (seq % 1_000_000))
}

func (w *BISMetadataWorkload) entitySetID(seq uint64) string {
	return fmt.Sprintf("eset_%d", seq%entitySetSpace)
}

func (w *BISMetadataWorkload) entityID(seq uint64) string {
	return fmt.Sprintf("eid_%d", seq)
}

func (w *BISMetadataWorkload) contentHash(tableIndex int, seq uint64) string {
	return fmt.Sprintf("%016x%016x", uint64(tableIndex), seq)
}

func (w *BISMetadataWorkload) pinID(tableIndex int, seq uint64) int64 {
	return int64(uint64(tableIndex)*1_000_000 + seq)
}

func (w *BISMetadataWorkload) BuildCreateTableStatement(n int) string {
	entityName := getEntityTableName(n)
	batchName := getBatchTableName(n)
	return fmt.Sprintf(createEntityMetadataTableFormat, entityName) + ";" + fmt.Sprintf(createBatchMetadataTableFormat, batchName)
}

func (w *BISMetadataWorkload) BuildInsertSql(tableIndex int, batchSize int) string {
	entityName := getEntityTableName(tableIndex)
	mediaExpr := fmt.Sprintf("REPEAT('a',%d)", w.entityMediaSize)
	return fmt.Sprintf("INSERT INTO %s (`updated_at`, `created_at`, `id`, `user_id`, `entity_set_id`, `entity_id`, `board_id`, `content_hash`, `pin_id`, `media_metadata`) VALUES (NOW(), NOW(), UUID(), 1, 'eset_0', 'eid_0', 1, '00000000000000000000000000000000', 1, %s)", entityName, mediaExpr)
}

func (w *BISMetadataWorkload) BuildInsertSqlWithValues(tableIndex int, batchSize int) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	if r.Float64() < entityInsertRatio {
		return w.buildEntityInsertWithValues(tableIndex, batchSize, r)
	}
	return w.buildBatchInsertWithValues(tableIndex, batchSize, r)
}

func (w *BISMetadataWorkload) buildEntityInsertWithValues(tableIndex int, batchSize int, r *rand.Rand) (string, []interface{}) {
	tableName := getEntityTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	includeMigrated := r.Float64() < entityMigratedInsertWeight

	columns := "`updated_at`, `created_at`, `id`, `user_id`, `entity_set_id`, `entity_id`, `board_id`, `content_hash`, `pin_id`, `media_metadata`"
	colCount := 10
	if includeMigrated {
		columns += ", `migrated_from_hyperloop_at`"
		colCount = 11
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	sb.WriteString(columns)
	sb.WriteString(") VALUES ")

	placeholders := make([]string, 0, batchSize)
	values := make([]interface{}, 0, batchSize*colCount)

	for range batchSize {
		seq := w.entitySeq[slot].Add(1)
		id := w.newID('e', tableIndex, seq)
		userID := w.userID(tableIndex, seq)
		entitySetID := w.entitySetID(seq)
		entityID := w.entityID(seq)
		boardID := int64(seq % 100_000)
		contentHash := w.contentHash(tableIndex, seq)
		pinID := w.pinID(tableIndex, seq)

		if includeMigrated {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, userID, entitySetID, entityID, boardID, contentHash, pinID, w.entityMediaInsert, now)
		} else {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, userID, entitySetID, entityID, boardID, contentHash, pinID, w.entityMediaInsert)
		}
	}

	sb.WriteString(strings.Join(placeholders, ","))
	return sb.String(), values
}

func (w *BISMetadataWorkload) buildBatchInsertWithValues(tableIndex int, batchSize int, r *rand.Rand) (string, []interface{}) {
	tableName := getBatchTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	includeAux := r.Float64() < batchInsertWithAuxWeight

	columns := "`created_at`, `updated_at`, `id`, `user_id`, `entity_set_id`, `status`, `metadata`, `client_id`, `callback_job_metadata`"
	colCount := 9
	if includeAux {
		columns = "`created_at`, `updated_at`, `id`, `user_id`, `entity_set_id`, `status`, `metadata`, `client_id`, `aux_data`, `callback_job_metadata`, `bmd_image_pipeline_id`, `bmd_client_name`"
		colCount = 12
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	sb.WriteString(columns)
	sb.WriteString(") VALUES ")

	placeholders := make([]string, 0, batchSize)
	values := make([]interface{}, 0, batchSize*colCount)

	for range batchSize {
		seq := w.batchSeq[slot].Add(1)
		id := w.newID('b', tableIndex, seq)
		userID := w.userID(tableIndex, seq)
		entitySetID := w.entitySetID(seq)
		status := int16(r.Intn(16))
		clientID := fmt.Sprintf("client_%d", seq%100)

		if includeAux {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, userID, entitySetID, status, w.batchMetaInsert, clientID,
				w.batchAuxData, w.batchCjData, fmt.Sprintf("pipe_%d", seq%100), fmt.Sprintf("client_%d", seq%100))
		} else {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, userID, entitySetID, status, w.batchMetaInsert, clientID, w.batchCjData)
		}
	}

	sb.WriteString(strings.Join(placeholders, ","))
	return sb.String(), values
}

func (w *BISMetadataWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	entityName := getEntityTableName(opt.TableIndex)
	return fmt.Sprintf("UPDATE %s SET `updated_at` = NOW() WHERE `delete_after` IS NULL LIMIT %d", entityName, max(1, opt.Batch))
}

func (w *BISMetadataWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	if r.Float64() < entityUpdateRatio {
		return w.buildEntityUpdateWithValues(opt.TableIndex, r)
	}
	return w.buildBatchUpdateWithValues(opt.TableIndex, r)
}

func (w *BISMetadataWorkload) buildEntityUpdateWithValues(tableIndex int, r *rand.Rand) (string, []interface{}) {
	tableName := getEntityTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	upper := minUint64(w.perTableUpdateKeySpace, w.entitySeq[slot].Load())
	seq := randSeq(r, upper)

	switch {
	case r.Float64() < entityUpdateByPinIDWeight:
		pinID := w.pinID(tableIndex, seq)
		sql := fmt.Sprintf("UPDATE %s SET `delete_after` = ?, `updated_at` = ?, `pin_id` = ? WHERE (`pin_id` = ?) AND `delete_after` IS NULL", tableName)
		return sql, []interface{}{nil, now, pinID, pinID}
	case r.Float64() < entityUpdateByPinIDWeight+entityUpdateByCompositeKeyWeight:
		userID := w.userID(tableIndex, seq)
		entitySetID := w.entitySetID(seq)
		entityID := w.entityID(seq)
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `user_id` = ?, `content_hash` = ?, `entity_id` = ?, `entity_set_id` = ? WHERE (`entity_id` = ?) AND (`entity_set_id` = ?) AND (`user_id` = ?) AND `delete_after` IS NULL", tableName)
		contentHash := w.contentHash(tableIndex, uint64(now.UnixNano()))
		return sql, []interface{}{now, userID, contentHash, entityID, entitySetID, entityID, entitySetID, userID}
	default:
		id := w.newID('e', tableIndex, seq)
		sql := fmt.Sprintf("UPDATE %s SET `id` = ?, `delete_after` = ?, `updated_at` = ? WHERE (`id` = ?) AND `delete_after` IS NULL", tableName)
		return sql, []interface{}{id, nil, now, id}
	}
}

func (w *BISMetadataWorkload) buildBatchUpdateWithValues(tableIndex int, r *rand.Rand) (string, []interface{}) {
	tableName := getBatchTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	upper := minUint64(w.perTableUpdateKeySpace, w.batchSeq[slot].Load())
	seq := randSeq(r, upper)

	id := w.newID('b', tableIndex, seq)
	status := int16(r.Intn(16))

	p := r.Float64()
	switch {
	case p < batchUpdateStatusOnlyWeight:
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{now, status, id}
	case p < batchUpdateStatusOnlyWeight+batchUpdateMetadataWeight:
		sql := fmt.Sprintf("UPDATE %s SET `metadata` = ?, `updated_at` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.batchMetaUpdate, now, status, id}
	case p < batchUpdateStatusOnlyWeight+batchUpdateMetadataWeight+batchUpdateMetadataAndKeysWeight:
		userID := w.userID(tableIndex, uint64(now.UnixNano()))
		entitySetID := w.entitySetID(uint64(now.UnixNano()))
		sql := fmt.Sprintf("UPDATE %s SET `metadata` = ?, `updated_at` = ?, `user_id` = ?, `entity_set_id` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.batchMetaUpdate, now, userID, entitySetID, status, id}
	default:
		userID := w.userID(tableIndex, uint64(now.UnixNano()))
		entitySetID := w.entitySetID(uint64(now.UnixNano()))
		start := now.Add(-time.Minute)
		end := now
		sql := fmt.Sprintf("UPDATE %s SET `bmd_end_timestamp` = ?, `metadata` = ?, `updated_at` = ?, `user_id` = ?, `bmd_start_timestamp` = ?, `entity_set_id` = ?, `status` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{end, w.batchMetaUpdate, now, userID, start, entitySetID, status, id}
	}
}

func (w *BISMetadataWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	r := w.getRand()
	defer w.putRand(r)

	tableName := getEntityTableName(opt.TableIndex)
	slot := w.slot(opt.TableIndex)
	upper := minUint64(w.perTableUpdateKeySpace, w.entitySeq[slot].Load())

	deleteByID := r.Float64() < 0.67

	var sb strings.Builder
	for i := 0; i < max(1, opt.Batch); i++ {
		if i > 0 {
			sb.WriteString(";")
		}
		seq := randSeq(r, upper)
		if deleteByID {
			id := w.newID('e', opt.TableIndex, seq)
			sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE (`id` = '%s')", tableName, id))
			continue
		}
		userID := w.userID(opt.TableIndex, seq)
		entitySetID := w.entitySetID(seq)
		entityID := w.entityID(seq)
		sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE (`entity_id` = '%s') AND (`entity_set_id` = '%s') AND (`user_id` = %d)",
			tableName, entityID, entitySetID, userID))
	}
	return sb.String()
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

func newConstBytes(size int, value byte) []byte {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = value
	}
	return buf
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
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
