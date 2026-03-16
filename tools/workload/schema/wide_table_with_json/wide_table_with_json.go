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

package widetablewithjson

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

	entityUpdateBySecondaryIDWeight  = 0.66
	entityUpdateByCompositeKeyWeight = 0.25
	// remaining is entityUpdateByIDWeight

	batchUpdateStatusOnlyWeight      = 0.45
	batchUpdateMetadataWeight        = 0.31
	batchUpdateMetadataAndKeysWeight = 0.21
	// remaining is batchUpdateTimestampWeight

	entityMigratedInsertWeight = 0.01
	batchInsertWithAuxWeight   = 0.46

	attrKeySpace = 10_000
)

const createPrimaryTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  ` + "`id`" + ` varchar(36) NOT NULL,
  ` + "`owner_key`" + ` bigint(20) NOT NULL,
  ` + "`attr_key_1`" + ` varchar(255) NOT NULL,
  ` + "`attr_key_2`" + ` varchar(255) NOT NULL,
  ` + "`group_key`" + ` bigint(20) DEFAULT NULL,
  ` + "`hash_value`" + ` char(32) DEFAULT NULL,
  ` + "`lookup_key`" + ` bigint(20) DEFAULT NULL,
  ` + "`payload_text`" + ` varchar(6144) DEFAULT NULL,
  ` + "`expire_at`" + ` timestamp NULL DEFAULT NULL,
  ` + "`created_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`sync_at`" + ` timestamp NULL DEFAULT NULL,
  KEY ` + "`idx_owner_attr_keys`" + ` (` + "`owner_key`" + `,` + "`attr_key_1`" + `,` + "`attr_key_2`" + `),
  KEY ` + "`idx_expire_at`" + ` (` + "`expire_at`" + `),
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] NONCLUSTERED */,
  KEY ` + "`idx_lookup_key`" + ` (` + "`lookup_key`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 */ /*T![ttl] TTL=` + "`expire_at`" + ` + INTERVAL 1 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */
`

const createSecondaryTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  ` + "`id`" + ` varchar(36) NOT NULL,
  ` + "`owner_key`" + ` bigint(20) NOT NULL,
  ` + "`attr_key_1`" + ` varchar(255) NOT NULL,
  ` + "`state`" + ` smallint(6) DEFAULT NULL,
  ` + "`payload_blob`" + ` mediumblob DEFAULT NULL,
  ` + "`created_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`updated_at`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`tag_1`" + ` varchar(255) DEFAULT NULL,
  ` + "`payload_aux`" + ` varchar(3072) DEFAULT NULL,
  ` + "`payload_aux_2`" + ` varchar(3072) DEFAULT NULL,
  ` + "`tag_2`" + ` varchar(255) DEFAULT NULL,
  ` + "`tag_3`" + ` varchar(255) DEFAULT NULL,
  ` + "`flag_1`" + ` tinyint(1) DEFAULT NULL,
  ` + "`event_time_1`" + ` timestamp NULL DEFAULT NULL,
  ` + "`event_time_2`" + ` timestamp NULL DEFAULT NULL,
  ` + "`event_time_3`" + ` timestamp NULL DEFAULT NULL,
  ` + "`event_time_4`" + ` timestamp NULL DEFAULT NULL,
  KEY ` + "`idx_owner_attr_key_1`" + ` (` + "`owner_key`" + `,` + "`attr_key_1`" + `),
  PRIMARY KEY (` + "`id`" + `) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin /*T! SHARD_ROW_ID_BITS=4 */ /*T![ttl] TTL=` + "`updated_at`" + ` + INTERVAL 30 DAY */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='24h' */
`

type WideTableWithJSONWorkload struct {
	entityMediaSize int
	batchAuxSize    int
	batchCbSize     int
	batchMetaSize   int

	payloadMode jsonPayloadMode

	entityMediaInsert string
	entityMediaUpdate string
	batchAuxData      string
	batchCbData       string
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

type jsonPayloadMode uint8

const (
	jsonPayloadModeConst jsonPayloadMode = iota
	jsonPayloadModeZstd
	jsonPayloadModeRandom
)

func parseJSONPayloadMode(mode string) jsonPayloadMode {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "const", "constant":
		return jsonPayloadModeConst
	case "zstd", "zstd-heavy", "zstd_slow", "json":
		return jsonPayloadModeZstd
	case "random", "rand", "incompressible":
		return jsonPayloadModeRandom
	default:
		plog.Warn("unsupported json payload mode, use const", zap.String("payloadMode", mode))
		return jsonPayloadModeConst
	}
}

func NewWideTableWithJSONWorkload(rowSize int, tableCount int, tableStartIndex int, totalRowCount uint64, payloadMode string) schema.Workload {
	if rowSize < 0 {
		rowSize = 0
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	entityMediaSize := min(rowSize, maxEntityMediaMetadataSize)
	if rowSize > maxEntityMediaMetadataSize {
		plog.Warn("row size too large for entity media metadata, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxSize", maxEntityMediaMetadataSize))
	}

	auxSize := min(rowSize/8, maxBatchAuxDataSize)
	cbSize := min(rowSize/8, maxBatchCallbackJobSize)
	metaSize := rowSize - auxSize - cbSize
	if metaSize < 0 {
		metaSize = 0
	}
	if metaSize > maxBatchMetadataSize {
		plog.Warn("row size too large for batch metadata, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxSize", maxBatchMetadataSize+maxBatchAuxDataSize+maxBatchCallbackJobSize))
		metaSize = maxBatchMetadataSize
	}

	perTableUpdateKeySpace := uint64(defaultUpdateKeySpace)
	if totalRowCount > 0 {
		perTableUpdateKeySpace = maxUint64(1, totalRowCount/uint64(tableCount))
	}

	mode := parseJSONPayloadMode(payloadMode)

	w := &WideTableWithJSONWorkload{
		entityMediaSize:        entityMediaSize,
		batchAuxSize:           auxSize,
		batchCbSize:            cbSize,
		batchMetaSize:          metaSize,
		payloadMode:            mode,
		tableStartIndex:        tableStartIndex,
		entitySeq:              make([]atomic.Uint64, tableCount),
		batchSeq:               make([]atomic.Uint64, tableCount),
		perTableUpdateKeySpace: perTableUpdateKeySpace,
	}

	if mode == jsonPayloadModeConst {
		w.entityMediaInsert = strings.Repeat("a", entityMediaSize)
		w.entityMediaUpdate = strings.Repeat("b", entityMediaSize)
		w.batchAuxData = strings.Repeat("c", auxSize)
		w.batchCbData = strings.Repeat("d", cbSize)
		w.batchMetaInsert = newConstBytes(metaSize, 'e')
		w.batchMetaUpdate = newConstBytes(metaSize, 'f')
	}

	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}

	r := w.getRand()
	binary.BigEndian.PutUint32(w.idSuffix[:], r.Uint32())
	w.putRand(r)

	plog.Info("wide table with json workload initialized",
		zap.Int("rowSize", rowSize),
		zap.Int("entityMediaSize", w.entityMediaSize),
		zap.Int("batchAuxDataSize", w.batchAuxSize),
		zap.Int("batchCallbackMetadataSize", w.batchCbSize),
		zap.Int("batchMetadataSize", w.batchMetaSize),
		zap.String("payloadMode", payloadMode),
		zap.Uint64("perTableUpdateKeySpace", w.perTableUpdateKeySpace),
		zap.Int("tableStartIndex", w.tableStartIndex),
		zap.Int("tableSlots", len(w.entitySeq)))
	return w
}

var (
	jsonPayloadAlphaNum = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	jsonPayloadDigits   = []byte("0123456789")

	jsonPayloadChunkPrefix = []byte(`{"key":"`)
	jsonPayloadChunkMid1   = []byte(`","val":"`)
	jsonPayloadChunkMid2   = []byte(`","ts":"`)
	jsonPayloadChunkMid3   = []byte(`","extra":"`)
	jsonPayloadChunkSuffix = []byte("\"}\n")
)

const (
	jsonPayloadKeyLen   = 16
	jsonPayloadValLen   = 48
	jsonPayloadTsLen    = 10
	jsonPayloadExtraLen = 64
)

func fillRandomFromAlphabet(dst []byte, r *rand.Rand, alphabet []byte) {
	if len(dst) == 0 {
		return
	}
	if r == nil {
		for i := range dst {
			dst[i] = alphabet[rand.Intn(len(alphabet))]
		}
		return
	}
	_, _ = r.Read(dst)
	for i, b := range dst {
		dst[i] = alphabet[int(b)%len(alphabet)]
	}
}

func fillZstdHeavyPayload(dst []byte, r *rand.Rand) {
	minChunkLen := len(jsonPayloadChunkPrefix) + jsonPayloadKeyLen +
		len(jsonPayloadChunkMid1) + jsonPayloadValLen +
		len(jsonPayloadChunkMid2) + jsonPayloadTsLen +
		len(jsonPayloadChunkMid3) + jsonPayloadExtraLen +
		len(jsonPayloadChunkSuffix)
	if len(dst) < minChunkLen {
		fillRandomFromAlphabet(dst, r, jsonPayloadAlphaNum)
		return
	}

	pos := 0
	for pos < len(dst) {
		if len(dst)-pos < minChunkLen {
			fillRandomFromAlphabet(dst[pos:], r, jsonPayloadAlphaNum)
			return
		}

		pos += copy(dst[pos:], jsonPayloadChunkPrefix)
		fillRandomFromAlphabet(dst[pos:pos+jsonPayloadKeyLen], r, jsonPayloadAlphaNum)
		pos += jsonPayloadKeyLen

		pos += copy(dst[pos:], jsonPayloadChunkMid1)
		fillRandomFromAlphabet(dst[pos:pos+jsonPayloadValLen], r, jsonPayloadAlphaNum)
		pos += jsonPayloadValLen

		pos += copy(dst[pos:], jsonPayloadChunkMid2)
		fillRandomFromAlphabet(dst[pos:pos+jsonPayloadTsLen], r, jsonPayloadDigits)
		pos += jsonPayloadTsLen

		pos += copy(dst[pos:], jsonPayloadChunkMid3)
		fillRandomFromAlphabet(dst[pos:pos+jsonPayloadExtraLen], r, jsonPayloadAlphaNum)
		pos += jsonPayloadExtraLen

		pos += copy(dst[pos:], jsonPayloadChunkSuffix)
	}
}

func (w *WideTableWithJSONWorkload) entityMediaInsertPayload(r *rand.Rand) string {
	switch w.payloadMode {
	case jsonPayloadModeZstd:
		buf := make([]byte, w.entityMediaSize)
		fillZstdHeavyPayload(buf, r)
		return string(buf)
	case jsonPayloadModeRandom:
		buf := make([]byte, w.entityMediaSize)
		fillRandomFromAlphabet(buf, r, jsonPayloadAlphaNum)
		return string(buf)
	default:
		return w.entityMediaInsert
	}
}

func (w *WideTableWithJSONWorkload) batchAuxPayload(r *rand.Rand) string {
	switch w.payloadMode {
	case jsonPayloadModeZstd:
		buf := make([]byte, w.batchAuxSize)
		fillZstdHeavyPayload(buf, r)
		return string(buf)
	case jsonPayloadModeRandom:
		buf := make([]byte, w.batchAuxSize)
		fillRandomFromAlphabet(buf, r, jsonPayloadAlphaNum)
		return string(buf)
	default:
		return w.batchAuxData
	}
}

func (w *WideTableWithJSONWorkload) batchCallbackPayload(r *rand.Rand) string {
	switch w.payloadMode {
	case jsonPayloadModeZstd:
		buf := make([]byte, w.batchCbSize)
		fillZstdHeavyPayload(buf, r)
		return string(buf)
	case jsonPayloadModeRandom:
		buf := make([]byte, w.batchCbSize)
		fillRandomFromAlphabet(buf, r, jsonPayloadAlphaNum)
		return string(buf)
	default:
		return w.batchCbData
	}
}

func (w *WideTableWithJSONWorkload) batchMetaInsertPayload(r *rand.Rand) []byte {
	switch w.payloadMode {
	case jsonPayloadModeZstd:
		buf := make([]byte, w.batchMetaSize)
		fillZstdHeavyPayload(buf, r)
		return buf
	case jsonPayloadModeRandom:
		buf := make([]byte, w.batchMetaSize)
		_, _ = r.Read(buf)
		return buf
	default:
		return w.batchMetaInsert
	}
}

func (w *WideTableWithJSONWorkload) batchMetaUpdatePayload(r *rand.Rand) []byte {
	switch w.payloadMode {
	case jsonPayloadModeZstd:
		buf := make([]byte, w.batchMetaSize)
		fillZstdHeavyPayload(buf, r)
		return buf
	case jsonPayloadModeRandom:
		buf := make([]byte, w.batchMetaSize)
		_, _ = r.Read(buf)
		return buf
	default:
		return w.batchMetaUpdate
	}
}

func (w *WideTableWithJSONWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *WideTableWithJSONWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func getPrimaryTableName(n int) string {
	if n == 0 {
		return "`wide_table_with_json_primary`"
	}
	return fmt.Sprintf("`wide_table_with_json_primary_%d`", n)
}

func getSecondaryTableName(n int) string {
	if n == 0 {
		return "`wide_table_with_json_secondary`"
	}
	return fmt.Sprintf("`wide_table_with_json_secondary_%d`", n)
}

func (w *WideTableWithJSONWorkload) slot(tableIndex int) int {
	if len(w.entitySeq) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return slot % len(w.entitySeq)
}

func (w *WideTableWithJSONWorkload) newID(kind byte, tableIndex int, seq uint64) string {
	var id uuid.UUID
	id[0] = kind
	binary.BigEndian.PutUint32(id[1:5], uint32(tableIndex))
	var seqBuf [8]byte
	binary.BigEndian.PutUint64(seqBuf[:], seq)
	copy(id[5:12], seqBuf[1:])
	copy(id[12:], w.idSuffix[:])
	return id.String()
}

func (w *WideTableWithJSONWorkload) ownerKey(tableIndex int, seq uint64) int64 {
	return int64(uint64(tableIndex)*1_000_000 + (seq % 1_000_000))
}

func (w *WideTableWithJSONWorkload) attrKey1(seq uint64) string {
	return fmt.Sprintf("key1_%d", seq%attrKeySpace)
}

func (w *WideTableWithJSONWorkload) attrKey2(seq uint64) string {
	return fmt.Sprintf("key2_%d", seq)
}

func (w *WideTableWithJSONWorkload) hashValue(tableIndex int, seq uint64) string {
	return fmt.Sprintf("%016x%016x", uint64(tableIndex), seq)
}

func (w *WideTableWithJSONWorkload) lookupKey(tableIndex int, seq uint64) int64 {
	return int64(uint64(tableIndex)*1_000_000 + seq)
}

func (w *WideTableWithJSONWorkload) BuildCreateTableStatement(n int) string {
	primaryName := getPrimaryTableName(n)
	secondaryName := getSecondaryTableName(n)
	return fmt.Sprintf(createPrimaryTableFormat, primaryName) + ";" + fmt.Sprintf(createSecondaryTableFormat, secondaryName)
}

func (w *WideTableWithJSONWorkload) BuildInsertSql(tableIndex int, batchSize int) string {
	primaryName := getPrimaryTableName(tableIndex)
	mediaExpr := fmt.Sprintf("REPEAT('a',%d)", w.entityMediaSize)
	return fmt.Sprintf("INSERT INTO %s (`updated_at`, `created_at`, `id`, `owner_key`, `attr_key_1`, `attr_key_2`, `group_key`, `hash_value`, `lookup_key`, `payload_text`) VALUES (NOW(), NOW(), UUID(), 1, 'key1_0', 'key2_0', 1, '00000000000000000000000000000000', 1, %s)", primaryName, mediaExpr)
}

func (w *WideTableWithJSONWorkload) BuildInsertSqlWithValues(tableIndex int, batchSize int) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	if r.Float64() < entityInsertRatio {
		return w.buildPrimaryInsertWithValues(tableIndex, batchSize, r)
	}
	return w.buildSecondaryInsertWithValues(tableIndex, batchSize, r)
}

func (w *WideTableWithJSONWorkload) buildPrimaryInsertWithValues(tableIndex int, batchSize int, r *rand.Rand) (string, []interface{}) {
	tableName := getPrimaryTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	includeMigrated := r.Float64() < entityMigratedInsertWeight

	columns := "`updated_at`, `created_at`, `id`, `owner_key`, `attr_key_1`, `attr_key_2`, `group_key`, `hash_value`, `lookup_key`, `payload_text`"
	colCount := 10
	if includeMigrated {
		columns += ", `sync_at`"
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
		ownerKey := w.ownerKey(tableIndex, seq)
		attrKey1 := w.attrKey1(seq)
		attrKey2 := w.attrKey2(seq)
		groupKey := int64(seq % 100_000)
		hashValue := w.hashValue(tableIndex, seq)
		lookupKey := w.lookupKey(tableIndex, seq)

		if includeMigrated {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, ownerKey, attrKey1, attrKey2, groupKey, hashValue, lookupKey, w.entityMediaInsertPayload(r), now)
		} else {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, ownerKey, attrKey1, attrKey2, groupKey, hashValue, lookupKey, w.entityMediaInsertPayload(r))
		}
	}

	sb.WriteString(strings.Join(placeholders, ","))
	return sb.String(), values
}

func (w *WideTableWithJSONWorkload) buildSecondaryInsertWithValues(tableIndex int, batchSize int, r *rand.Rand) (string, []interface{}) {
	tableName := getSecondaryTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	includeAux := r.Float64() < batchInsertWithAuxWeight

	columns := "`created_at`, `updated_at`, `id`, `owner_key`, `attr_key_1`, `state`, `payload_blob`, `tag_1`, `payload_aux_2`"
	colCount := 9
	if includeAux {
		columns = "`created_at`, `updated_at`, `id`, `owner_key`, `attr_key_1`, `state`, `payload_blob`, `tag_1`, `payload_aux`, `payload_aux_2`, `tag_2`, `tag_3`"
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
		ownerKey := w.ownerKey(tableIndex, seq)
		attrKey1 := w.attrKey1(seq)
		state := int16(r.Intn(16))
		tag1 := fmt.Sprintf("tag1_%d", seq%100)

		if includeAux {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, ownerKey, attrKey1, state, w.batchMetaInsertPayload(r), tag1,
				w.batchAuxPayload(r), w.batchCallbackPayload(r), fmt.Sprintf("tag2_%d", seq%100), fmt.Sprintf("tag3_%d", seq%100))
		} else {
			placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?)")
			values = append(values,
				now, now, id, ownerKey, attrKey1, state, w.batchMetaInsertPayload(r), tag1, w.batchCallbackPayload(r))
		}
	}

	sb.WriteString(strings.Join(placeholders, ","))
	return sb.String(), values
}

func (w *WideTableWithJSONWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	primaryName := getPrimaryTableName(opt.TableIndex)
	return fmt.Sprintf("UPDATE %s SET `updated_at` = NOW() WHERE `expire_at` IS NULL LIMIT %d", primaryName, max(1, opt.Batch))
}

func (w *WideTableWithJSONWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	r := w.getRand()
	defer w.putRand(r)

	if r.Float64() < entityUpdateRatio {
		return w.buildPrimaryUpdateWithValues(opt.TableIndex, r)
	}
	return w.buildSecondaryUpdateWithValues(opt.TableIndex, r)
}

func (w *WideTableWithJSONWorkload) buildPrimaryUpdateWithValues(tableIndex int, r *rand.Rand) (string, []interface{}) {
	tableName := getPrimaryTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	upper := minUint64(w.perTableUpdateKeySpace, w.entitySeq[slot].Load())
	seq := randSeq(r, upper)

	switch {
	case r.Float64() < entityUpdateBySecondaryIDWeight:
		lookupKey := w.lookupKey(tableIndex, seq)
		sql := fmt.Sprintf("UPDATE %s SET `expire_at` = ?, `updated_at` = ?, `lookup_key` = ? WHERE (`lookup_key` = ?) AND `expire_at` IS NULL", tableName)
		return sql, []interface{}{nil, now, lookupKey, lookupKey}
	case r.Float64() < entityUpdateBySecondaryIDWeight+entityUpdateByCompositeKeyWeight:
		ownerKey := w.ownerKey(tableIndex, seq)
		attrKey1 := w.attrKey1(seq)
		attrKey2 := w.attrKey2(seq)
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `owner_key` = ?, `hash_value` = ?, `attr_key_2` = ?, `attr_key_1` = ? WHERE (`attr_key_2` = ?) AND (`attr_key_1` = ?) AND (`owner_key` = ?) AND `expire_at` IS NULL", tableName)
		hashValue := w.hashValue(tableIndex, uint64(now.UnixNano()))
		return sql, []interface{}{now, ownerKey, hashValue, attrKey2, attrKey1, attrKey2, attrKey1, ownerKey}
	default:
		id := w.newID('e', tableIndex, seq)
		sql := fmt.Sprintf("UPDATE %s SET `id` = ?, `expire_at` = ?, `updated_at` = ? WHERE (`id` = ?) AND `expire_at` IS NULL", tableName)
		return sql, []interface{}{id, nil, now, id}
	}
}

func (w *WideTableWithJSONWorkload) buildSecondaryUpdateWithValues(tableIndex int, r *rand.Rand) (string, []interface{}) {
	tableName := getSecondaryTableName(tableIndex)
	now := time.Now()
	slot := w.slot(tableIndex)

	upper := minUint64(w.perTableUpdateKeySpace, w.batchSeq[slot].Load())
	seq := randSeq(r, upper)

	id := w.newID('b', tableIndex, seq)
	state := int16(r.Intn(16))

	p := r.Float64()
	switch {
	case p < batchUpdateStatusOnlyWeight:
		sql := fmt.Sprintf("UPDATE %s SET `updated_at` = ?, `state` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{now, state, id}
	case p < batchUpdateStatusOnlyWeight+batchUpdateMetadataWeight:
		sql := fmt.Sprintf("UPDATE %s SET `payload_blob` = ?, `updated_at` = ?, `state` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.batchMetaUpdatePayload(r), now, state, id}
	case p < batchUpdateStatusOnlyWeight+batchUpdateMetadataWeight+batchUpdateMetadataAndKeysWeight:
		ownerKey := w.ownerKey(tableIndex, uint64(now.UnixNano()))
		attrKey1 := w.attrKey1(uint64(now.UnixNano()))
		sql := fmt.Sprintf("UPDATE %s SET `payload_blob` = ?, `updated_at` = ?, `owner_key` = ?, `attr_key_1` = ?, `state` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{w.batchMetaUpdatePayload(r), now, ownerKey, attrKey1, state, id}
	default:
		ownerKey := w.ownerKey(tableIndex, uint64(now.UnixNano()))
		attrKey1 := w.attrKey1(uint64(now.UnixNano()))
		start := now.Add(-time.Minute)
		end := now
		sql := fmt.Sprintf("UPDATE %s SET `event_time_2` = ?, `payload_blob` = ?, `updated_at` = ?, `owner_key` = ?, `event_time_1` = ?, `attr_key_1` = ?, `state` = ? WHERE (`id` = ?)", tableName)
		return sql, []interface{}{end, w.batchMetaUpdatePayload(r), now, ownerKey, start, attrKey1, state, id}
	}
}

func (w *WideTableWithJSONWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	r := w.getRand()
	defer w.putRand(r)

	tableName := getPrimaryTableName(opt.TableIndex)
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
		ownerKey := w.ownerKey(opt.TableIndex, seq)
		attrKey1 := w.attrKey1(seq)
		attrKey2 := w.attrKey2(seq)
		sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE (`attr_key_2` = '%s') AND (`attr_key_1` = '%s') AND (`owner_key` = %d)",
			tableName, attrKey2, attrKey1, ownerKey))
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
