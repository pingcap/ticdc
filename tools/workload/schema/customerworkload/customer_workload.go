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

package customerworkload

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
	modelA = "A"
	modelB = "B"
	modelC = "C"
	modelD = "D"

	defaultPayloadPoolSize = 64
	defaultKeyspace        = 1_000_000

	maxTextPayloadSize = 10_000
	maxJSONPaddingSize = 8_000
	maxAuxPayloadSize  = 16 * 1024
)

type primaryKeyKind int

const (
	singleColumnPrimaryKey primaryKeyKind = iota
	compositePrimaryKey
)

// Options controls the anonymized customer workload.
type Options struct {
	Model           string
	TableCount      int
	TableStartIndex int
	TotalRowCount   uint64
	RowSize         int
	KeyspaceSize    uint64
	InitialSeq      uint64
}

type tableSpec struct {
	prefix      string
	keyKind     primaryKeyKind
	payloadSize int
}

type rowPayload struct {
	text string
	json string
	blob []byte
	aux  []byte
}

// CustomerWorkload generates anonymized workloads derived from customer traffic
// models. It intentionally avoids real customer, table, and column names.
type CustomerWorkload struct {
	model           string
	tableStartIndex int
	tableSlots      []tableSpec
	tableSeq        []atomic.Uint64
	keyspaceSize    uint64

	payloadPools map[string][]rowPayload

	seed     atomic.Int64
	randPool sync.Pool
}

// NewCustomerWorkload creates an anonymized customer workload.
func NewCustomerWorkload(opts Options) schema.Workload {
	model := normalizeModel(opts.Model)
	tableCount := opts.TableCount
	if tableCount <= 0 {
		tableCount = 1
	}

	tableSlots := buildTableSlots(model, opts.RowSize)
	if len(tableSlots) == 0 {
		tableSlots = buildTableSlots(modelA, opts.RowSize)
	}

	keyspaceSize := opts.KeyspaceSize
	if keyspaceSize == 0 {
		keyspaceSize = defaultKeyspace
		if opts.TotalRowCount > 0 {
			keyspaceSize = maxUint64(1, opts.TotalRowCount/uint64(tableCount))
		}
	}

	w := &CustomerWorkload{
		model:           model,
		tableStartIndex: opts.TableStartIndex,
		tableSlots:      tableSlots,
		tableSeq:        make([]atomic.Uint64, tableCount),
		keyspaceSize:    keyspaceSize,
		payloadPools:    make(map[string][]rowPayload),
	}
	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}
	for i := range w.tableSeq {
		w.tableSeq[i].Store(opts.InitialSeq)
	}

	r := rand.New(rand.NewSource(w.seed.Add(1)))
	for _, spec := range uniqueSpecs(tableSlots) {
		w.payloadPools[spec.prefix] = buildPayloadPool(spec, r)
	}

	plog.Info("customer workload initialized",
		zap.String("model", model),
		zap.Int("tableSlots", len(tableSlots)),
		zap.Int("tableCount", tableCount),
		zap.Int("tableStartIndex", opts.TableStartIndex),
		zap.Uint64("keyspaceSize", keyspaceSize),
		zap.Uint64("initialSeq", opts.InitialSeq))

	return w
}

func normalizeModel(model string) string {
	switch strings.ToUpper(strings.TrimSpace(model)) {
	case "", modelA, "MODEL-A", "CATALOG":
		return modelA
	case modelB, "MODEL-B", "EVENT":
		return modelB
	case modelC, "MODEL-C", "GRAPH":
		return modelC
	case modelD, "MODEL-D", "INGEST":
		return modelD
	default:
		plog.Warn("unknown customer workload model, fallback to model A", zap.String("model", model))
		return modelA
	}
}

func buildTableSlots(model string, rowSizeOverride int) []tableSpec {
	withOverride := func(defaultSize int) int {
		if rowSizeOverride > 0 {
			return rowSizeOverride
		}
		return defaultSize
	}

	repeat := func(spec tableSpec, n int) []tableSpec {
		result := make([]tableSpec, 0, n)
		for range n {
			result = append(result, spec)
		}
		return result
	}

	switch model {
	case modelA:
		var slots []tableSpec
		// The 27-slot shape approximates the observed Model A row mix derived
		// from 286K rows/s and 6.66GB/s: large ~=32%, blended ~=32%,
		// json/blob ~=26%, compact ~=9%.
		slots = append(slots, repeat(tableSpec{"catalog_large", singleColumnPrimaryKey, withOverride(37 * 1024)}, 9)...)
		slots = append(slots, repeat(tableSpec{"catalog_blended", singleColumnPrimaryKey, withOverride(20 * 1024)}, 9)...)
		slots = append(slots, repeat(tableSpec{"catalog_json", singleColumnPrimaryKey, withOverride(17 * 1024)}, 7)...)
		slots = append(slots, tableSpec{"catalog_compact_a", singleColumnPrimaryKey, withOverride(2300)})
		slots = append(slots, tableSpec{"catalog_compact_b", singleColumnPrimaryKey, withOverride(1100)})
		return slots
	case modelB:
		var slots []tableSpec
		slots = append(slots, repeat(tableSpec{"event_log", singleColumnPrimaryKey, withOverride(8300)}, 7)...)
		slots = append(slots, tableSpec{"event_meta", singleColumnPrimaryKey, withOverride(3300)})
		return slots
	case modelC:
		return []tableSpec{{"graph_node", singleColumnPrimaryKey, withOverride(5100)}}
	case modelD:
		var slots []tableSpec
		slots = append(slots, repeat(tableSpec{"object_main", compositePrimaryKey, withOverride(7500)}, 10)...)
		slots = append(slots, tableSpec{"object_owner", compositePrimaryKey, withOverride(3300)})
		slots = append(slots, tableSpec{"object_edge", compositePrimaryKey, withOverride(3100)})
		slots = append(slots, tableSpec{"object_link", compositePrimaryKey, withOverride(2600)})
		slots = append(slots, tableSpec{"object_video_edge", compositePrimaryKey, withOverride(3300)})
		return slots
	default:
		return nil
	}
}

func uniqueSpecs(specs []tableSpec) []tableSpec {
	seen := make(map[string]struct{}, len(specs))
	result := make([]tableSpec, 0, len(specs))
	for _, spec := range specs {
		if _, ok := seen[spec.prefix]; ok {
			continue
		}
		seen[spec.prefix] = struct{}{}
		result = append(result, spec)
	}
	return result
}

func buildPayloadPool(spec tableSpec, r *rand.Rand) []rowPayload {
	pool := make([]rowPayload, 0, defaultPayloadPoolSize)
	for i := 0; i < defaultPayloadPoolSize; i++ {
		pool = append(pool, buildPayload(spec, i, r))
	}
	return pool
}

func buildPayload(spec tableSpec, salt int, r *rand.Rand) rowPayload {
	size := maxInt(256, spec.payloadSize)

	textSize := minInt(maxTextPayloadSize, size/4)
	jsonPaddingSize := minInt(maxJSONPaddingSize, size/5)
	auxSize := minInt(maxAuxPayloadSize, size/8)
	blobSize := size - textSize - jsonPaddingSize - auxSize
	if blobSize < 128 {
		blobSize = 128
	}

	text := randomString(r, textSize)
	jsonPayload := fmt.Sprintf(`{"kind":"%s","salt":%d,"pad":"%s"}`,
		spec.prefix, salt, randomString(r, jsonPaddingSize))
	blob := randomBytes(r, blobSize)
	aux := randomBytes(r, auxSize)

	return rowPayload{
		text: text,
		json: jsonPayload,
		blob: blob,
		aux:  aux,
	}
}

func (w *CustomerWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *CustomerWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func (w *CustomerWorkload) specForTable(tableIndex int) tableSpec {
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return w.tableSlots[slot%len(w.tableSlots)]
}

func (w *CustomerWorkload) seqSlot(tableIndex int) int {
	slot := tableIndex - w.tableStartIndex
	if slot < 0 {
		slot = -slot
	}
	return slot % len(w.tableSeq)
}

func (w *CustomerWorkload) tableName(tableIndex int) string {
	spec := w.specForTable(tableIndex)
	return fmt.Sprintf("`%s_%d`", spec.prefix, tableIndex)
}

func (w *CustomerWorkload) choosePayload(spec tableSpec, r *rand.Rand) rowPayload {
	pool := w.payloadPools[spec.prefix]
	return pool[r.Intn(len(pool))]
}

func (w *CustomerWorkload) randSeq(tableIndex int, r *rand.Rand) uint64 {
	slot := w.seqSlot(tableIndex)
	upper := minUint64(w.keyspaceSize, maxUint64(1, w.tableSeq[slot].Load()))
	if upper <= 1 {
		return 1
	}
	if upper <= uint64(math.MaxInt64) {
		return uint64(r.Int63n(int64(upper))) + 1
	}
	return (r.Uint64() % upper) + 1
}

func keyParts(tableIndex int, seq uint64) (uint64, uint64, string) {
	entityID := uint64(tableIndex)*1_000_000_000 + seq
	bucketID := seq % 1024
	sequenceNo := fmt.Sprintf("%d", seq)
	return entityID, bucketID, sequenceNo
}

func lookupKey(tableIndex int, seq uint64) []byte {
	return []byte(fmt.Sprintf("lk_%d_%d", tableIndex, seq))
}

func (w *CustomerWorkload) BuildCreateTableStatement(tableIndex int) string {
	spec := w.specForTable(tableIndex)
	tableName := w.tableName(tableIndex)

	primaryKey := "`entity_id`"
	if spec.keyKind == compositePrimaryKey {
		primaryKey = "`entity_id`,`bucket_id`,`sequence_no`"
	}

	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  `+"`entity_id`"+` bigint unsigned NOT NULL,
  `+"`bucket_id`"+` bigint unsigned NOT NULL DEFAULT 0,
  `+"`sequence_no`"+` decimal(38,0) NOT NULL DEFAULT 0,
  `+"`lookup_key`"+` varbinary(255) DEFAULT NULL,
  `+"`group_key`"+` bigint unsigned DEFAULT NULL,
  `+"`state_code`"+` int DEFAULT NULL,
  `+"`version_no`"+` bigint unsigned NOT NULL DEFAULT 0,
  `+"`payload_text`"+` varchar(10000) DEFAULT NULL,
  `+"`payload_json`"+` json DEFAULT NULL,
  `+"`payload_blob`"+` mediumblob DEFAULT NULL,
  `+"`aux_blob`"+` blob DEFAULT NULL,
  `+"`created_at`"+` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `+"`updated_at`"+` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (%s) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY `+"`uniq_lookup_key`"+` (`+"`lookup_key`"+`),
  KEY `+"`idx_group_sequence`"+` (`+"`group_key`"+`,`+"`sequence_no`"+`,`+"`entity_id`"+`),
  KEY `+"`idx_updated_at`"+` (`+"`updated_at`"+`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`, tableName, primaryKey)
}

func (w *CustomerWorkload) BuildInsertSql(tableIndex int, batchSize int) string {
	sql, _ := w.BuildInsertSqlWithValues(tableIndex, batchSize)
	return sql
}

func (w *CustomerWorkload) BuildInsertSqlWithValues(tableIndex int, batchSize int) (string, []interface{}) {
	if batchSize <= 0 {
		batchSize = 1
	}

	spec := w.specForTable(tableIndex)
	tableName := w.tableName(tableIndex)
	slot := w.seqSlot(tableIndex)
	now := time.Now()
	r := w.getRand()
	defer w.putRand(r)

	const colCount = 13
	values := make([]interface{}, 0, batchSize*colCount)
	placeholders := make([]string, 0, batchSize)

	for i := 0; i < batchSize; i++ {
		seq := w.tableSeq[slot].Add(1)
		entityID, bucketID, sequenceNo := keyParts(tableIndex, seq)
		payload := w.choosePayload(spec, r)
		placeholders = append(placeholders, "(?,?,?,?,?,?,?,?,?,?,?,?,?)")
		values = append(values,
			entityID,
			bucketID,
			sequenceNo,
			lookupKey(tableIndex, seq),
			entityID%100_000,
			int(seq%32),
			seq,
			payload.text,
			payload.json,
			payload.blob,
			payload.aux,
			now,
			now,
		)
	}

	return fmt.Sprintf("INSERT INTO %s (`entity_id`,`bucket_id`,`sequence_no`,`lookup_key`,`group_key`,`state_code`,`version_no`,`payload_text`,`payload_json`,`payload_blob`,`aux_blob`,`created_at`,`updated_at`) VALUES %s ON DUPLICATE KEY UPDATE `group_key`=VALUES(`group_key`),`state_code`=VALUES(`state_code`),`version_no`=VALUES(`version_no`),`payload_text`=VALUES(`payload_text`),`payload_json`=VALUES(`payload_json`),`payload_blob`=VALUES(`payload_blob`),`aux_blob`=VALUES(`aux_blob`),`updated_at`=VALUES(`updated_at`)",
		tableName, strings.Join(placeholders, ",")), values
}

func (w *CustomerWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	sql, _ := w.BuildUpdateSqlWithValues(opt)
	return sql
}

func (w *CustomerWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	spec := w.specForTable(opt.TableIndex)
	tableName := w.tableName(opt.TableIndex)
	r := w.getRand()
	defer w.putRand(r)

	seq := w.randSeq(opt.TableIndex, r)
	entityID, bucketID, sequenceNo := keyParts(opt.TableIndex, seq)
	payload := w.choosePayload(spec, r)
	now := time.Now()

	if spec.keyKind == compositePrimaryKey {
		return fmt.Sprintf("UPDATE %s SET `version_no`=?,`state_code`=?,`payload_text`=?,`payload_json`=?,`payload_blob`=?,`aux_blob`=?,`updated_at`=? WHERE `entity_id`=? AND `bucket_id`=? AND `sequence_no`=?",
				tableName),
			[]interface{}{uint64(now.UnixNano()), int(seq % 32), payload.text, payload.json, payload.blob, payload.aux, now, entityID, bucketID, sequenceNo}
	}

	return fmt.Sprintf("UPDATE %s SET `version_no`=?,`state_code`=?,`payload_text`=?,`payload_json`=?,`payload_blob`=?,`aux_blob`=?,`updated_at`=? WHERE `entity_id`=?",
			tableName),
		[]interface{}{uint64(now.UnixNano()), int(seq % 32), payload.text, payload.json, payload.blob, payload.aux, now, entityID}
}

func (w *CustomerWorkload) BuildDeleteSql(opt schema.DeleteOption) string {
	sql, _ := w.BuildDeleteSqlWithValues(opt)
	return sql
}

func (w *CustomerWorkload) BuildDeleteSqlWithValues(opt schema.DeleteOption) (string, []interface{}) {
	batchSize := opt.Batch
	if batchSize <= 0 {
		batchSize = 1
	}

	spec := w.specForTable(opt.TableIndex)
	tableName := w.tableName(opt.TableIndex)
	r := w.getRand()
	defer w.putRand(r)

	if spec.keyKind == compositePrimaryKey {
		placeholders := make([]string, 0, batchSize)
		values := make([]interface{}, 0, batchSize*3)
		for i := 0; i < batchSize; i++ {
			seq := w.randSeq(opt.TableIndex, r)
			entityID, bucketID, sequenceNo := keyParts(opt.TableIndex, seq)
			placeholders = append(placeholders, "(?,?,?)")
			values = append(values, entityID, bucketID, sequenceNo)
		}
		return fmt.Sprintf("DELETE FROM %s WHERE (`entity_id`,`bucket_id`,`sequence_no`) IN (%s)",
			tableName, strings.Join(placeholders, ",")), values
	}

	placeholders := make([]string, 0, batchSize)
	values := make([]interface{}, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		seq := w.randSeq(opt.TableIndex, r)
		entityID, _, _ := keyParts(opt.TableIndex, seq)
		placeholders = append(placeholders, "?")
		values = append(values, entityID)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE `entity_id` IN (%s)",
		tableName, strings.Join(placeholders, ",")), values
}

var payloadAlphabet = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(r *rand.Rand, size int) string {
	if size <= 0 {
		return ""
	}
	buf := randomBytes(r, size)
	for i, b := range buf {
		buf[i] = payloadAlphabet[int(b)%len(payloadAlphabet)]
	}
	return string(buf)
}

func randomBytes(r *rand.Rand, size int) []byte {
	if size <= 0 {
		return nil
	}
	buf := make([]byte, size)
	_, _ = r.Read(buf)
	return buf
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a, b int) int {
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
