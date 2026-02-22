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

package forwardindex

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	plog "github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	"workload/util"
)

const (
	pinPromotionIDSize  = 32
	pinPromotionIDSeed  = 24
	maxBlobSize         = 65535
	maxMediumBlobSize   = 16777215
	defaultUpdateRanges = 1_000_000
)

const createStagingForwardIndexTableFormat = `
CREATE TABLE IF NOT EXISTS %s (
  g_pin_promotion_id VARBINARY(32) NOT NULL,
  ad_doc MEDIUMBLOB DEFAULT NULL,
  debug_info BLOB DEFAULT NULL,
  debug_info_history BLOB DEFAULT NULL,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (g_pin_promotion_id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
/*T![ttl] TTL=updated_at + INTERVAL 1209600 SECOND */ /*T![ttl] TTL_ENABLE='ON' */ /*T![ttl] TTL_JOB_INTERVAL='1h' */
`

func getTableName(n int) string {
	if n == 0 {
		return "staging_forward_index"
	}
	return fmt.Sprintf("staging_forward_index_%d", n)
}

type StagingForwardIndexWorkload struct {
	debugInfoSize        int
	debugInfoHistorySize int
	adDocSize            int

	updateKeySpace uint64

	insertSeq atomic.Uint64

	idSeed [pinPromotionIDSeed]byte

	seed     atomic.Int64
	randPool sync.Pool
}

func NewStagingForwardIndexWorkload(rowSize int, updateKeySpace uint64) schema.Workload {
	if rowSize < 0 {
		rowSize = 0
	}

	maxRowSize := maxMediumBlobSize + 2*maxBlobSize
	if rowSize > maxRowSize {
		plog.Warn("row size too large for schema, use max supported size",
			zap.Int("rowSize", rowSize),
			zap.Int("maxRowSize", maxRowSize))
		rowSize = maxRowSize
	}

	if updateKeySpace == 0 {
		updateKeySpace = defaultUpdateRanges
	}

	baseShare := rowSize / 3
	debugInfoSize := min(baseShare, maxBlobSize)
	debugInfoHistorySize := min(baseShare, maxBlobSize)
	adDocSize := rowSize - debugInfoSize - debugInfoHistorySize
	if adDocSize > maxMediumBlobSize {
		plog.Warn("ad doc size too large for mediumblob, use max supported size",
			zap.Int("adDocSize", adDocSize),
			zap.Int("maxAdDocSize", maxMediumBlobSize))
		adDocSize = maxMediumBlobSize
	}

	w := &StagingForwardIndexWorkload{
		debugInfoSize:        debugInfoSize,
		debugInfoHistorySize: debugInfoHistorySize,
		adDocSize:            adDocSize,
		updateKeySpace:       updateKeySpace,
	}
	w.seed.Store(time.Now().UnixNano())
	w.randPool.New = func() any {
		return rand.New(rand.NewSource(w.seed.Add(1)))
	}

	r := w.getRand()
	util.RandomBytes(r, w.idSeed[:])
	w.putRand(r)

	plog.Info("staging forward index workload initialized",
		zap.Int("rowSize", rowSize),
		zap.Int("debugInfoSize", w.debugInfoSize),
		zap.Int("debugInfoHistorySize", w.debugInfoHistorySize),
		zap.Int("adDocSize", w.adDocSize),
		zap.Uint64("updateKeySpace", w.updateKeySpace))

	return w
}

func (w *StagingForwardIndexWorkload) getRand() *rand.Rand {
	return w.randPool.Get().(*rand.Rand)
}

func (w *StagingForwardIndexWorkload) putRand(r *rand.Rand) {
	w.randPool.Put(r)
}

func (w *StagingForwardIndexWorkload) fillPromotionID(dst []byte, seq uint64) {
	copy(dst[:pinPromotionIDSeed], w.idSeed[:])
	binary.BigEndian.PutUint64(dst[pinPromotionIDSeed:], seq)
}

func (w *StagingForwardIndexWorkload) newPayload(r *rand.Rand) (debugInfo, debugInfoHistory, adDoc []byte) {
	payloadSize := w.debugInfoSize + w.debugInfoHistorySize + w.adDocSize
	if payloadSize == 0 {
		return nil, nil, nil
	}

	buf := make([]byte, payloadSize)
	util.RandomBytes(r, buf)

	debugInfo = buf[:w.debugInfoSize]
	debugInfoHistory = buf[w.debugInfoSize : w.debugInfoSize+w.debugInfoHistorySize]
	adDoc = buf[w.debugInfoSize+w.debugInfoHistorySize:]
	return debugInfo, debugInfoHistory, adDoc
}

func (w *StagingForwardIndexWorkload) BuildCreateTableStatement(n int) string {
	tableName := getTableName(n)
	return fmt.Sprintf(createStagingForwardIndexTableFormat, tableName)
}

func (w *StagingForwardIndexWorkload) BuildInsertSql(tableN int, batchSize int) string {
	tableName := getTableName(tableN)
	debugInfoExpr := fmt.Sprintf("REPEAT('a',%d)", w.debugInfoSize)
	debugInfoHistoryExpr := fmt.Sprintf("REPEAT('b',%d)", w.debugInfoHistorySize)
	adDocExpr := fmt.Sprintf("REPEAT('c',%d)", w.adDocSize)
	idExpr := "UNHEX(CONCAT(REPLACE(UUID(),'-',''),REPLACE(UUID(),'-','')))"

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (debug_info, debug_info_history, g_pin_promotion_id, ad_doc) VALUES ")

	for i := 0; i < batchSize; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(")
		sb.WriteString(debugInfoExpr)
		sb.WriteString(",")
		sb.WriteString(debugInfoHistoryExpr)
		sb.WriteString(",")
		sb.WriteString(idExpr)
		sb.WriteString(",")
		sb.WriteString(adDocExpr)
		sb.WriteString(")")
	}

	sb.WriteString(" ON DUPLICATE KEY UPDATE debug_info=VALUES(debug_info), debug_info_history=VALUES(debug_info_history), ad_doc=VALUES(ad_doc)")
	return sb.String()
}

func (w *StagingForwardIndexWorkload) BuildInsertSqlWithValues(tableN int, batchSize int) (string, []interface{}) {
	tableName := getTableName(tableN)
	r := w.getRand()
	debugInfo, debugInfoHistory, adDoc := w.newPayload(r)
	w.putRand(r)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (debug_info, debug_info_history, g_pin_promotion_id, ad_doc) VALUES ")

	placeholders := make([]string, 0, batchSize)
	values := make([]interface{}, 0, batchSize*4)

	for range batchSize {
		placeholders = append(placeholders, "(?,?,?,?)")

		id := make([]byte, pinPromotionIDSize)
		seq := w.insertSeq.Add(1)
		w.fillPromotionID(id, seq)
		values = append(values, debugInfo, debugInfoHistory, id, adDoc)
	}

	sb.WriteString(strings.Join(placeholders, ","))
	sb.WriteString(" ON DUPLICATE KEY UPDATE debug_info=VALUES(debug_info), debug_info_history=VALUES(debug_info_history), ad_doc=VALUES(ad_doc)")
	return sb.String(), values
}

func (w *StagingForwardIndexWorkload) BuildUpdateSql(opt schema.UpdateOption) string {
	return w.BuildInsertSql(opt.TableIndex, opt.Batch)
}

func (w *StagingForwardIndexWorkload) BuildUpdateSqlWithValues(opt schema.UpdateOption) (string, []interface{}) {
	tableName := getTableName(opt.TableIndex)

	r := w.getRand()
	debugInfo, debugInfoHistory, adDoc := w.newPayload(r)

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (debug_info, debug_info_history, g_pin_promotion_id, ad_doc) VALUES ")

	placeholders := make([]string, 0, opt.Batch)
	values := make([]interface{}, 0, opt.Batch*4)

	for range opt.Batch {
		placeholders = append(placeholders, "(?,?,?,?)")

		seq := (uint64(r.Int63()) % w.updateKeySpace) + 1
		id := make([]byte, pinPromotionIDSize)
		w.fillPromotionID(id, seq)
		values = append(values, debugInfo, debugInfoHistory, id, adDoc)
	}
	w.putRand(r)

	sb.WriteString(strings.Join(placeholders, ","))
	sb.WriteString(" ON DUPLICATE KEY UPDATE debug_info=VALUES(debug_info), debug_info_history=VALUES(debug_info_history), ad_doc=VALUES(ad_doc)")
	return sb.String(), values
}

func (w *StagingForwardIndexWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	tableName := getTableName(opts.TableIndex)
	r := w.getRand()
	defer w.putRand(r)

	var sb strings.Builder
	for i := 0; i < opts.Batch; i++ {
		if i > 0 {
			sb.WriteString(";")
		}
		seq := (uint64(r.Int63()) % w.updateKeySpace) + 1
		id := make([]byte, pinPromotionIDSize)
		w.fillPromotionID(id, seq)
		sb.WriteString(fmt.Sprintf("DELETE FROM %s WHERE g_pin_promotion_id = 0x%x", tableName, id))
	}
	return sb.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
