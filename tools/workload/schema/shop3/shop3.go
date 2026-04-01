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

package shop3

import (
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"workload/schema"
)

const shop3PrimaryKeyPayloadMask uint64 = 1<<63 - 1

const createShop3Table = `
CREATE TABLE IF NOT EXISTS shop3_%d (
  ` + "`col1`" + ` bigint(20) NOT NULL,
  ` + "`col2`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ` + "`col3`" + ` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ` + "`col4`" + ` tinyint(1) NOT NULL,
  ` + "`col5`" + ` varbinary(65535) DEFAULT NULL,
  ` + "`col6`" + ` varbinary(1536) DEFAULT NULL,
  ` + "`col7`" + ` varbinary(32767) DEFAULT NULL,
  ` + "`col8`" + ` varbinary(1536) DEFAULT NULL,
  ` + "`col9`" + ` varbinary(1536) DEFAULT NULL,
  ` + "`col10`" + ` varbinary(1536) DEFAULT NULL,
  ` + "`col11`" + ` varbinary(1536) DEFAULT NULL,
  ` + "`col12`" + ` varbinary(1536) DEFAULT NULL,
  PRIMARY KEY (` + "`col1`" + `) /*T![clustered_index] CLUSTERED */,
  UNIQUE KEY ` + "`uk_col9`" + ` (` + "`col9`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type Shop3Workload struct {
	rowSize         int
	tableStartIndex int
	identities      []shop3IdentityTracker
}

type shop3IdentityTracker struct {
	mu         sync.Mutex
	tokens     map[uint64]struct{}
	identities []shop3Identity
}

type shop3Identity struct {
	token  uint64
	nodeID int64
	key    string
}

func NewShop3Workload(_ uint64, rowSize int, tableCount int, tableStartIndex int) schema.Workload {
	if rowSize <= 0 {
		rowSize = 8 * 1024
	}
	if tableCount <= 0 {
		tableCount = 1
	}

	trackers := make([]shop3IdentityTracker, tableCount)
	for idx := range trackers {
		trackers[idx].tokens = make(map[uint64]struct{})
	}

	return &Shop3Workload{
		rowSize:         rowSize,
		tableStartIndex: tableStartIndex,
		identities:      trackers,
	}
}

func tableName(tableIndex int) string {
	return fmt.Sprintf("shop3_%d", tableIndex)
}

func (w *Shop3Workload) BuildCreateTableStatement(tableIndex int) string {
	return fmt.Sprintf(createShop3Table, tableIndex)
}

func (w *Shop3Workload) BuildInsertSql(tableIndex int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}

	var sb strings.Builder
	sb.Grow(batchSize * 512)
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName(tableIndex))
	sb.WriteString(" (`col1`,`col4`,`col5`,`col6`,`col7`,`col8`,`col9`,`col10`,`col11`,`col12`) VALUES ")

	for idx := 0; idx < batchSize; idx++ {
		if idx > 0 {
			sb.WriteString(",")
		}
		identity := w.nextIdentity(tableIndex)
		sb.WriteString("(")
		sb.WriteString(w.generateInsertRow(tableIndex, identity))
		sb.WriteString(")")
	}
	return sb.String()
}

func (w *Shop3Workload) BuildUpdateSql(opt schema.UpdateOption) string {
	if opt.Batch <= 0 {
		return ""
	}

	identity := w.sampleIdentity(opt.TableIndex)
	rowSeed := int64(identity.token & shop3PrimaryKeyPayloadMask)
	col4 := int(rowSeed % 2)
	col5 := quoteLiteral(w.binaryValue(rowSeed, '5', 64, 8192, 2))
	col6 := quoteLiteral(w.binaryValue(rowSeed, '6', 24, 256, 24))
	col7 := quoteLiteral(w.binaryValue(rowSeed, '7', 96, 4096, 1))
	col8 := quoteLiteral(w.binaryValue(rowSeed, '8', 24, 256, 24))
	col10 := quoteLiteral(w.binaryValue(rowSeed, 'a', 24, 256, 24))
	col11 := quoteLiteral(w.binaryValue(rowSeed, 'b', 24, 256, 24))
	col12 := quoteLiteral(w.binaryValue(rowSeed, 'c', 24, 256, 24))

	if rand.Intn(2) == 0 {
		return fmt.Sprintf(
			"UPDATE %s SET `col4` = %d, `col5` = %s, `col7` = %s, `col10` = %s, `col11` = %s WHERE `col1` IN (%s)",
			tableName(opt.TableIndex), col4, col5, col7, col10, col11, w.samplePrimaryKeyList(opt.TableIndex, opt.Batch))
	}

	return fmt.Sprintf(
		"UPDATE %s SET `col4` = %d, `col8` = %s, `col6` = %s, `col12` = %s WHERE `col9` IN (%s)",
		tableName(opt.TableIndex), col4, col8, col6, col12, w.sampleUniqueKeyList(opt.TableIndex, opt.Batch))
}

func (w *Shop3Workload) BuildDeleteSql(opt schema.DeleteOption) string {
	if opt.Batch <= 0 {
		return ""
	}

	if rand.Intn(2) == 0 {
		return fmt.Sprintf("DELETE FROM %s WHERE `col1` IN (%s)", tableName(opt.TableIndex), w.samplePrimaryKeyList(opt.TableIndex, opt.Batch))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE `col9` IN (%s)", tableName(opt.TableIndex), w.sampleUniqueKeyList(opt.TableIndex, opt.Batch))
}

func (w *Shop3Workload) generateInsertRow(tableIndex int, identity shop3Identity) string {
	rowSeed := int64(identity.token & shop3PrimaryKeyPayloadMask)
	values := []string{
		strconv.FormatInt(identity.nodeID, 10),
		strconv.FormatInt(rowSeed%2, 10),
		quoteLiteral(w.binaryValue(rowSeed, '5', 64, 8192, 2)),
		quoteLiteral(w.binaryValue(rowSeed, '6', 24, 256, 24)),
		quoteLiteral(w.binaryValue(rowSeed, '7', 96, 4096, 1)),
		quoteLiteral(w.binaryValue(rowSeed, '8', 24, 256, 24)),
		quoteLiteral(identity.key),
		quoteLiteral(w.binaryValue(rowSeed, 'a', 24, 256, 24)),
		quoteLiteral(w.binaryValue(rowSeed, 'b', 24, 256, 24)),
		quoteLiteral(w.binaryValue(rowSeed, 'c', 24, 256, 24)),
	}
	return strings.Join(values, ",")
}

func (w *Shop3Workload) nextIdentity(tableIndex int) shop3Identity {
	tracker := &w.identities[w.tableSlot(tableIndex)]

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	for {
		token := (uint64(time.Now().UnixNano()) ^ randomUint64()) & shop3PrimaryKeyPayloadMask
		if token == 0 {
			continue
		}
		if _, exists := tracker.tokens[token]; exists {
			continue
		}

		identity := shop3Identity{
			token:  token,
			nodeID: int64(token),
			key:    uniqueKey(tableIndex, token),
		}
		tracker.tokens[token] = struct{}{}
		tracker.identities = append(tracker.identities, identity)
		return identity
	}
}

func (w *Shop3Workload) sampleIdentity(tableIndex int) shop3Identity {
	tracker := &w.identities[w.tableSlot(tableIndex)]

	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if len(tracker.identities) > 0 {
		return tracker.identities[rand.Intn(len(tracker.identities))]
	}

	token := (uint64(time.Now().UnixNano()) ^ randomUint64()) & shop3PrimaryKeyPayloadMask
	if token == 0 {
		token = 1
	}
	return shop3Identity{
		token:  token,
		nodeID: int64(token),
		key:    uniqueKey(tableIndex, token),
	}
}

func (w *Shop3Workload) binaryValue(rowSeed int64, channel byte, minTarget int, maxTarget int, divisor int) string {
	if divisor <= 0 {
		divisor = 1
	}
	target := clampInt(w.rowSize/divisor, minTarget, maxTarget)
	return buildPayload(uint64(rowSeed), channel, target)
}

func (w *Shop3Workload) samplePrimaryKeyList(tableIndex int, batchSize int) string {
	identities := w.sampleIdentities(tableIndex, batchSize)
	values := make([]string, 0, len(identities))
	for _, identity := range identities {
		values = append(values, strconv.FormatInt(identity.nodeID, 10))
	}
	return strings.Join(values, ",")
}

func (w *Shop3Workload) sampleUniqueKeyList(tableIndex int, batchSize int) string {
	identities := w.sampleIdentities(tableIndex, batchSize)
	values := make([]string, 0, len(identities))
	for _, identity := range identities {
		values = append(values, quoteLiteral(identity.key))
	}
	return strings.Join(values, ",")
}

func (w *Shop3Workload) sampleIdentities(tableIndex int, batchSize int) []shop3Identity {
	if batchSize <= 0 {
		return nil
	}

	tracker := &w.identities[w.tableSlot(tableIndex)]
	tracker.mu.Lock()
	defer tracker.mu.Unlock()

	if len(tracker.identities) == 0 {
		token := (uint64(time.Now().UnixNano()) ^ randomUint64()) & shop3PrimaryKeyPayloadMask
		if token == 0 {
			token = 1
		}
		return []shop3Identity{{
			token:  token,
			nodeID: int64(token),
			key:    uniqueKey(tableIndex, token),
		}}
	}

	if len(tracker.identities) <= batchSize {
		out := make([]shop3Identity, len(tracker.identities))
		copy(out, tracker.identities)
		return out
	}

	perm := rand.Perm(len(tracker.identities))
	out := make([]shop3Identity, 0, batchSize)
	for _, idx := range perm[:batchSize] {
		out = append(out, tracker.identities[idx])
	}
	return out
}

func (w *Shop3Workload) tableSlot(tableIndex int) int {
	if len(w.identities) == 0 {
		return 0
	}
	slot := tableIndex - w.tableStartIndex
	if slot < 0 || slot >= len(w.identities) {
		return 0
	}
	return slot
}

func uniqueKey(tableIndex int, token uint64) string {
	mixed := token ^ (uint64(uint32(tableIndex)) << 32) ^ 0x9e3779b97f4a7c15
	return buildPayload(mixed, 'k', 96)
}

func buildPayload(seed uint64, channel byte, length int) string {
	if length <= 0 {
		return ""
	}

	var sb strings.Builder
	sb.Grow(length)

	var input [17]byte
	binary.BigEndian.PutUint64(input[:8], seed)
	input[8] = channel

	for counter := uint64(0); sb.Len() < length; counter++ {
		binary.BigEndian.PutUint64(input[9:], counter)
		sum := sha256.Sum256(input[:])
		chunk := hex.EncodeToString(sum[:])
		remaining := length - sb.Len()
		if remaining >= len(chunk) {
			sb.WriteString(chunk)
			continue
		}
		sb.WriteString(chunk[:remaining])
	}
	return sb.String()
}

func quoteLiteral(value string) string {
	return "'" + value + "'"
}

func clampInt(v, minV, maxV int) int {
	if v < minV {
		return minV
	}
	if v > maxV {
		return maxV
	}
	return v
}

func randomUint64() uint64 {
	var buf [8]byte
	if _, err := cryptorand.Read(buf[:]); err == nil {
		return binary.LittleEndian.Uint64(buf[:])
	}
	return uint64(time.Now().UnixNano()) ^ uint64(rand.Int63())
}
