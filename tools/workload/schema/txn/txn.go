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

package txn

import (
	"bytes"
	"database/sql"
	"fmt"
	"math/rand"
	"sync/atomic"

	"workload/schema"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const createTable = `
CREATE TABLE if not exists sbtest%d (
id bigint NOT NULL,
k bigint NOT NULL DEFAULT '0',
c char(30) NOT NULL DEFAULT '',
pad char(20) NOT NULL DEFAULT '',
commitTs bigint,
startTs bigint DEFAULT NULL,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

const queryCount = `
SELECT count(*) from sbtest%d
`

const (
	padStringLength = 20
	cacheSize       = 100000
)

var (
	cachePadString = make(map[int]string)
	cacheIdx       atomic.Int64
)

// InitPadStringCache initializes the cache with random pad strings
func InitPadStringCache() {
	for i := 0; i < cacheSize; i++ {
		cachePadString[i] = genRandomPadString(padStringLength)
	}
	log.Info("Initialized pad string cache",
		zap.Int("cacheSize", cacheSize),
		zap.Int("stringLength", padStringLength))
}

func genRandomPadString(length int) string {
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = byte(rand.Intn(26) + 97)
	}
	return string(buf)
}

func getPadString() string {
	// Get a random index from the cache
	idx := cacheIdx.Add(1) % int64(cacheSize)
	return cachePadString[int(idx)]
}

type TxnWorkload struct {
	tableCount    int
	startIndex    int
	totalRowCount uint64
	tableMap      map[int]*atomic.Int64
	crossTable    bool
	value         *atomic.Int64
}

func NewTxnWorkload(totalRowCount uint64, tableCount, startIndex int, crossTable bool) schema.Workload {
	return &TxnWorkload{
		totalRowCount: totalRowCount,
		tableCount:    tableCount,
		startIndex:    startIndex,
		crossTable:    crossTable,
		tableMap:      make(map[int]*atomic.Int64),
		value:         &atomic.Int64{},
	}
}

func (t *TxnWorkload) Prepare(db *sql.DB) {
	var num int64
	for i := 0; i < t.tableCount; i++ {
		n := i + t.startIndex
		t.tableMap[n] = &atomic.Int64{}
		row := db.QueryRow(fmt.Sprintf(queryCount, n))
		row.Scan(&num)
		t.tableMap[n].Store(num)
	}
}

// BuildCreateTableStatement returns the create-table sql for both Data and index_Data tables
func (t *TxnWorkload) BuildCreateTableStatement(n int) string {
	t.tableMap[n] = &atomic.Int64{}
	return fmt.Sprintf(createTable, n)
}

func (t *TxnWorkload) BuildInsertSql(tableN int, batchSize int) string {
	var buf bytes.Buffer
	buf.WriteString("begin;")
	tso := t.value.Add(1)
	for r := 0; r < batchSize; r++ {
		if t.crossTable {
			tableN = rand.Intn(t.tableCount) + t.startIndex
		}
		idx := t.tableMap[tableN]
		id := idx.Add(1)
		buf.WriteString(fmt.Sprintf("insert into sbtest%d (id, k, c, pad, commitTs) values(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', '%s', '%d');",
			tableN, id, rand.Int63(), getPadString(), tso))
	}
	buf.WriteString("commit;")
	return buf.String()
}

func (t *TxnWorkload) BuildUpdateSql(updateOption schema.UpdateOption) string {
	batchSize := updateOption.Batch
	tableN := updateOption.TableIndex
	var buf bytes.Buffer
	buf.WriteString("begin;")
	// conflict when only update because the value maybe duplicated with other row
	tso := t.value.Add(1)
	for r := 0; r < batchSize; r++ {
		if t.crossTable {
			tableN = rand.Intn(t.tableCount) + t.startIndex
		}
		idx, ok := t.tableMap[tableN]
		if !ok {
			log.Panic("failed to load table", zap.Any("tableN", tableN))
		}
		id := rand.Int63n(idx.Load()) + 1
		buf.WriteString(fmt.Sprintf("update sbtest%d set pad = '%s', commitTs = '%d' where id = '%d';",
			tableN, getPadString(), id, tso))
	}
	buf.WriteString("commit;")
	return buf.String()
}
