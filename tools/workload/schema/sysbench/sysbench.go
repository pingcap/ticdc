// Copyright 2024 PingCAP, Inc.
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

package sysbench

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"

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
PRIMARY KEY (id),
KEY k_1 (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type SysbenchWorkload struct {
	mu                     sync.RWMutex
	tableUpdateRangesCache map[int]*schema.TableUpdateRangeCache
}

func NewSysbenchWorkload() schema.Workload {
	return &SysbenchWorkload{
		tableUpdateRangesCache: make(map[int]*schema.TableUpdateRangeCache),
	}
}

// BuildCreateTableStatement returns the create-table sql of the table n
func (c *SysbenchWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createTable, n)
}

func (c *SysbenchWorkload) BuildInsertSql(tableN int, batchSize int) string {
	n := rand.Int63()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("insert into sbtest%d (id, k, c, pad) values(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', 'abcdefghijklmnopsrst')", tableN, n, n))

	for r := 1; r < batchSize; r++ {
		n = rand.Int63()
		buf.WriteString(fmt.Sprintf(",(%d, %d, 'abcdefghijklmnopsrstuvwxyzabcd', 'abcdefghijklmnopsrst')", n, n))
	}
	return buf.String()
}

func GetAddIndexStatement(n int) string {
	return fmt.Sprintf("alter table sbtest%d add index k2(k);", n)
}

func (c *SysbenchWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	panic("unimplemented")
}

// BuildUpdateSqlWithConn generates update SQL with connection for range updates
func (c *SysbenchWorkload) BuildUpdateSqlWithConn(conn *sql.Conn, opts schema.UpdateOption) string {
	cache := c.getOrCreateCache(conn, opts.TableIndex, opts)
	if cache == nil {
		return ""
	}

	tableUpdateRange := cache.GetNextTableUpdateRange()
	return c.buildRangeUpdateSQL(opts.TableIndex, tableUpdateRange)
}

// getOrCreateCache gets existing cache or creates a new one with ranges
func (c *SysbenchWorkload) getOrCreateCache(conn *sql.Conn, tableIndex int, opts schema.UpdateOption) *schema.TableUpdateRangeCache {
	// Try to get existing cache
	c.mu.RLock()
	cache := c.tableUpdateRangesCache[tableIndex]
	c.mu.RUnlock()
	if cache != nil {
		return cache
	}

	// Create new cache
	cache = schema.NewTableUpdateRangeCache(opts.RangeNum)

	// Initialize ranges
	if err := c.initializeRanges(conn, cache, tableIndex, opts.Batch); err != nil {
		log.Error("failed to initialize ranges", zap.Error(err))
		return nil
	}

	// Store cache
	c.mu.Lock()
	c.tableUpdateRangesCache[tableIndex] = cache
	c.mu.Unlock()

	return cache
}

// initializeRanges initializes update ranges for the cache
func (c *SysbenchWorkload) initializeRanges(conn *sql.Conn, cache *schema.TableUpdateRangeCache, tableIndex int, batch int) error {
	ids, err := c.fetchSortedIDs(conn, tableIndex, batch*cache.Len())
	if err != nil {
		return err
	}

	tableName := fmt.Sprintf("sbtest%d", tableIndex)

	if len(ids) == 0 {
		log.Warn("no records found in table", zap.String("tableName", tableName))
		return fmt.Errorf("no records found in table %s", tableName)
	}

	c.divideIntoRanges(cache, ids, tableIndex)
	return nil
}

// fetchSortedIDs fetches sorted IDs from the database
func (c *SysbenchWorkload) fetchSortedIDs(conn *sql.Conn, tableIndex, limit int) ([]int, error) {
	query := fmt.Sprintf(`
		SELECT id 
		FROM sbtest%d 
		ORDER BY id 
		LIMIT %d`,
		tableIndex,
		limit)

	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		log.Error("failed to query sorted IDs from DB", zap.Error(err))
		return nil, err
	}
	defer rows.Close()

	tableName := fmt.Sprintf("sbtest%d", tableIndex)

	ids := make([]int, 0, limit)
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			log.Error("failed to scan ID", zap.Error(err))
			return nil, err
		}
		ids = append(ids, id)
	}

	log.Info("query sorted IDs from DB", zap.String("query", query), zap.Int("limit", limit), zap.String("tableName", tableName), zap.Int("rows", len(ids)))

	if len(ids) == 0 {
		log.Panic("no records found in table", zap.String("tableName", tableName))
	}

	return ids, nil
}

// divideIntoRanges divides IDs into ranges and adds them to cache
func (c *SysbenchWorkload) divideIntoRanges(cache *schema.TableUpdateRangeCache, ids []int, tableIndex int) {
	batchSize := len(ids) / cache.Len()
	if batchSize == 0 {
		batchSize = 1
	}

	for i := 0; i < cache.Len(); i++ {
		start := i * batchSize
		end := start + batchSize
		if i == cache.Len()-1 {
			end = len(ids)
		}

		if start >= len(ids) {
			break
		}
		irange := &schema.TableUpdateRange{
			TableIndex: tableIndex,
			Start:      ids[start],
			End:        ids[end-1],
		}
		cache.AddTableUpdateRange(irange)
	}
}

// buildRangeUpdateSQL builds the final update SQL for a range
func (c *SysbenchWorkload) buildRangeUpdateSQL(tableIndex int, updateRange *schema.TableUpdateRange) string {
	n := rand.Int63()
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf(
		"update sbtest%d set k = %d where id between %d and %d;",
		tableIndex,
		n,
		updateRange.Start,
		updateRange.End,
	))
	return buf.String()
}
