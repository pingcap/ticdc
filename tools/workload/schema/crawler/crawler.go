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

package crawler

import (
	"bytes"
	"container/list"
	"encoding/hex"
	"fmt"
	"sync"

	"workload/schema"
	"workload/util"
)

const createContentTable = `
CREATE TABLE contents_%d (
  id varchar(128) NOT NULL,
  path varchar(1024) DEFAULT NULL,
  content mediumblob DEFAULT NULL,
  code int(11) DEFAULT NULL,
  config int(11) DEFAULT NULL,
  col1 mediumtext DEFAULT NULL,
  col2 json DEFAULT NULL,
  col3 json DEFAULT NULL,
  col4 json DEFAULT NULL,
  col5 int(11) DEFAULT NULL,
  col6 int(11) DEFAULT NULL,
  col7 int(11) DEFAULT NULL,
  col8 int(11) DEFAULT NULL,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type CrawlerWorkload struct {
	mu             sync.Mutex
	keys           *list.List
	maxKeyCapacity int
}

func NewCrawlerWorkload() schema.Workload {
	return &CrawlerWorkload{
		keys:           list.New(),
		maxKeyCapacity: 1000000,
	}
}

func (c *CrawlerWorkload) getNewRowKey() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := randomKeyForCrawler()
	if c.keys.Len() >= c.maxKeyCapacity {
		c.keys.Remove(c.keys.Front())
	}
	c.keys.PushBack(key)
	return key
}

func (c *CrawlerWorkload) getExistingRowKey() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keys.Len() == 0 {
		return "", false
	}
	oldKey := c.keys.Remove(c.keys.Front()).(string)
	return oldKey, true
}

// BuildCreateTableStatement returns the create-table sql of the table n
func (c *CrawlerWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createContentTable, n)
}

func (c *CrawlerWorkload) BuildInsertSql(tableN int, batchSize int) string {
	var buf bytes.Buffer
	key := c.getNewRowKey()
	buf.WriteString(fmt.Sprintf("INSERT INTO contents_%d ( "+
		"id, "+
		"path, "+
		"content, "+
		"code, "+
		"config, "+
		"col1, "+
		"col2, "+
		"col3, "+
		"col4, "+
		"col5, "+
		"col6, "+
		"col7, "+
		"col8) VALUES ( "+
		"'%s', 's3://crawler-debug/hello/METADATA/00/00/00/%s-zzzz.com','%s', 200, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
		tableN, key, key, util.GenerateRandomString(1000)))

	for r := 1; r < batchSize; r++ {
		key = c.getNewRowKey()
		buf.WriteString(fmt.Sprintf(", ('%s','s3://crawler-debug/hello/METADATA/00/00/00/%s-zzzz.com', '%s', 200, 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
			key, key, util.GenerateRandomString(1000)))
	}
	insertSQL := buf.String()
	return insertSQL
}

func (c *CrawlerWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	var buf bytes.Buffer
	for i := 0; i < opts.Batch; i++ {
		key, ok := c.getExistingRowKey()
		if !ok {
			break
		}
		buf.WriteString(fmt.Sprintf("UPDATE contents_%d SET content = %s WHERE id = %s;",
			opts.Table, util.GenerateRandomString(1000), key))
	}
	return buf.String()
}

func randomKeyForCrawler() string {
	buffer := make([]byte, 16)
	util.RandomBytes(nil, buffer)
	randomString := hex.EncodeToString(buffer)
	return randomString
}
