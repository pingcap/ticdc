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

package schema

import (
	"bytes"
	"container/list"
	"encoding/hex"
	"fmt"
	"sync"
)

const createContentTable = `
CREATE TABLE content_crawled_%d (
  content_id varchar(128) NOT NULL,
  html mediumtext DEFAULT NULL,
  visual_representation mediumblob DEFAULT NULL,
  microdata json DEFAULT NULL,
  opengraph json DEFAULT NULL,
  meta_tags json DEFAULT NULL,
  status_code int(11) DEFAULT NULL,
  crawler_config_id int(11) DEFAULT NULL,
  country int(11) DEFAULT NULL,
  language int(11) DEFAULT NULL,
  updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  html_s3_path varchar(1024) DEFAULT NULL,
  html_compression_algorithm int(11) DEFAULT NULL,
  currency int(11) DEFAULT NULL,
  PRIMARY KEY (content_id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type CrawlerWorkload struct {
	mu             sync.Mutex
	keys           *list.List
	maxKeyCapacity int
}

func NewCrawlerWorkload() Workload {
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
	buf.WriteString(fmt.Sprintf("INSERT INTO web_content_prod.content_crawled_%d ( "+
		"content_id, "+
		"html, "+
		"html_s3_path, "+
		"html_compression_algorithm, "+
		"visual_representation, "+
		"microdata, "+
		"opengraph, "+
		"meta_tags, "+
		"status_code, "+
		"crawler_config_id, "+
		"language, "+
		"country, "+
		"currency) VALUES ( "+
		"%s, NULL, 's3://crawler-debug/hello/METADATA/00/00/00/%s-zzzz.com', NULL, %s, NULL, NULL, NULL, 200, 1, NULL, NULL, NULL)",
		tableN, key, key, randomContentForCrawler()))

	for r := 1; r < batchSize; r++ {
		key = c.getNewRowKey()
		buf.WriteString(fmt.Sprintf(", (%s, NULL, 's3://crawler-debug/hello/METADATA/00/00/00/%s-zzzz.com', NULL, %s, NULL, NULL, NULL, 200, 1, NULL, NULL, NULL))",
			key, key, randomContentForCrawler()))
	}
	return buf.String()
}

func (c *CrawlerWorkload) BuildUpdateSql(opts UpdateOption) string {
	var buf bytes.Buffer
	for i := 0; i < opts.Batch; i++ {
		key, ok := c.getExistingRowKey()
		if !ok {
			break
		}
		buf.WriteString(fmt.Sprintf("UPDATE web_content_prod.content_crawled_%d SET html = %s WHERE content_id = %s;",
			opts.Table, randomContentForCrawler(), key))
	}
	return buf.String()
}

func randomKeyForCrawler() string {
	buffer := make([]byte, 16)
	randomBytes(nil, buffer)
	randomString := hex.EncodeToString(buffer)
	return randomString
}

func randomContentForCrawler() string {
	buffer := make([]byte, 20480)
	randomBytes(nil, buffer)
	randomString := hex.EncodeToString(buffer)
	return randomString
}
