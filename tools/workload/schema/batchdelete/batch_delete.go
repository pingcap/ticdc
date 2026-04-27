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

package batchdelete

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"workload/schema"
)

const createBatchDeleteTable = `
CREATE TABLE if not exists batch_delete%d (
id bigint NOT NULL AUTO_INCREMENT,
k bigint NOT NULL,
payload mediumtext NOT NULL,
created_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
updated_at timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
PRIMARY KEY (id),
KEY idx_k (k)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

type BatchDeleteWorkload struct {
	nextK   atomic.Int64
	payload string
}

func NewBatchDeleteWorkload(rowSize int) schema.Workload {
	if rowSize <= 0 {
		rowSize = 1
	}
	return &BatchDeleteWorkload{
		payload: strings.Repeat("x", rowSize),
	}
}

func (b *BatchDeleteWorkload) BuildCreateTableStatement(n int) string {
	return fmt.Sprintf(createBatchDeleteTable, n)
}

func (b *BatchDeleteWorkload) BuildInsertSql(tableN int, batchSize int) string {
	if batchSize <= 0 {
		return ""
	}

	end := b.nextK.Add(int64(batchSize))
	start := end - int64(batchSize) + 1

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO batch_delete%d (k, payload) VALUES ", tableN))
	sb.Grow(batchSize * (len(b.payload) + 64))
	for i := 0; i < batchSize; i++ {
		k := start + int64(i)
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteByte('(')
		sb.WriteString(strconv.FormatInt(k, 10))
		sb.WriteString(", '")
		sb.WriteString(b.payload)
		sb.WriteString("')")
	}
	return sb.String()
}

func (b *BatchDeleteWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	if opts.Batch <= 0 {
		return ""
	}
	return fmt.Sprintf("UPDATE batch_delete%d SET k = k + 1, updated_at = CURRENT_TIMESTAMP(6) ORDER BY id LIMIT %d",
		opts.TableIndex, opts.Batch)
}

func (b *BatchDeleteWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	if opts.Batch <= 0 {
		return ""
	}
	return fmt.Sprintf("DELETE FROM batch_delete%d ORDER BY id LIMIT %d", opts.TableIndex, opts.Batch)
}
