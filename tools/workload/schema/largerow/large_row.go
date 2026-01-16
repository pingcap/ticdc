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

package largerow

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"workload/schema"
	"workload/util"
)

const varcharColumnMaxLen = 16383

var maxValue int64 = 9223372036854775807

func newColumnValues(r *rand.Rand, size, count int) [][]byte {
	result := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		buf := make([]byte, size)
		util.RandomBytes(r, buf)
		result = append(result, buf)
	}
	return result
}

func newRowValues(r *rand.Rand, columnSize int, columnCount int, batchSize int) []string {
	const numColumnValues = 512
	columns := newColumnValues(r, columnSize, numColumnValues)

	result := make([]string, 0, batchSize)

	var sb strings.Builder
	for i := 0; i < batchSize; i++ {
		sb.Reset()

		for j := 0; j < columnCount; j++ {
			if sb.Len() != 0 {
				sb.Write([]byte(","))
			}
			index := r.Intn(numColumnValues)
			columnValue := columns[index]

			sb.WriteByte('\'')
			sb.Write(columnValue)
			sb.WriteByte('\'')
		}
		result = append(result, sb.String())
	}
	return result
}

type LargeRowWorkload struct {
	smallRows []string
	largeRows []string

	largeRatio float64

	columnCount int

	seed     atomic.Int64
	randPool sync.Pool
}

func (l *LargeRowWorkload) getRand() *rand.Rand {
	return l.randPool.Get().(*rand.Rand)
}

func (l *LargeRowWorkload) putRand(r *rand.Rand) {
	l.randPool.Put(r)
}

func (l *LargeRowWorkload) getSmallRow(r *rand.Rand) string {
	return l.smallRows[r.Intn(len(l.smallRows))]
}

func (l *LargeRowWorkload) getLargeRow(r *rand.Rand) string {
	return l.largeRows[r.Intn(len(l.largeRows))]
}

func NewLargeRowWorkload(
	normalRowSize, largeRowSize int, largeRatio float64,
) schema.Workload {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	columnCount := int(float64(largeRowSize) / varcharColumnMaxLen)
	smallColumnSize := int(float64(normalRowSize) / float64(columnCount))

	l := &LargeRowWorkload{
		largeRatio:  largeRatio,
		columnCount: columnCount,
		smallRows:   newRowValues(r, smallColumnSize, columnCount, 512),
		largeRows:   newRowValues(r, varcharColumnMaxLen, columnCount, 128),
	}
	l.seed.Store(time.Now().UnixNano())
	l.randPool.New = func() any {
		return rand.New(rand.NewSource(l.seed.Add(1)))
	}
	return l
}

func getTableName(n int) string {
	return fmt.Sprintf("large_row_%d", n)
}

func (l *LargeRowWorkload) BuildCreateTableStatement(n int) string {
	var cols string
	for i := 0; i < l.columnCount; i++ {
		cols = fmt.Sprintf("%s, col_%d VARCHAR(%d)", cols, i, varcharColumnMaxLen)
	}
	tableName := getTableName(n)
	query := fmt.Sprintf("CREATE TABLE %s(id bigint primary key %s);", tableName, cols)

	log.Info("large row workload, create the table", zap.Int("table", n), zap.Int("length", len(query)))

	return query
}

func (l *LargeRowWorkload) BuildInsertSql(tableN int, batchSize int) string {
	tableName := getTableName(tableN)
	r := l.getRand()
	defer l.putRand(r)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (%d,%s)", tableName, r.Int63()%maxValue, l.getSmallRow(r)))

	var largeRowCount int
	for i := 1; i < batchSize; i++ {
		if r.Float64() < l.largeRatio {
			sb.WriteString(fmt.Sprintf(",(%d,%s)", r.Int63()%maxValue, l.getLargeRow(r)))
			largeRowCount++
		} else {
			sb.WriteString(fmt.Sprintf(",(%d,%s)", r.Int63()%maxValue, l.getSmallRow(r)))
		}
	}

	log.Debug("large row workload, insert the table",
		zap.Int("table", tableN), zap.Int("batchSize", batchSize),
		zap.Int("largeRowCount", largeRowCount), zap.Int("length", sb.Len()))

	return sb.String()
}

func (l *LargeRowWorkload) BuildUpdateSql(opts schema.UpdateOption) string {
	tableName := getTableName(opts.TableIndex)
	r := l.getRand()
	defer l.putRand(r)

	upsertSQL := strings.Builder{}
	upsertSQL.WriteString(fmt.Sprintf("INSERT INTO %s VALUES (%d,%s)", tableName, r.Int63()%maxValue, l.getSmallRow(r)))

	var largeRowCount int
	for i := 1; i < opts.Batch; i++ {
		if r.Float64() < l.largeRatio {
			upsertSQL.WriteString(fmt.Sprintf(",(%d,%s)", r.Int63()%maxValue, l.getLargeRow(r)))
			largeRowCount++
		} else {
			upsertSQL.WriteString(fmt.Sprintf(",(%d,%s)", r.Int63()%maxValue, l.getSmallRow(r)))
		}
	}
	upsertSQL.WriteString(" ON DUPLICATE KEY UPDATE col_0=VALUES(col_0)")

	log.Debug("large row workload, upsert the table",
		zap.Int("table", opts.TableIndex), zap.Int("batchSize", opts.Batch),
		zap.Int("largeRowCount", largeRowCount))
	return upsertSQL.String()
}

func (l *LargeRowWorkload) BuildDeleteSql(opts schema.DeleteOption) string {
	r := l.getRand()
	defer l.putRand(r)

	deleteType := r.Intn(3)
	tableName := getTableName(opts.TableIndex)

	switch deleteType {
	case 0:
		// Strategy 1: Random single/multiple row delete by ID
		var buf strings.Builder
		for i := 0; i < opts.Batch; i++ {
			id := r.Int63() % maxValue
			if i > 0 {
				buf.WriteString(";")
			}
			buf.WriteString(fmt.Sprintf("DELETE FROM %s WHERE id = %d", tableName, id))
		}
		return buf.String()

	case 1:
		// Strategy 2: Range delete by ID
		startID := r.Int63() % maxValue
		endID := startID + int64(opts.Batch*100)
		if endID > maxValue {
			endID = maxValue
		}
		return fmt.Sprintf("DELETE FROM %s WHERE id BETWEEN %d AND %d LIMIT %d",
			tableName, startID, endID, opts.Batch)

	case 2:
		// Strategy 3: Conditional delete by random ID modulo
		modValue := r.Intn(1000)
		return fmt.Sprintf("DELETE FROM %s WHERE id %% 1000 = %d LIMIT %d",
			tableName, modValue, opts.Batch)

	default:
		return ""
	}
}

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
