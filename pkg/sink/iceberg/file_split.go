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

package iceberg

type rowChunk struct {
	rows           []ChangeRow
	estimatedBytes int64
}

func splitRowsByTargetSize(rows []ChangeRow, targetSizeBytes int64, emitMetadata bool) []rowChunk {
	if len(rows) == 0 {
		return nil
	}

	if targetSizeBytes <= 0 || len(rows) == 1 {
		return []rowChunk{{
			rows:           rows,
			estimatedBytes: estimateRowsSize(rows, emitMetadata),
		}}
	}

	chunks := make([]rowChunk, 0, 1)
	start := 0
	var currentSize int64
	for i := range rows {
		rowSize := estimateChangeRowSize(rows[i], emitMetadata)
		if currentSize+rowSize < targetSizeBytes {
			currentSize += rowSize
			continue
		}
		if i == start {
			// One row is already too large, write it as a single file.
			chunks = append(chunks, rowChunk{
				rows:           rows[start : i+1],
				estimatedBytes: rowSize,
			})
			start = i + 1
			currentSize = 0
			continue
		}
		chunks = append(chunks, rowChunk{
			rows:           rows[start:i],
			estimatedBytes: currentSize,
		})
		start = i
		currentSize = rowSize
	}

	if start < len(rows) {
		chunks = append(chunks, rowChunk{
			rows:           rows[start:],
			estimatedBytes: currentSize,
		})
	}
	return chunks
}

func estimateRowsSize(rows []ChangeRow, emitMetadata bool) int64 {
	if len(rows) == 0 {
		return 0
	}
	var size int64
	for _, row := range rows {
		size += estimateChangeRowSize(row, emitMetadata)
	}
	return size
}

func estimateChangeRowSize(row ChangeRow, emitMetadata bool) int64 {
	var size int64
	if emitMetadata {
		size += int64(len(row.Op) + len(row.CommitTs) + len(row.CommitTime) + 32)
	}
	for _, v := range row.Columns {
		if v == nil {
			size++
			continue
		}
		size += int64(len(*v))
	}
	if size <= 0 {
		return 1
	}
	return size
}
