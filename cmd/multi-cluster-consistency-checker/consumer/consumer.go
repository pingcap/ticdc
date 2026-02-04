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

package consumer

import (
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/utils"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
)

type (
	fileIndexRange  map[cloudstorage.FileIndexKey]indexRange
	fileIndexKeyMap map[cloudstorage.FileIndexKey]uint64
)

type indexRange struct {
	start uint64
	end   uint64
}

func updateTableDMLIdxMap(
	tableDMLIdxMap map[cloudstorage.DmlPathKey]fileIndexKeyMap,
	dmlkey cloudstorage.DmlPathKey,
	fileIdx *cloudstorage.FileIndex,
) {
	m, ok := tableDMLIdxMap[dmlkey]
	if !ok {
		tableDMLIdxMap[dmlkey] = fileIndexKeyMap{
			fileIdx.FileIndexKey: fileIdx.Idx,
		}
	} else if fileIdx.Idx > m[fileIdx.FileIndexKey] {
		m[fileIdx.FileIndexKey] = fileIdx.Idx
	}
}

type schemaParser struct {
	path   string
	parser *utils.TableParser
}

type schemaKey struct {
	schema string
	table  string
}
