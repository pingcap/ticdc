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

package iceberg

import (
	"encoding/json"
	"strings"

	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
)

const icebergFieldDocTableColPrefix = "tidb.table-col:"

func encodeOriginalTableCol(colInfo *timodel.ColumnInfo) (string, error) {
	if colInfo == nil {
		return "", nil
	}

	var tableCol cloudstorage.TableCol
	tableCol.FromTiColumnInfo(colInfo, false)

	payload, err := json.Marshal(tableCol)
	if err != nil {
		return "", err
	}
	return icebergFieldDocTableColPrefix + string(payload), nil
}

func decodeOriginalTableCol(doc string) (*cloudstorage.TableCol, error) {
	doc = strings.TrimSpace(doc)
	if doc == "" || !strings.HasPrefix(doc, icebergFieldDocTableColPrefix) {
		return nil, nil
	}

	var tableCol cloudstorage.TableCol
	if err := json.Unmarshal([]byte(strings.TrimPrefix(doc, icebergFieldDocTableColPrefix)), &tableCol); err != nil {
		return nil, err
	}
	return &tableCol, nil
}
