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

//go:build nextgen

package common

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/util/naming"
)

const (
	// JobTableID is the id of `tidb_ddl_job`.
	JobTableID = metadef.TiDBDDLJobTableID
	// JobHistoryID is the id of `tidb_ddl_history`
	JobHistoryID = metadef.TiDBDDLHistoryTableID
)

// ValidateKeyspace use the naming rules of TiDB to check the validation of the keyspace
func ValidateKeyspace(keyspace string) error {
	return errors.Trace(naming.CheckKeyspaceName(keyspace))
}
