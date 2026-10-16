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

package schemastore

import (
	"strings"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sqlname"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
)

// newViewTableResolver uses one immutable metadata snapshot. The cache lasts
// for one DDL and loads only referenced databases; ListSimpleTables includes
// views as well as physical tables. No changefeed filtering is applied here.
func newViewTableResolver(snapshot meta.Reader) sqlname.Resolver {
	var schemas map[string]*model.DBInfo
	tables := make(map[int64]map[string]string)
	return func(source sqlname.Name) (sqlname.Name, error) {
		if schemas == nil {
			dbs, err := snapshot.ListDatabases()
			if err != nil {
				return sqlname.Name{}, errors.WrapError(errors.ErrMetaListDatabases, err)
			}
			schemas = make(map[string]*model.DBInfo, len(dbs))
			for _, db := range dbs {
				schemas[db.Name.L] = db
			}
		}
		db, ok := schemas[strings.ToLower(source.Schema)]
		if !ok {
			return sqlname.Name{}, errors.ErrDDLEventError.GenWithStack("view source schema %q is absent from the DDL snapshot", source.Schema)
		}
		names, loaded := tables[db.ID]
		if !loaded {
			infos, err := snapshot.ListSimpleTables(db.ID)
			if err != nil {
				return sqlname.Name{}, errors.WrapError(errors.ErrDDLEventError, err)
			}
			names = make(map[string]string, len(infos))
			for _, info := range infos {
				names[info.Name.L] = info.Name.O
			}
			tables[db.ID] = names
		}
		table, ok := names[strings.ToLower(source.Table)]
		if !ok {
			return sqlname.Name{}, errors.ErrDDLEventError.GenWithStack("view source table %s.%s is absent from the DDL snapshot", source.Schema, source.Table)
		}
		return sqlname.Name{Schema: db.Name.O, Table: table}, nil
	}
}
