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

package cloudstorage

import (
	"context"
	"encoding/json"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// ParseTableDefinition parses a schema file and validates its path metadata.
func ParseTableDefinition(
	ctx context.Context,
	storage storeapi.Storage,
	path string,
) (SchemaPathKey, TableDefinition, error) {
	var schemaKey SchemaPathKey
	checksum, err := schemaKey.ParseSchemaFilePath(path)
	if err != nil {
		return schemaKey, TableDefinition{}, err
	}
	var tableDef TableDefinition
	schemaContent, err := storage.ReadFile(ctx, path)
	if err != nil {
		return schemaKey, tableDef, errors.Trace(err)
	}
	if err = json.Unmarshal(schemaContent, &tableDef); err != nil {
		return schemaKey, tableDef, errors.Trace(err)
	}
	checksumInMem, err := tableDef.Sum32(nil)
	if err != nil {
		return schemaKey, tableDef, errors.Trace(err)
	}
	if checksumInMem != checksum || schemaKey.TableVersion != tableDef.TableVersion {
		return schemaKey, tableDef, errors.ErrStorageSinkInvalidFileName.GenWithStack(
			"checksum mismatch in schema file %s: checksum in memory %d, checksum in file %d, table version in path %d, table version in file %d",
			path, checksumInMem, checksum, schemaKey.TableVersion, tableDef.TableVersion)
	}
	return schemaKey, tableDef, nil
}
