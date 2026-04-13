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

import (
	"fmt"
	"strings"

	cerror "github.com/pingcap/ticdc/pkg/errors"
)

func validateSchemaEvolution(schemaMode SchemaMode, current *icebergSchema, desired *icebergSchema) error {
	if current == nil || desired == nil {
		return nil
	}

	currentTypes := make(map[int]string, len(current.Fields))
	for _, f := range current.Fields {
		currentTypes[f.ID] = strings.TrimSpace(f.Type)
	}

	for _, f := range desired.Fields {
		oldType, ok := currentTypes[f.ID]
		if !ok {
			continue
		}
		oldType = strings.TrimSpace(oldType)
		newType := strings.TrimSpace(f.Type)
		if oldType == newType {
			continue
		}
		if schemaMode == SchemaModeEvolve && isSafeIcebergTypeWidening(oldType, newType) {
			continue
		}
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs(
			fmt.Sprintf("iceberg schema evolution is not supported: field %d type changed from %s to %s", f.ID, oldType, newType),
		)
	}

	return nil
}

func isSafeIcebergTypeWidening(oldType string, newType string) bool {
	if strings.TrimSpace(oldType) == strings.TrimSpace(newType) {
		return true
	}

	switch oldType {
	case "int":
		return newType == "long"
	case "float":
		return newType == "double"
	default:
	}

	if oldPrec, oldScale, ok := parseDecimalType(oldType); ok {
		newPrec, newScale, ok := parseDecimalType(newType)
		if !ok {
			return false
		}
		if newScale != oldScale {
			return false
		}
		return newPrec >= oldPrec
	}

	return false
}
