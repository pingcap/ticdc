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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateSchemaEvolutionStrictRejectsTypeChange(t *testing.T) {
	current := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "a", Required: false, Type: "int"},
		},
	}
	desired := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "a", Required: false, Type: "long"},
		},
	}

	err := validateSchemaEvolution(SchemaModeStrict, current, desired)
	require.Error(t, err)
}

func TestValidateSchemaEvolutionEvolveAllowsIntToLong(t *testing.T) {
	current := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "a", Required: false, Type: "int"},
		},
	}
	desired := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "a", Required: false, Type: "long"},
		},
	}

	require.NoError(t, validateSchemaEvolution(SchemaModeEvolve, current, desired))
}

func TestValidateSchemaEvolutionEvolveAllowsDecimalPrecisionIncrease(t *testing.T) {
	current := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "d", Required: false, Type: "decimal(10,2)"},
		},
	}
	desired := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "d", Required: false, Type: "decimal(12,2)"},
		},
	}

	require.NoError(t, validateSchemaEvolution(SchemaModeEvolve, current, desired))
}

func TestValidateSchemaEvolutionEvolveRejectsDecimalScaleChange(t *testing.T) {
	current := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "d", Required: false, Type: "decimal(10,2)"},
		},
	}
	desired := &icebergSchema{
		Type:     "struct",
		SchemaID: 0,
		Fields: []icebergField{
			{ID: 1, Name: "d", Required: false, Type: "decimal(12,3)"},
		},
	}

	require.Error(t, validateSchemaEvolution(SchemaModeEvolve, current, desired))
}
