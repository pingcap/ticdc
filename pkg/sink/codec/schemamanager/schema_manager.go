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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemamanager

import (
	"context"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
)

// NewSchemaManager creates a schema manager for the configured registry.
func NewSchemaManager(ctx context.Context, config *common.Config) (SchemaManager, error) {
	schemaRegistryType := config.SchemaRegistryType()
	switch schemaRegistryType {
	case common.SchemaRegistryTypeConfluent:
		schemaM, err := NewConfluentSchemaManager(ctx, config.AvroConfluentSchemaRegistry, nil)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return schemaM, nil
	case common.SchemaRegistryTypeGlue:
		schemaM, err := NewGlueSchemaManager(ctx, config.AvroGlueSchemaRegistry)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return schemaM, nil
	default:
		return nil, errors.ErrAvroSchemaAPIError.GenWithStackByArgs(schemaRegistryType)
	}
}

// SchemaManager manages schemas registered in a schema registry.
type SchemaManager interface {
	Register(ctx context.Context, schemaName string, schemaDefinition string) (SchemaID, error)
	Lookup(ctx context.Context, schemaName string, schemaID SchemaID) (*goavro.Codec, error)
	GetCachedOrRegister(ctx context.Context, schemaName, schemaIdentity string,
		tableVersion uint64, schemaGen SchemaGenerator) (*goavro.Codec, []byte, error)
	RegistryType() string
	ClearRegistry(ctx context.Context, schemaName string) error
}

// SchemaGenerator returns an Avro schema in JSON.
type SchemaGenerator func() (string, error)

// SchemaID identifies a schema in Confluent or AWS Glue Schema Registry.
type SchemaID struct {
	confluentSchemaID int
	glueSchemaID      string
}

// NewConfluentSchemaID creates a Confluent schema ID.
func NewConfluentSchemaID(schemaID int) SchemaID {
	return SchemaID{confluentSchemaID: schemaID}
}

// NewGlueSchemaID creates an AWS Glue schema ID.
func NewGlueSchemaID(schemaID string) SchemaID {
	return SchemaID{glueSchemaID: schemaID}
}

// GenCodec creates an Avro codec without treating the string literal "null" as null.
func GenCodec(schema string) (*goavro.Codec, error) {
	return goavro.NewCodecWithOptions(schema, &goavro.CodecOption{EnableStringNull: false})
}
