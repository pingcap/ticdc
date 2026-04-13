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

import "encoding/json"

type manifestPartitionField struct {
	Name     string
	FieldID  int
	AvroType any
}

func buildManifestEntrySchemaV2(partitionFields []manifestPartitionField) (string, error) {
	if len(partitionFields) == 0 {
		return manifestEntrySchemaV2Unpartitioned, nil
	}

	fields := make([]any, 0, len(partitionFields))
	for _, f := range partitionFields {
		fields = append(fields, map[string]any{
			"name":     f.Name,
			"type":     []any{"null", f.AvroType},
			"default":  nil,
			"field-id": f.FieldID,
		})
	}

	schema := map[string]any{
		"type": "record",
		"name": "manifest_entry",
		"fields": []any{
			map[string]any{"name": "status", "type": "int", "field-id": 0},
			map[string]any{"name": "snapshot_id", "type": []any{"null", "long"}, "default": nil, "field-id": 1},
			map[string]any{"name": "sequence_number", "type": []any{"null", "long"}, "default": nil, "field-id": 3},
			map[string]any{"name": "file_sequence_number", "type": []any{"null", "long"}, "default": nil, "field-id": 4},
			map[string]any{
				"name":     "data_file",
				"field-id": 2,
				"type": map[string]any{
					"type": "record",
					"name": "r2",
					"fields": []any{
						map[string]any{"name": "content", "type": "int", "field-id": 134, "default": 0},
						map[string]any{"name": "file_path", "type": "string", "field-id": 100},
						map[string]any{"name": "file_format", "type": "string", "field-id": 101},
						map[string]any{
							"name":     "partition",
							"field-id": 102,
							"type": map[string]any{
								"type":   "record",
								"name":   "r102",
								"fields": fields,
							},
						},
						map[string]any{"name": "record_count", "type": "long", "field-id": 103},
						map[string]any{"name": "file_size_in_bytes", "type": "long", "field-id": 104},
						map[string]any{
							"name":     "equality_ids",
							"type":     []any{"null", map[string]any{"type": "array", "items": "int", "element-id": 136}},
							"default":  nil,
							"field-id": 135,
						},
						map[string]any{"name": "sort_order_id", "type": []any{"null", "int"}, "default": nil, "field-id": 140},
					},
				},
			},
		},
	}

	schemaBytes, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}
	return string(schemaBytes), nil
}
