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

const (
	manifestListSchemaV2 = `
{
  "type": "record",
  "name": "manifest_file",
  "fields": [
    { "name": "manifest_path", "type": "string", "field-id": 500 },
    { "name": "manifest_length", "type": "long", "field-id": 501 },
    { "name": "partition_spec_id", "type": "int", "field-id": 502 },
    { "name": "content", "type": "int", "field-id": 517, "default": 0 },
    { "name": "sequence_number", "type": "long", "field-id": 515 },
    { "name": "min_sequence_number", "type": "long", "field-id": 516 },
    { "name": "added_snapshot_id", "type": "long", "field-id": 503 }
  ]
}
`

	manifestEntrySchemaV2Unpartitioned = `
{
  "type": "record",
  "name": "manifest_entry",
  "fields": [
    { "name": "status", "type": "int", "field-id": 0 },
    { "name": "snapshot_id", "type": ["null", "long"], "default": null, "field-id": 1 },
    { "name": "sequence_number", "type": ["null", "long"], "default": null, "field-id": 3 },
    { "name": "file_sequence_number", "type": ["null", "long"], "default": null, "field-id": 4 },
    {
      "name": "data_file",
      "field-id": 2,
      "type": {
        "type": "record",
        "name": "r2",
        "fields": [
          { "name": "content", "type": "int", "field-id": 134, "default": 0 },
          { "name": "file_path", "type": "string", "field-id": 100 },
          { "name": "file_format", "type": "string", "field-id": 101 },
          { "name": "partition", "field-id": 102, "type": { "type": "record", "name": "r102", "fields": [] } },
          { "name": "record_count", "type": "long", "field-id": 103 },
          { "name": "file_size_in_bytes", "type": "long", "field-id": 104 },
          { "name": "equality_ids", "type": ["null", { "type": "array", "items": "int", "element-id": 136 }], "default": null, "field-id": 135 },
          { "name": "sort_order_id", "type": ["null", "int"], "default": null, "field-id": 140 }
        ]
      }
    }
  ]
}
`
)
