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

package encryption

// EncryptionMeta is aligned with kvproto `keyspace_encryptionpb.EncryptionMeta`.
type EncryptionMeta struct {
	KeyspaceId uint32              `json:"keyspace_id,omitempty"`
	Current    *EncryptionEpoch    `json:"current,omitempty"`
	MasterKey  *MasterKey          `json:"master_key,omitempty"`
	DataKeys   map[uint32]*DataKey `json:"data_keys,omitempty"`
	History    []*EncryptionEpoch  `json:"history,omitempty"`
}

// EncryptionEpoch is aligned with kvproto `keyspace_encryptionpb.EncryptionEpoch`.
type EncryptionEpoch struct {
	FileId    uint64 `json:"file_id,omitempty"`
	DataKeyId uint32 `json:"data_key_id,omitempty"`
	CreatedAt uint64 `json:"created_at,omitempty"`
}

// MasterKey is aligned with kvproto `keyspace_encryptionpb.MasterKey`.
type MasterKey struct {
	Vendor     string `json:"vendor,omitempty"`
	CmekId     string `json:"cmek_id,omitempty"`
	Region     string `json:"region,omitempty"`
	Endpoint   string `json:"endpoint,omitempty"`
	Ciphertext []byte `json:"ciphertext,omitempty"`
}

// DataKey is aligned with kvproto `keyspace_encryptionpb.DataKey`.
type DataKey struct {
	Ciphertext []byte `json:"ciphertext,omitempty"`
}
