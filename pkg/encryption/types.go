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

import (
	"github.com/pingcap/errors"
)

// EncryptionAlgorithm represents the encryption algorithm type
type EncryptionAlgorithm string

const (
	// AES256CTR represents AES-256-CTR encryption algorithm
	AES256CTR EncryptionAlgorithm = "AES256_CTR"
	// AES256GCM represents AES-256-GCM encryption algorithm
	AES256GCM EncryptionAlgorithm = "AES256_GCM"
)

// String returns the string representation of the encryption algorithm
func (a EncryptionAlgorithm) String() string {
	return string(a)
}

// KMSVendor represents the KMS vendor type
type KMSVendor string

const (
	// KMSVendorAWS represents AWS KMS
	KMSVendorAWS KMSVendor = "AWS"
	// KMSVendorAzure represents Azure Key Vault
	KMSVendorAzure KMSVendor = "Azure"
	// KMSVendorGCP represents GCP KMS
	KMSVendorGCP KMSVendor = "GCP"
)

// String returns the string representation of the KMS vendor
func (v KMSVendor) String() string {
	return string(v)
}

// MasterKey represents the master key information
type MasterKey struct {
	Vendor     KMSVendor `json:"vendor"`
	CMEKID     string    `json:"cmek_id"`
	Region     string    `json:"region"`
	Ciphertext []byte    `json:"ciphertext"`
}

// DataKey represents a data encryption key
type DataKey struct {
	Ciphertext          []byte              `json:"ciphertext"`
	EncryptionAlgorithm EncryptionAlgorithm `json:"encryption_algorithm"`
}

// KeyspaceEncryptionMeta represents the encryption metadata for a keyspace
type KeyspaceEncryptionMeta struct {
	Enabled          bool                `json:"enabled"`
	Version          byte                `json:"version"` // Encryption format version from TiKV
	MasterKey        *MasterKey          `json:"master_key"`
	CurrentDataKeyID string              `json:"current_data_key_id"`
	DataKeyMap       map[string]*DataKey `json:"data_key_map"`
}

// DataKeyID represents a 3-byte data key identifier
type DataKeyID [3]byte

// ToString converts DataKeyID to string
func (id DataKeyID) ToString() string {
	return string(id[:])
}

// FromString creates DataKeyID from string (must be 3 bytes)
func DataKeyIDFromString(s string) (DataKeyID, error) {
	if len(s) != 3 {
		return DataKeyID{}, errors.New("data key ID must be exactly 3 bytes")
	}
	var id DataKeyID
	copy(id[:], s)
	return id, nil
}
