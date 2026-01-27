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

package kms

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/config"
	"github.com/stretchr/testify/require"
)

type fakeDecryptor struct {
	lastKeyID     string
	lastCipher    []byte
	plaintextResp []byte
}

func (d *fakeDecryptor) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	d.lastKeyID = keyID
	d.lastCipher = append(d.lastCipher[:0], ciphertext...)
	return append([]byte(nil), d.plaintextResp...), nil
}

type fakeClosableDecryptor struct {
	fakeDecryptor
	closed bool
}

func (d *fakeClosableDecryptor) Close() error {
	d.closed = true
	return nil
}

func TestKMSClientDecryptMasterKeyAWSUsesResolvedRegionEndpointAndCachesClient(t *testing.T) {
	t.Parallel()

	const (
		region   = "us-west-2"
		endpoint = "https://kms.us-west-2.amazonaws.com"
		keyID    = "arn:aws:kms:us-west-2:123456789012:key/abcd"
		vendor   = "aws-kms"
	)
	ciphertext := []byte{1, 2, 3}
	plaintext := []byte{9, 8, 7}

	var calls int
	decryptor := &fakeDecryptor{plaintextResp: plaintext}
	awsFactory := func(ctx context.Context, cfg awsClientConfig) (awsDecryptor, error) {
		calls++
		require.Equal(t, region, cfg.Region)
		require.Equal(t, endpoint, cfg.Endpoint)
		return decryptor, nil
	}

	cli, err := NewClient(&config.EncryptionConfig{}, WithAWSDecryptorFactory(awsFactory))
	require.NoError(t, err)

	out1, err := cli.DecryptMasterKey(context.Background(), ciphertext, keyID, vendor, region, endpoint)
	require.NoError(t, err)
	require.Equal(t, plaintext, out1)
	require.Equal(t, keyID, decryptor.lastKeyID)
	require.Equal(t, ciphertext, decryptor.lastCipher)

	out2, err := cli.DecryptMasterKey(context.Background(), ciphertext, keyID, vendor, region, endpoint)
	require.NoError(t, err)
	require.Equal(t, plaintext, out2)
	require.Equal(t, 1, calls, "expected client to be cached")
}

func TestKMSClientDecryptMasterKeyGCPUsesResolvedEndpointAndCachesClient(t *testing.T) {
	t.Parallel()

	const (
		endpoint = "cloudkms.googleapis.com:443"
		keyID    = "projects/p/locations/l/keyRings/r/cryptoKeys/k"
		vendor   = "gcp-kms"
	)
	ciphertext := []byte{4, 5, 6}
	plaintext := []byte{6, 5, 4}

	var calls int
	decryptor := &fakeClosableDecryptor{fakeDecryptor: fakeDecryptor{plaintextResp: plaintext}}
	gcpFactory := func(ctx context.Context, cfg gcpClientConfig) (gcpDecryptor, error) {
		calls++
		require.Equal(t, endpoint, cfg.Endpoint)
		return decryptor, nil
	}

	cli, err := NewClient(&config.EncryptionConfig{}, WithGCPDecryptorFactory(gcpFactory))
	require.NoError(t, err)

	out1, err := cli.DecryptMasterKey(context.Background(), ciphertext, keyID, vendor, "", endpoint)
	require.NoError(t, err)
	require.Equal(t, plaintext, out1)
	require.Equal(t, keyID, decryptor.lastKeyID)
	require.Equal(t, ciphertext, decryptor.lastCipher)

	out2, err := cli.DecryptMasterKey(context.Background(), ciphertext, keyID, vendor, "", endpoint)
	require.NoError(t, err)
	require.Equal(t, plaintext, out2)
	require.Equal(t, 1, calls, "expected client to be cached")
}

func TestKMSClientDecryptMasterKeyUnknownVendorReturnsError(t *testing.T) {
	t.Parallel()

	cli, err := NewClient(&config.EncryptionConfig{})
	require.NoError(t, err)

	_, err = cli.DecryptMasterKey(context.Background(), []byte{1}, "kid", "unknown", "r", "e")
	require.Error(t, err)
}

func TestKMSClientAWSConfigOverridesRegionAndEndpoint(t *testing.T) {
	t.Parallel()

	cfg := &config.EncryptionConfig{
		KMS: &config.KMSConfig{
			AWS: &config.AWSKMSConfig{
				Region:   "override-region",
				Endpoint: "https://override.endpoint",
				Profile:  "test-profile",
			},
			GCP: &config.GCPKMSConfig{},
		},
	}

	awsFactory := func(ctx context.Context, cfg awsClientConfig) (awsDecryptor, error) {
		require.Equal(t, "override-region", cfg.Region)
		require.Equal(t, "https://override.endpoint", cfg.Endpoint)
		require.Equal(t, "test-profile", cfg.Profile)
		return &fakeDecryptor{plaintextResp: []byte{1}}, nil
	}

	cli, err := NewClient(cfg, WithAWSDecryptorFactory(awsFactory))
	require.NoError(t, err)
	_, err = cli.DecryptMasterKey(context.Background(), []byte{1}, "kid", "aws-kms", "meta-region", "meta-endpoint")
	require.NoError(t, err)
}

func TestKMSClientGCPConfigOverridesEndpointAndCredentials(t *testing.T) {
	t.Parallel()

	cfg := &config.EncryptionConfig{
		KMS: &config.KMSConfig{
			AWS: &config.AWSKMSConfig{},
			GCP: &config.GCPKMSConfig{
				Endpoint:        "override.gcp.endpoint:443",
				CredentialsFile: "/tmp/creds.json",
			},
		},
	}

	gcpFactory := func(ctx context.Context, cfg gcpClientConfig) (gcpDecryptor, error) {
		require.Equal(t, "override.gcp.endpoint:443", cfg.Endpoint)
		require.Equal(t, "/tmp/creds.json", cfg.CredentialsFile)
		require.Empty(t, cfg.CredentialsJSON)
		return &fakeClosableDecryptor{fakeDecryptor: fakeDecryptor{plaintextResp: []byte{1}}}, nil
	}

	cli, err := NewClient(cfg, WithGCPDecryptorFactory(gcpFactory))
	require.NoError(t, err)
	_, err = cli.DecryptMasterKey(context.Background(), []byte{1}, "kid", "gcp-kms", "", "meta-endpoint")
	require.NoError(t, err)
}

func TestNewClientValidatesAWSStaticCredentials(t *testing.T) {
	t.Parallel()

	_, err := NewClient(&config.EncryptionConfig{
		KMS: &config.KMSConfig{
			AWS: &config.AWSKMSConfig{
				AccessKey: "ak",
			},
			GCP: &config.GCPKMSConfig{},
		},
	})
	require.Error(t, err)
}

func TestNewClientValidatesGCPCredentialsExclusive(t *testing.T) {
	t.Parallel()

	_, err := NewClient(&config.EncryptionConfig{
		KMS: &config.KMSConfig{
			AWS: &config.AWSKMSConfig{},
			GCP: &config.GCPKMSConfig{
				CredentialsFile: "/tmp/a.json",
				CredentialsJSON: "{}",
			},
		},
	})
	require.Error(t, err)
}
