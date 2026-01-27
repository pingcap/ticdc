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
	"net/url"
	"strings"

	cloudkms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/api/option"
)

type gcpKMSDecryptor struct {
	client *cloudkms.KeyManagementClient
}

func (d *gcpKMSDecryptor) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	resp, err := d.client.Decrypt(ctx, &kmspb.DecryptRequest{
		Name:       keyID,
		Ciphertext: ciphertext,
	})
	if err != nil {
		return nil, err
	}
	return resp.Plaintext, nil
}

func (d *gcpKMSDecryptor) Close() error {
	return d.client.Close()
}

func newGCPDecryptor(ctx context.Context, cfg gcpClientConfig) (gcpDecryptor, error) {
	var opts []option.ClientOption
	if cfg.Endpoint != "" {
		opts = append(opts, option.WithEndpoint(normalizeGCPEndpoint(cfg.Endpoint)))
	}
	if cfg.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(cfg.CredentialsFile))
	}
	if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(cfg.CredentialsJSON)))
	}

	client, err := cloudkms.NewKeyManagementClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &gcpKMSDecryptor{client: client}, nil
}

func normalizeGCPEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if strings.Contains(endpoint, "://") {
		parsed, err := url.Parse(endpoint)
		if err == nil && parsed.Host != "" {
			return parsed.Host
		}
	}
	return endpoint
}
