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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awskms "github.com/aws/aws-sdk-go-v2/service/kms"
)

type awsKMSDecryptor struct {
	client *awskms.Client
}

func (d *awsKMSDecryptor) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	input := &awskms.DecryptInput{
		CiphertextBlob: ciphertext,
	}
	if keyID != "" {
		input.KeyId = aws.String(keyID)
	}
	out, err := d.client.Decrypt(ctx, input)
	if err != nil {
		return nil, err
	}
	return out.Plaintext, nil
}

func newAWSDecryptor(ctx context.Context, cfg awsClientConfig) (awsDecryptor, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(cfg.Profile))
	}
	if cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}
	if cfg.Endpoint != "" {
		endpointURL := normalizeAWSEndpoint(cfg.Endpoint)
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
			if service != awskms.ServiceID {
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			}
			return aws.Endpoint{
				URL:               endpointURL,
				SigningRegion:     cfg.Region,
				HostnameImmutable: true,
			}, nil
		})
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(resolver))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return &awsKMSDecryptor{client: awskms.NewFromConfig(awsCfg)}, nil
}

func normalizeAWSEndpoint(endpoint string) string {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return ""
	}
	if strings.Contains(endpoint, "://") {
		return endpoint
	}
	return "https://" + endpoint
}
