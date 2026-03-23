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
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/config"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

type awsClientConfig struct {
	Region          string
	Endpoint        string
	Profile         string
	AccessKey       string
	SecretAccessKey string
	SessionToken    string
}

type gcpClientConfig struct {
	Endpoint        string
	CredentialsFile string
	CredentialsJSON string
}

type awsDecryptor interface {
	Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error)
}

type gcpDecryptor interface {
	Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error)
	Close() error
}

type (
	awsDecryptorFactory func(ctx context.Context, cfg awsClientConfig) (awsDecryptor, error)
	gcpDecryptorFactory func(ctx context.Context, cfg gcpClientConfig) (gcpDecryptor, error)
)

type ClientOption func(*client)

func WithAWSDecryptorFactory(factory awsDecryptorFactory) ClientOption {
	return func(c *client) {
		c.awsFactory = factory
	}
}

func WithGCPDecryptorFactory(factory gcpDecryptorFactory) ClientOption {
	return func(c *client) {
		c.gcpFactory = factory
	}
}

type awsClientKey struct {
	region   string
	endpoint string
}

type gcpClientKey struct {
	endpoint string
}

type client struct {
	cfg *config.EncryptionConfig

	awsFactory awsDecryptorFactory
	gcpFactory gcpDecryptorFactory

	awsMu      sync.Mutex
	awsClients map[awsClientKey]awsDecryptor

	gcpMu      sync.Mutex
	gcpClients map[gcpClientKey]gcpDecryptor
}

func NewClient(cfg *config.EncryptionConfig, opts ...ClientOption) (KMSClient, error) {
	c := &client{
		cfg:        cfg,
		awsClients: make(map[awsClientKey]awsDecryptor),
		gcpClients: make(map[gcpClientKey]gcpDecryptor),
		awsFactory: newAWSDecryptor,
		gcpFactory: newGCPDecryptor,
	}
	for _, opt := range opts {
		opt(c)
	}
	if err := c.validateConfig(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *client) DecryptMasterKey(ctx context.Context, ciphertext []byte, keyID string, vendor string, region string, endpoint string) ([]byte, error) {
	normalizedVendor := normalizeVendor(vendor)
	switch normalizedVendor {
	case vendorAWS:
		awsCfg := c.resolveAWS(region, endpoint)
		if awsCfg.Region == "" {
			log.Warn("aws kms region is empty",
				zap.String("vendor", vendor),
				zap.String("keyID", keyID),
				zap.String("endpoint", awsCfg.Endpoint))
			return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("aws kms region is empty")
		}
		decryptor, err := c.getAWSDecryptor(ctx, awsCfg)
		if err != nil {
			log.Warn("failed to create aws kms client",
				zap.String("vendor", vendor),
				zap.String("keyID", keyID),
				zap.String("region", awsCfg.Region),
				zap.String("endpoint", awsCfg.Endpoint),
				zap.Error(err))
			return nil, cerrors.ErrDecodeFailed.Wrap(err)
		}
		plaintext, err := decryptor.Decrypt(ctx, keyID, ciphertext)
		if err != nil {
			log.Warn("failed to decrypt master key via aws kms",
				zap.String("vendor", vendor),
				zap.String("keyID", keyID),
				zap.String("region", awsCfg.Region),
				zap.String("endpoint", awsCfg.Endpoint),
				zap.Error(err))
			return nil, cerrors.ErrDecodeFailed.Wrap(err)
		}
		return plaintext, nil
	case vendorGCP:
		gcpCfg := c.resolveGCP(endpoint)
		decryptor, err := c.getGCPDecryptor(ctx, gcpCfg)
		if err != nil {
			log.Warn("failed to create gcp kms client",
				zap.String("vendor", vendor),
				zap.String("keyID", keyID),
				zap.String("endpoint", gcpCfg.Endpoint),
				zap.Error(err))
			return nil, cerrors.ErrDecodeFailed.Wrap(err)
		}
		plaintext, err := decryptor.Decrypt(ctx, keyID, ciphertext)
		if err != nil {
			log.Warn("failed to decrypt master key via gcp kms",
				zap.String("vendor", vendor),
				zap.String("keyID", keyID),
				zap.String("endpoint", gcpCfg.Endpoint),
				zap.Error(err))
			return nil, cerrors.ErrDecodeFailed.Wrap(err)
		}
		return plaintext, nil
	default:
		log.Warn("unsupported KMS vendor",
			zap.String("vendor", vendor),
			zap.String("normalizedVendor", normalizedVendor),
			zap.String("keyID", keyID))
		return nil, cerrors.ErrDecodeFailed.GenWithStackByArgs("unsupported KMS vendor: " + vendor)
	}
}

func (c *client) Close() {
	c.gcpMu.Lock()
	defer c.gcpMu.Unlock()

	for _, cli := range c.gcpClients {
		_ = cli.Close()
	}
	clear(c.gcpClients)
}

const (
	vendorAWS = "aws"
	vendorGCP = "gcp"
)

func normalizeVendor(v string) string {
	v = strings.TrimSpace(v)
	v = strings.ToLower(v)
	switch v {
	case "aws-kms", "aws":
		return vendorAWS
	case "gcp-kms", "gcp":
		return vendorGCP
	default:
		return v
	}
}

func (c *client) validateConfig() error {
	if c.cfg == nil || c.cfg.KMS == nil {
		return nil
	}

	if c.cfg.KMS.AWS != nil {
		awsCfg := c.cfg.KMS.AWS
		if awsCfg.AccessKey != "" || awsCfg.SecretAccessKey != "" || awsCfg.SessionToken != "" {
			if awsCfg.AccessKey == "" || awsCfg.SecretAccessKey == "" {
				return cerrors.ErrEncryptionFailed.GenWithStackByArgs("aws kms access-key and secret-access-key must be set together")
			}
			if awsCfg.Profile != "" {
				return cerrors.ErrEncryptionFailed.GenWithStackByArgs("aws kms profile and static credentials are mutually exclusive")
			}
		}
	}

	if c.cfg.KMS.GCP != nil {
		gcpCfg := c.cfg.KMS.GCP
		if gcpCfg.CredentialsFile != "" && gcpCfg.CredentialsJSON != "" {
			return cerrors.ErrEncryptionFailed.GenWithStackByArgs("gcp kms credentials-file and credentials-json are mutually exclusive")
		}
	}

	return nil
}

func (c *client) resolveAWS(metaRegion, metaEndpoint string) awsClientConfig {
	resolved := awsClientConfig{
		Region:   metaRegion,
		Endpoint: metaEndpoint,
	}

	if c.cfg == nil || c.cfg.KMS == nil || c.cfg.KMS.AWS == nil {
		return resolved
	}
	awsCfg := c.cfg.KMS.AWS
	if awsCfg.Region != "" {
		resolved.Region = awsCfg.Region
	}
	if awsCfg.Endpoint != "" {
		resolved.Endpoint = awsCfg.Endpoint
	}
	resolved.Profile = awsCfg.Profile
	resolved.AccessKey = awsCfg.AccessKey
	resolved.SecretAccessKey = awsCfg.SecretAccessKey
	resolved.SessionToken = awsCfg.SessionToken
	return resolved
}

func (c *client) resolveGCP(metaEndpoint string) gcpClientConfig {
	resolved := gcpClientConfig{
		Endpoint: metaEndpoint,
	}

	if c.cfg == nil || c.cfg.KMS == nil || c.cfg.KMS.GCP == nil {
		return resolved
	}
	gcpCfg := c.cfg.KMS.GCP
	if gcpCfg.Endpoint != "" {
		resolved.Endpoint = gcpCfg.Endpoint
	}
	resolved.CredentialsFile = gcpCfg.CredentialsFile
	resolved.CredentialsJSON = gcpCfg.CredentialsJSON
	return resolved
}

func (c *client) getAWSDecryptor(ctx context.Context, cfg awsClientConfig) (awsDecryptor, error) {
	if c.awsFactory == nil {
		log.Error("aws kms decryptor factory is nil")
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("aws kms decryptor factory is nil")
	}

	key := awsClientKey{region: cfg.Region, endpoint: cfg.Endpoint}

	c.awsMu.Lock()
	if existing, ok := c.awsClients[key]; ok {
		c.awsMu.Unlock()
		return existing, nil
	}
	c.awsMu.Unlock()

	cli, err := c.awsFactory(ctx, cfg)
	if err != nil {
		log.Warn("failed to initialize aws kms decryptor",
			zap.String("region", cfg.Region),
			zap.String("endpoint", cfg.Endpoint),
			zap.Bool("hasProfile", cfg.Profile != ""),
			zap.Bool("hasStaticCredentials", cfg.AccessKey != ""),
			zap.Error(err))
		return nil, err
	}

	c.awsMu.Lock()
	defer c.awsMu.Unlock()
	if existing, ok := c.awsClients[key]; ok {
		return existing, nil
	}
	c.awsClients[key] = cli
	return cli, nil
}

func (c *client) getGCPDecryptor(ctx context.Context, cfg gcpClientConfig) (gcpDecryptor, error) {
	if c.gcpFactory == nil {
		log.Error("gcp kms decryptor factory is nil")
		return nil, cerrors.ErrEncryptionFailed.GenWithStackByArgs("gcp kms decryptor factory is nil")
	}

	key := gcpClientKey{endpoint: cfg.Endpoint}

	c.gcpMu.Lock()
	if existing, ok := c.gcpClients[key]; ok {
		c.gcpMu.Unlock()
		return existing, nil
	}
	c.gcpMu.Unlock()

	cli, err := c.gcpFactory(ctx, cfg)
	if err != nil {
		log.Warn("failed to initialize gcp kms decryptor",
			zap.String("endpoint", cfg.Endpoint),
			zap.Bool("hasCredentialsFile", cfg.CredentialsFile != ""),
			zap.Bool("hasCredentialsJSON", cfg.CredentialsJSON != ""),
			zap.Error(err))
		return nil, err
	}

	c.gcpMu.Lock()
	defer c.gcpMu.Unlock()
	if existing, ok := c.gcpClients[key]; ok {
		_ = cli.Close()
		return existing, nil
	}
	c.gcpClients[key] = cli
	return cli, nil
}
