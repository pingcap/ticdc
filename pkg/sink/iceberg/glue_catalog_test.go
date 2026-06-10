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

package iceberg

import (
	"context"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigApplyParsesStaticCredentialsForGlueCatalog(t *testing.T) {
	ctx := context.Background()
	sinkURI, err := url.Parse("s3://bucket/prefix?protocol=iceberg&catalog=glue&region=us-west-1&access-key=test-ak&secret-access-key=test-sk&session-token=test-token")
	require.NoError(t, err)

	cfg := NewConfig()
	err = cfg.Apply(ctx, sinkURI, nil)
	require.NoError(t, err)
	require.Equal(t, CatalogGlue, cfg.Catalog)
	require.Equal(t, "us-west-1", cfg.AWSRegion)
	require.Equal(t, "test-ak", cfg.AccessKey)
	require.Equal(t, "test-sk", cfg.SecretAccessKey)
	require.Equal(t, "test-token", cfg.SessionToken)
}

func TestResolveGlueAWSConfigUsesStaticCredentials(t *testing.T) {
	ctx := context.Background()
	writer := &TableWriter{
		cfg: &Config{
			Catalog:         CatalogGlue,
			AWSRegion:       "us-west-1",
			AccessKey:       "test-ak",
			SecretAccessKey: "test-sk",
			SessionToken:    "test-token",
		},
	}

	awsCfg, err := writer.resolveGlueAWSConfig(ctx)
	require.NoError(t, err)
	require.Equal(t, "us-west-1", awsCfg.Region)

	creds, err := awsCfg.Credentials.Retrieve(ctx)
	require.NoError(t, err)
	require.Equal(t, "test-ak", creds.AccessKeyID)
	require.Equal(t, "test-sk", creds.SecretAccessKey)
	require.Equal(t, "test-token", creds.SessionToken)
}

func TestConfigApplyRejectsIncompleteStaticCredentials(t *testing.T) {
	ctx := context.Background()
	sinkURI, err := url.Parse("s3://bucket/prefix?protocol=iceberg&catalog=glue&region=us-west-1&access-key=test-ak")
	require.NoError(t, err)

	cfg := NewConfig()
	err = cfg.Apply(ctx, sinkURI, nil)
	require.ErrorContains(t, err, "access-key and secret-access-key must be set together")
}

func TestConfigApplyRejectsSessionTokenWithoutStaticCredentials(t *testing.T) {
	ctx := context.Background()
	sinkURI, err := url.Parse("s3://bucket/prefix?protocol=iceberg&catalog=glue&region=us-west-1&session-token=test-token")
	require.NoError(t, err)

	cfg := NewConfig()
	err = cfg.Apply(ctx, sinkURI, nil)
	require.ErrorContains(t, err, "session-token requires access-key and secret-access-key")
}
