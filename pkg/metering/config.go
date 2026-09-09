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

package metering

import (
	"net/url"
	"strings"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/ticdc/pkg/errors"
)

// ValidateConfig validates an optional metering destination without contacting it.
// An absent type disables reporting. Credentials use the SDK provider chains.
func ValidateConfig(c *config.MeteringConfig) error {
	if c == nil || c.Type == "" {
		return nil
	}
	invalid := func(reason string) error {
		return errors.ErrInvalidServerOption.GenWithStack("metering: %s", reason)
	}
	switch c.Type {
	case storage.ProviderTypeS3, storage.ProviderTypeOSS:
		if c.Region == "" || c.Bucket == "" {
			return invalid("region and bucket are required")
		}
	case storage.ProviderTypeCOS:
		if (c.Region == "" && c.Endpoint == "") || c.Bucket == "" {
			return invalid("bucket and either region or endpoint are required")
		}
	case storage.ProviderTypeAzure:
		if c.Bucket == "" {
			return invalid("bucket is required")
		}
	case storage.ProviderTypeLocalFS:
		if c.LocalFS == nil || c.LocalFS.BasePath == "" {
			return invalid("localfs.base-path is required")
		}
	default:
		return invalid("unsupported storage type")
	}
	if c.SharedPoolID != "" && !validPathComponent(c.SharedPoolID) {
		return invalid("shared-pool-id must be a single path component")
	}
	if c.Endpoint != "" {
		u, err := url.Parse(c.Endpoint)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return invalid("endpoint must be an HTTP URL without credentials, query or fragment")
		}
	}
	return nil
}

func validPathComponent(s string) bool {
	return s != "" && s != "." && s != ".." && !strings.ContainsAny(s, "/\\\x00\r\n")
}

// RedactConfig returns an independent copy suitable for logs and the config API.
// Serialization used for configuration cloning must retain the credentials.
func RedactConfig(c *config.MeteringConfig) *config.MeteringConfig {
	if c == nil {
		return nil
	}
	clone := *c
	if c.AWS != nil {
		v := *c.AWS
		v.AccessKey, v.SecretAccessKey, v.SessionToken = "", "", ""
		clone.AWS = &v
	}
	if c.OSS != nil {
		v := *c.OSS
		v.AccessKey, v.SecretAccessKey, v.SessionToken = "", "", ""
		clone.OSS = &v
	}
	if c.COS != nil {
		v := *c.COS
		v.AccessKey, v.SecretAccessKey, v.SessionToken = "", "", ""
		clone.COS = &v
	}
	if c.Azure != nil {
		v := *c.Azure
		v.AccountKey, v.SASToken = "", ""
		clone.Azure = &v
	}
	if c.LocalFS != nil {
		v := *c.LocalFS
		clone.LocalFS = &v
	}
	// Also handle invalid configurations that may be logged before validation.
	if clone.Endpoint != "" {
		u, err := url.Parse(clone.Endpoint)
		if err != nil {
			clone.Endpoint = ""
		} else {
			u.User, u.RawQuery, u.Fragment = nil, "", ""
			clone.Endpoint = u.String()
		}
	}
	return &clone
}
