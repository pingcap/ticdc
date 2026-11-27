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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metering

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/ticdc/pkg/errors"
)

// ValidateConfig performs local static validation of metering configuration
// without creating directories or dialing external storage endpoints.
func ValidateConfig(c *config.MeteringConfig) error {
	if c == nil || c.Type == "" {
		return nil
	}
	if c.SharedPoolID != "" &&
		(c.SharedPoolID == "." || c.SharedPoolID == ".." || strings.ContainsAny(c.SharedPoolID, "/\\\x00\r\n")) {
		return errors.ErrInvalidServerOption.GenWithStack("metering: shared-pool-id must be a single path component")
	}
	if c.Endpoint != "" {
		parsed, err := url.Parse(c.Endpoint)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return errors.ErrInvalidServerOption.GenWithStack("metering: endpoint must be a valid http or https URL with host")
		}
	}

	switch c.Type {
	case storage.ProviderTypeS3, storage.ProviderTypeOSS, storage.ProviderTypeCOS:
		if strings.TrimSpace(c.Bucket) == "" {
			return errors.ErrInvalidServerOption.GenWithStack(fmt.Sprintf("metering: bucket is required for %s storage", c.Type))
		}
		if strings.TrimSpace(c.Region) == "" {
			return errors.ErrInvalidServerOption.GenWithStack(fmt.Sprintf("metering: region is required for %s storage", c.Type))
		}
	case storage.ProviderTypeAzure:
		if strings.TrimSpace(c.Bucket) == "" {
			return errors.ErrInvalidServerOption.GenWithStack("metering: bucket is required for azure storage")
		}
		if c.Azure == nil || strings.TrimSpace(c.Azure.AccountName) == "" {
			return errors.ErrInvalidServerOption.GenWithStack("metering: azure account-name is required for azure storage")
		}
	case storage.ProviderTypeLocalFS:
		if c.LocalFS == nil || strings.TrimSpace(c.LocalFS.BasePath) == "" {
			return errors.ErrInvalidServerOption.GenWithStack("metering: localfs base-path is required for localfs storage")
		}
	default:
		return errors.ErrInvalidServerOption.GenWithStack(fmt.Sprintf("metering: unsupported storage type %q", c.Type))
	}
	return nil
}
