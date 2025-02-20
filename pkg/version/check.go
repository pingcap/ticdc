// Copyright 2024 PingCAP, Inc.
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

package version

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/util/engine"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	pd "github.com/tikv/pd/client"
)

var (
	// MinTiKVVersion is the version of the minimal compatible TiKV.
	// The min version should be 7.5 for new arch.
	MinTiKVVersion = semver.New("7.5.0-alpha")
	// maxTiKVVersion is the version of the maximum compatible TiKV.
	// Compatible versions are in [MinTiKVVersion, maxTiKVVersion)
	maxTiKVVersion = semver.New("15.0.0")
)

var versionHash = regexp.MustCompile("-[0-9]+-g[0-9a-f]{7,}(-dev)?")

// SanitizeVersion remove the prefix "v" and suffix git hash.
func SanitizeVersion(v string) string {
	if v == "" {
		return v
	}
	v = versionHash.ReplaceAllLiteralString(v, "")
	v = strings.TrimSuffix(v, "-fips")
	v = strings.TrimSuffix(v, "-dirty")
	return strings.TrimPrefix(v, "v")
}

// CheckStoreVersion checks whether the given TiKV is compatible with this CDC.
// If storeID is 0, it checks all TiKV.
func CheckStoreVersion(ctx context.Context, client pd.Client, storeID uint64) error {
	failpoint.Inject("GetStoreFailed", func() {
		failpoint.Return(cerror.WrapError(cerror.ErrGetAllStoresFailed, fmt.Errorf("unknown store %d", storeID)))
	})
	var stores []*metapb.Store
	var err error
	if storeID == 0 {
		stores, err = client.GetAllStores(ctx, pd.WithExcludeTombstone())
	} else {
		stores = make([]*metapb.Store, 1)
		stores[0], err = client.GetStore(ctx, storeID)
	}
	if err != nil {
		return cerror.WrapError(cerror.ErrGetAllStoresFailed, err)
	}

	for _, s := range stores {
		if engine.IsTiFlash(s) {
			continue
		}

		ver, err := semver.NewVersion(SanitizeVersion(s.Version))
		if err != nil {
			err = errors.Annotate(err, "invalid TiKV version")
			return cerror.WrapError(cerror.ErrNewSemVersion, err)
		}
		minOrd := ver.Compare(*MinTiKVVersion)
		if minOrd < 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, the minimal compatible version is %s",
				SanitizeVersion(s.Version), MinTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
		maxOrd := ver.Compare(*maxTiKVVersion)
		if maxOrd >= 0 {
			arg := fmt.Sprintf("TiKV %s is not supported, only support version less than %s",
				SanitizeVersion(s.Version), maxTiKVVersion)
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(arg)
		}
	}
	return nil
}
