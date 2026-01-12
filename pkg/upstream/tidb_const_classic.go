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

//go:build !nextgen

package upstream

import (
	"github.com/pingcap/tidb/pkg/domain/infosync"
)

const (
	// topologyTiDB is /topology/tidb/{ip:port}.
	// Refer to https://github.com/pingcap/tidb/blob/release-7.5/pkg/domain/infosync/info.go#L78-L79.
	topologyTiDB    = infosync.TopologyInformationPath
	topologyTiDBTTL = infosync.TopologySessionTTL
)
