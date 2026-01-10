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

//go:build nextgen

package pdtype

import (
	pd "github.com/tikv/pd/client"
	pdopt "github.com/tikv/pd/client/opt"
)

type (
	GetStoreOption = pdopt.GetStoreOption
)

var (
	WithGRPCDialOptions     = pdopt.WithGRPCDialOptions
	WithForwardingOption    = pdopt.WithForwardingOption
	WithCustomTimeoutOption = pdopt.WithCustomTimeoutOption
	WithMaxErrorRetry       = pdopt.WithMaxErrorRetry
	WithExcludeTombstone    = pdopt.WithExcludeTombstone
	NewClientWithContext    = pd.NewClientWithContext
)
