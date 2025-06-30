// Copyright 2025 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package redo

import (
	"sync/atomic"

	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/util"
)

// StatefulRts stores both the flushed and unflushed ts.
// unflushed stores ts in memory
// flushed stores ts on disk
type statefulRts struct {
	flushed   atomic.Uint64
	unflushed atomic.Uint64
}

func (s *statefulRts) getFlushed() common.Ts {
	return s.flushed.Load()
}

func (s *statefulRts) getUnflushed() common.Ts {
	return s.unflushed.Load()
}

func (s *statefulRts) checkAndSetUnflushed(unflushed common.Ts) (ok bool) {
	return util.CompareAndIncrease(&s.unflushed, unflushed)
}

func (s *statefulRts) checkAndSetFlushed(flushed common.Ts) (ok bool) {
	return util.CompareAndIncrease(&s.flushed, flushed)
}
