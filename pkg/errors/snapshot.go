// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
package errors

import "github.com/pingcap/errors"

var ErrSnapshotBootstrap = errors.Normalize("snapshot bootstrap: %s", errors.RFCCodeText("CDC:ErrSnapshotBootstrap"))
