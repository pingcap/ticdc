// Copyright 2021 PingCAP, Inc.
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

package gc

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config/kerneltype"
	"github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const (
	// EnsureGCServiceCreating is a tag of GC service id for changefeed creation
	EnsureGCServiceCreating = "-creating-"
	// EnsureGCServiceResuming is a tag of GC service id for changefeed resumption
	EnsureGCServiceResuming = "-resuming-"
)

// PD leader switch may happen, so just gcServiceMaxRetries it.
// The default PD election timeout is 3 seconds. Triple the timeout as
// retry time to make sure PD leader can be elected during retry.
const (
	gcServiceBackoffDelay = 1000 // 1s
	gcServiceMaxRetries   = 9
)

// EnsureChangefeedStartTsSafety checks if the startTs less than the minimum of
// service GC safepoint and this function will update the service GC to startTs
func EnsureChangefeedStartTsSafety(
	ctx context.Context, client Client,
	gcServiceIDPrefix string,
	keyspaceID uint32,
	changefeedID common.ChangeFeedID,
	TTL int64, startTs uint64,
) error {
	gcServiceID := gcServiceIDPrefix + changefeedID.Keyspace() + "_" + changefeedID.Name()
	if kerneltype.IsClassic() {
		return ensureChangefeedStartTsSafetyClassic(ctx, client, gcServiceID, TTL, startTs)
	}
	return ensureChangefeedStartTsSafetyNextGen(ctx, client, keyspaceID, gcServiceID, TTL, startTs)
}

// UndoEnsureChangefeedStartTsSafety cleans the service GC safepoint of a changefeed
// if something goes wrong after successfully calling EnsureChangefeedStartTsSafety().
func UndoEnsureChangefeedStartTsSafety(
	ctx context.Context, client Client,
	keyspaceID uint32,
	gcServiceIDPrefix string,
	changefeedID common.ChangeFeedID,
) error {
	gcServiceID := gcServiceIDPrefix + changefeedID.Keyspace() + "_" + changefeedID.Name()
	err := UnifyDeleteGcSafepoint(ctx, client, keyspaceID, gcServiceID)
	if err != nil {
		log.Warn("undo ensure changefeed start ts safety failed", zap.String("gcServiceID", gcServiceID), zap.Error(err))
		return err
	}
	log.Info("undo ensure changefeed start ts safety", zap.String("gcServiceID", gcServiceID))
	return nil
}

func SetServiceGCSafepoint(ctx context.Context, client Client, keyspaceID uint32, serviceID string, TTL int64, safepoint uint64) error {
	if kerneltype.IsClassic() {
		_, err := setServiceGCSafepoint(ctx, client, serviceID, TTL, safepoint)
		return err
	}
	return setGCBarrier(ctx, client, keyspaceID, serviceID, safepoint, TTL)
}

// UnifyGetServiceGCSafepoint returns a service gc safepoint on classic mode or a gc barrier on next-gen mode
func UnifyGetServiceGCSafepoint(ctx context.Context, client Client, keyspaceID uint32) (uint64, error) {
	if kerneltype.IsClassic() {
		return getServiceGCSafepoint(ctx, client)
	}

	gcState, err := getGCState(ctx, client, keyspaceID)
	if err != nil {
		return 0, errors.WrapError(errors.ErrGetGCBarrierFailed, err)
	}
	return gcState.TxnSafePoint, nil
}

// UnifyDeleteGcSafepoint delete a gc safepoint on classic mode or delete a gc barrier on next-gen mode
func UnifyDeleteGcSafepoint(ctx context.Context, client Client, keyspaceID uint32, serviceID string) error {
	if kerneltype.IsClassic() {
		return removeServiceGCSafepoint(ctx, client, serviceID)
	}
	return deleteGCBarrier(ctx, client, keyspaceID, serviceID)
}
