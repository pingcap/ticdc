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

package watcher

import (
	"context"

	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/consumer"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/recorder"
	"github.com/pingcap/ticdc/cmd/multi-cluster-consistency-checker/types"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type S3Watcher struct {
	checkpointWatcher Watcher
	consumer          *consumer.S3Consumer
}

func NewS3Watcher(
	checkpointWatcher Watcher,
	s3Storage storage.ExternalStorage,
	tables map[string][]string,
) *S3Watcher {
	consumer := consumer.NewS3Consumer(s3Storage, tables)
	return &S3Watcher{
		checkpointWatcher: checkpointWatcher,
		consumer:          consumer,
	}
}

func (sw *S3Watcher) Close() {
	sw.checkpointWatcher.Close()
}

func (sw *S3Watcher) InitializeFromCheckpoint(ctx context.Context, clusterID string, checkpoint *recorder.Checkpoint) (map[cloudstorage.DmlPathKey]types.IncrementalData, error) {
	return sw.consumer.InitializeFromCheckpoint(ctx, clusterID, checkpoint)
}

func (sw *S3Watcher) AdvanceS3CheckpointTs(ctx context.Context, minCheckpointTs uint64) (uint64, error) {
	checkpointTs, err := sw.checkpointWatcher.AdvanceCheckpointTs(ctx, minCheckpointTs)
	if err != nil {
		return 0, errors.Annotate(err, "advance s3 checkpoint timestamp failed")
	}

	return checkpointTs, nil
}

func (sw *S3Watcher) ConsumeNewFiles(
	ctx context.Context,
) (map[cloudstorage.DmlPathKey]types.IncrementalData, map[types.SchemaTableKey]types.VersionKey, error) {
	// TODO: get the index updated from the s3
	newData, maxVersionMap, err := sw.consumer.ConsumeNewFiles(ctx)
	if err != nil {
		return nil, nil, errors.Annotate(err, "consume new files failed")
	}
	return newData, maxVersionMap, nil
}
