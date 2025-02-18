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

package sink

import (
	"context"
	"math"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/downstreamadapter/worker"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/util"
	putil "github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// It will send the events to cloud storage systems.
// Messages are encoded in the specific protocol and then sent to the defragmenter.
// The data flow is as follows: **data** -> encodingWorkers -> defragmenter -> dmlWorkers -> external storage
// The defragmenter will defragment the out-of-order encoded messages and sends encoded
// messages to individual dmlWorkers.
// The dmlWorkers will write the encoded messages to external storage in parallel between different tables.
type CloudStorageSink struct {
	changefeedID         commonType.ChangeFeedID
	scheme               string
	outputRawChangeEvent bool

	// workers defines a group of workers for writing events to external storage.
	dmlWorker *worker.CloudStorageDMLWorker
	ddlWorker *worker.CloudStorageDDLWorker

	statistics *metrics.Statistics

	isNormal uint32
}

func verifyCloudStorageSink(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	var (
		protocol config.Protocol
		storage  storage.ExternalStorage
		err      error
	)
	cfg := cloudstorage.NewConfig()
	if err = cfg.Apply(ctx, sinkURI, sinkConfig); err != nil {
		return err
	}
	if protocol, err = helper.GetProtocol(putil.GetOrZero(sinkConfig.Protocol)); err != nil {
		return err
	}
	if _, err = util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt); err != nil {
		return err
	}
	if storage, err = helper.GetExternalStorageFromURI(ctx, sinkURI.String()); err != nil {
		return err
	}
	storage.Close()
	return nil
}

func newCloudStorageSink(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
	cleanupJobs []func(), /* only for test */
) (*CloudStorageSink, error) {
	// create cloud storage config and then apply the params of sinkURI to it.
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return nil, err
	}
	// fetch protocol from replicaConfig defined by changefeed config file.
	protocol, err := helper.GetProtocol(
		putil.GetOrZero(sinkConfig.Protocol),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get cloud storage file extension according to the specific protocol.
	ext := helper.GetFileExtension(protocol)
	// the last param maxMsgBytes is mainly to limit the size of a single message for
	// batch protocols in mq scenario. In cloud storage sink, we just set it to max int.
	encoderConfig, err := util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return nil, errors.Trace(err)
	}
	storage, err := helper.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return nil, err
	}
	s := &CloudStorageSink{
		changefeedID:         changefeedID,
		scheme:               strings.ToLower(sinkURI.Scheme),
		outputRawChangeEvent: sinkConfig.CloudStorageConfig.GetOutputRawChangeEvent(),
		statistics:           metrics.NewStatistics(changefeedID, "CloudStorageSink"),
	}

	s.dmlWorker, err = worker.NewCloudStorageDMLWorker(changefeedID, storage, cfg, encoderConfig, ext, s.statistics)
	if err != nil {
		return nil, err
	}
	s.ddlWorker = worker.NewCloudStorageDDLWorker(changefeedID, sinkURI, cfg, cleanupJobs, storage, s.statistics)
	return s, nil
}

func (s *CloudStorageSink) SinkType() common.SinkType {
	return common.CloudStorageSinkType
}

func (s *CloudStorageSink) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.dmlWorker.Run(ctx)
	})

	eg.Go(func() error {
		return s.ddlWorker.Run(ctx)
	})

	return eg.Wait()
}

func (s *CloudStorageSink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *CloudStorageSink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWorker.AddDMLEvent(event)
}

func (s *CloudStorageSink) PassBlockEvent(event commonEvent.BlockEvent) {
	event.PostFlush()
}

func (s *CloudStorageSink) WriteBlockEvent(event commonEvent.BlockEvent) error {
	switch e := event.(type) {
	case *commonEvent.DDLEvent:
		if e.TiDBOnly {
			// run callback directly and return
			e.PostFlush()
			return nil
		}
		err := s.ddlWorker.WriteBlockEvent(e)
		if err != nil {
			atomic.StoreUint32(&s.isNormal, 0)
			return errors.Trace(err)
		}
	case *commonEvent.SyncPointEvent:
		log.Error("CloudStorageSink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("CloudStorageSink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *CloudStorageSink) AddCheckpointTs(ts uint64) {
	s.ddlWorker.AddCheckpointTs(ts)
}

func (s *CloudStorageSink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *CloudStorageSink) Close(_ bool) {
	s.dmlWorker.Close()
	s.ddlWorker.Close()
	if s.statistics != nil {
		s.statistics.Close()
	}
}
