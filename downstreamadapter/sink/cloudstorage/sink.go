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

package cloudstorage

import (
	"context"
	"math"
	"net/url"
	"strings"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/downstreamadapter/sink/helper"
	"github.com/pingcap/ticdc/pkg/common"
	commonType "github.com/pingcap/ticdc/pkg/common"
	commonEvent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/sink/cloudstorage"
	"github.com/pingcap/ticdc/pkg/sink/util"
	putil "github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// It will send the events to cloud storage systems.
// Messages are encoded in the specific protocol and then sent to the defragmenter.
// The data flow is as follows: **data** -> encodingWorkers -> defragmenter -> dmlWorkers -> external storage
// The defragmenter will defragment the out-of-order encoded messages and sends encoded
// messages to individual dmlWorkers.
// The dmlWorkers will write the encoded messages to external storage in parallel between different tables.
type sink struct {
	changefeedID         commonType.ChangeFeedID
	scheme               string
	outputRawChangeEvent bool

	// workers defines a group of workers for writing events to external storage.
	dmlWorker *dmlWorker
	ddlWorker *ddlWorker

	statistics *metrics.Statistics

	isNormal uint32
}

func Verify(ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig) error {
	cfg := cloudstorage.NewConfig()
	err := cfg.Apply(ctx, sinkURI, sinkConfig)
	if err != nil {
		return err
	}
	protocol, err := helper.GetProtocol(putil.GetOrZero(sinkConfig.Protocol))
	if err != nil {
		return err
	}
	_, err = util.GetEncoderConfig(changefeedID, sinkURI, protocol, sinkConfig, math.MaxInt)
	if err != nil {
		return err
	}
	storage, err := helper.GetExternalStorageFromURI(ctx, sinkURI.String())
	if err != nil {
		return err
	}
	storage.Close()
	return nil
}

func New(
	ctx context.Context, changefeedID common.ChangeFeedID, sinkURI *url.URL, sinkConfig *config.SinkConfig,
	cleanupJobs []func(), /* only for test */
) (*sink, error) {
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
	s := &sink{
		changefeedID:         changefeedID,
		scheme:               strings.ToLower(sinkURI.Scheme),
		outputRawChangeEvent: sinkConfig.CloudStorageConfig.GetOutputRawChangeEvent(),
		statistics:           metrics.NewStatistics(changefeedID, "sink"),
	}

	s.dmlWorker, err = newDMLWorker(changefeedID, storage, cfg, encoderConfig, ext, s.statistics)
	if err != nil {
		return nil, err
	}
	s.ddlWorker = newDDLWorker(changefeedID, sinkURI, cfg, cleanupJobs, storage, s.statistics)
	return s, nil
}

func (s *sink) SinkType() common.SinkType {
	return common.CloudStorageSinkType
}

func (s *sink) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return s.dmlWorker.Run(ctx)
	})

	eg.Go(func() error {
		return s.ddlWorker.Run(ctx)
	})

	return eg.Wait()
}

func (s *sink) IsNormal() bool {
	return atomic.LoadUint32(&s.isNormal) == 1
}

func (s *sink) AddDMLEvent(event *commonEvent.DMLEvent) {
	s.dmlWorker.AddDMLEvent(event)
}

func (s *sink) WriteBlockEvent(event commonEvent.BlockEvent) error {
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
		log.Error("sink doesn't support Sync Point Event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("event", event))
	default:
		log.Error("sink doesn't support this type of block event",
			zap.String("namespace", s.changefeedID.Namespace()),
			zap.String("changefeed", s.changefeedID.Name()),
			zap.Any("eventType", event.GetType()))
	}
	return nil
}

func (s *sink) AddCheckpointTs(ts uint64) {
	s.ddlWorker.AddCheckpointTs(ts)
}

func (s *sink) SetTableSchemaStore(tableSchemaStore *util.TableSchemaStore) {
	s.ddlWorker.SetTableSchemaStore(tableSchemaStore)
}

func (s *sink) GetStartTsList(_ []int64, startTsList []int64, _ bool) ([]int64, []bool, error) {
	return startTsList, make([]bool, len(startTsList)), nil
}

func (s *sink) Close(_ bool) {
	s.dmlWorker.Close()
	s.ddlWorker.Close()
	if s.statistics != nil {
		s.statistics.Close()
	}
}
