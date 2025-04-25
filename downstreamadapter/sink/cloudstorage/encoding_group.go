// Copyright 2025 PingCAP, Inc.
//
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

package cloudstorage

import (
	"context"

	commonType "github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/sink/codec"
	"github.com/pingcap/ticdc/pkg/sink/codec/common"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
)

const (
	defaultEncodingConcurrency = 8
	defaultChannelSize         = 1024
)

type encodingGroup struct {
	changeFeedID commonType.ChangeFeedID
	codecConfig  *common.Config

	concurrency int

	inputCh  <-chan eventFragment
	outputCh chan<- eventFragment

	closed *atomic.Bool
}

func newEncodingGroup(
	changefeedID commonType.ChangeFeedID,
	codecConfig *common.Config,
	concurrency int,
	inputCh <-chan eventFragment,
	outputCh chan<- eventFragment,
) *encodingGroup {
	return &encodingGroup{
		changeFeedID: changefeedID,
		codecConfig:  codecConfig,
		concurrency:  concurrency,
		inputCh:      inputCh,
		outputCh:     outputCh,

		closed: atomic.NewBool(false),
	}
}

func (eg *encodingGroup) Run(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < eg.concurrency; i++ {
		g.Go(func() error {
			return eg.runEncoder(ctx)
		})
	}
	return g.Wait()
}

func (eg *encodingGroup) runEncoder(ctx context.Context) error {
	encoder, err := codec.NewTxnEventEncoder(eg.codecConfig)
	if err != nil {
		return err
	}
	defer eg.closed.Store(true)
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case frag, ok := <-eg.inputCh:
			if !ok || eg.closed.Load() {
				return nil
			}
			err = encoder.AppendTxnEvent(frag.event)
			if err != nil {
				return err
			}
			frag.encodedMsgs = encoder.Build()

			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case eg.outputCh <- frag:
			}
		}
	}
}

func (eg *encodingGroup) close() {
	eg.closed.Store(true)
}
