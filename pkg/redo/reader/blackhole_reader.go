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

package reader

import (
	"context"

	commonType "github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	misc "github.com/pingcap/ticdc/pkg/redo/common"
)

// BlackHoleReader is a blockHole storage which implements LogReader interface
type BlackHoleReader struct{}

// newBlackHoleReader creates a new BlackHoleReader
func newBlackHoleReader() *BlackHoleReader {
	return &BlackHoleReader{}
}

// Run implements LogReader.Run
func (br *BlackHoleReader) Run(ctx context.Context) error {
	return nil
}

// ReadNextRow implements LogReader.ReadNextRow
func (br *BlackHoleReader) ReadNextRow(ctx context.Context) (*pevent.RedoDMLEvent, error) {
	return &pevent.RedoDMLEvent{}, nil
}

// ReadNextDDL implements LogReader.ReadNextDDL
func (br *BlackHoleReader) ReadNextDDL(ctx context.Context) (*pevent.RedoDDLEvent, error) {
	return &pevent.RedoDDLEvent{}, nil
}

// ReadMeta implements LogReader.ReadMeta
func (br *BlackHoleReader) ReadMeta(ctx context.Context) (checkpointTs, resolvedTs uint64, version int, err error) {
	return 0, 1, misc.Version, nil
}

// GetChangefeedID implements LogReader.GetChangefeedID
func (br *BlackHoleReader) GetChangefeedID() commonType.ChangeFeedID {
	return commonType.ChangeFeedID{}
}

// GetChangefeedID implements LogReader.GetChangefeedID
func (br *BlackHoleReader) GetVersion() int {
	return misc.Version
}

// Close implement the Close interface
func (br *BlackHoleReader) Close() error {
	return nil
}
