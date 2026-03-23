//  Copyright 2023 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

package file

import (
	"context"
	"testing"

	"github.com/pingcap/ticdc/pkg/common"
	pevent "github.com/pingcap/ticdc/pkg/common/event"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/redo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestLogWriterWriteDDL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		ctx       context.Context
		ddl       *pevent.DDLEvent
		isRunning bool
		writerErr error
		wantErr   error
	}{
		{
			name:      "happy",
			ctx:       context.Background(),
			ddl:       &pevent.DDLEvent{FinishedTs: 1},
			isRunning: true,
			writerErr: nil,
		},
		{
			name:      "writer err",
			ctx:       context.Background(),
			ddl:       &pevent.DDLEvent{FinishedTs: 1},
			writerErr: errors.New("err"),
			wantErr:   errors.New("err"),
			isRunning: true,
		},
		{
			name:      "ddl nil",
			ctx:       context.Background(),
			ddl:       nil,
			writerErr: errors.New("err"),
			isRunning: true,
		},
		{
			name:      "isStopped",
			ctx:       context.Background(),
			ddl:       &pevent.DDLEvent{FinishedTs: 1},
			writerErr: errors.ErrRedoWriterStopped,
			isRunning: false,
			wantErr:   errors.ErrRedoWriterStopped,
		},
		{
			name:      "context cancel",
			ctx:       context.Background(),
			ddl:       &pevent.DDLEvent{FinishedTs: 1},
			writerErr: nil,
			isRunning: true,
			wantErr:   context.Canceled,
		},
	}

	for _, tt := range tests {
		mockWriter := &mockFileWriter{}
		mockWriter.On("IsRunning").Return(tt.isRunning)
		mockWriter.On("SyncWrite", mock.Anything).Return(tt.writerErr)
		w := ddlWriter{logWriter: &logWriter{
			cfg:           newTestWriterConfig(t, common.ChangeFeedID{}, nil),
			backendWriter: mockWriter,
			fileType:      redo.RedoDDLLogFileType,
		}}

		if tt.name == "context cancel" {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			tt.ctx = ctx
		}

		err := w.WriteDDLEvent(tt.ctx, tt.ddl)
		if tt.wantErr != nil {
			require.Equal(t, tt.wantErr.Error(), err.Error(), tt.name)
		} else {
			require.Nil(t, err, tt.name)
		}
	}
}
