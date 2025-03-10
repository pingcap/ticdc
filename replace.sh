#!/usr/bin/env bash

find redo -name "*.go" -exec sed 's/model.Ts/common.Ts/g' {} -i \;
find redo -name "*.go" -exec sed 's/model.ChangeFeedID/common.ChangeFeedID/g' {} -i \;
find redo -name "*.go" -exec sed 's/model.RowChangedEvent/pevent.RowChangedEvent/g' {} -i \;
find redo -name "*.go" -exec sed 's/model.DDLEvent/pevent.DDLEvent/g' {} -i \;
find redo -name "*.go" -exec sed 's/model.RedoLog/pevent.RedoLog/g' {} -i \;
find redo -name "*.go" -exec sed 's/model.CaptureID/common.CaptureID/g' {} -i \;
find redo -name "*.go" -exec sed 's/tiflow\/cdc\/redo/ticdc\/redo/g' {} -i \;
find redo -name "*.go" -exec sed 's/RowChangedEvent/DMLEvent/g' {} -i \;
