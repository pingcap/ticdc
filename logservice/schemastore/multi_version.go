// Copyright 2024 PingCAP, Inc.
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

package schemastore

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"go.uber.org/zap"
)

type tableInfoItem struct {
	Version uint64
	Info    *common.TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	tableID int64

	// ordered by ts
	infos []*tableInfoItem

	deleteVersion uint64

	initialized bool

	pendingDDLs []PersistedDDLEvent

	// used to indicate whether the table info build is ready
	// must wait on it before reading table info from store
	readyToRead chan struct{}
}

func newEmptyVersionedTableInfoStore(tableID int64) *versionedTableInfoStore {
	return &versionedTableInfoStore{
		tableID:       tableID,
		infos:         make([]*tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
		initialized:   false,
		pendingDDLs:   make([]PersistedDDLEvent, 0),
		readyToRead:   make(chan struct{}),
	}
}

func (v *versionedTableInfoStore) addInitialTableInfo(info *common.TableInfo, version uint64) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// assertEmpty(v.infos)
	v.infos = append(v.infos, &tableInfoItem{Version: version, Info: info})
}

func (v *versionedTableInfoStore) getTableID() int64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.tableID
}

func (v *versionedTableInfoStore) setTableInfoInitialized() {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, job := range v.pendingDDLs {
		// log.Info("apply pending ddl",
		// 	zap.Int64("tableID", int64(v.tableID)),
		// 	zap.String("query", job.Query),
		// 	zap.Uint64("finishedTS", job.BinlogInfo.FinishedTS),
		// 	zap.Any("infosLen", len(v.infos)))
		v.doApplyDDL(&job)
	}
	v.initialized = true
	close(v.readyToRead)
}

func (v *versionedTableInfoStore) waitTableInfoInitialized() {
	<-v.readyToRead
}

type TableDeletedError struct{}

func (e *TableDeletedError) Error() string {
	return "table deleted"
}

// if ts < deleteVersion, return the table info with the largest version <= ts
// if ts >= deleteVersion, return nil table info and TableDeletedError
func (v *versionedTableInfoStore) getTableInfo(ts uint64) (*common.TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.initialized {
		log.Panic("should wait for table info initialized")
	}

	if ts >= v.deleteVersion {
		log.Warn("table info deleted",
			zap.Any("ts", ts),
			zap.Any("tableID", v.tableID),
			zap.Any("deleteVersion", v.deleteVersion),
			zap.Any("infos", v.infos))
		return nil, &TableDeletedError{}
	}

	target := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].Version > ts
	})
	if target == 0 {
		log.Error("no version found",
			zap.Any("ts", ts),
			zap.Any("tableID", v.tableID),
			zap.Any("deleteVersion", v.deleteVersion),
			zap.Any("infos", v.infos))
		return nil, errors.New("no version found")
	}
	return v.infos[target-1].Info, nil
}

// only keep one item with the largest version <= gcTS, return whether the store should be totally removed
func (v *versionedTableInfoStore) gc(gcTs uint64) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.initialized {
		return false
	}
	if len(v.infos) == 0 {
		log.Fatal("no table info found", zap.Int64("tableID", v.tableID))
	}

	if gcTs >= v.deleteVersion {
		return true
	}

	target := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].Version > gcTs
	})
	if target == 0 {
		return false
	}

	v.infos = v.infos[target-1:]
	if len(v.infos) == 0 {
		log.Panic("should not happen")
	}
	return false
}

func assertNonDeleted(v *versionedTableInfoStore) {
	if v.deleteVersion != uint64(math.MaxUint64) {
		log.Panic("shouldn't happen", zap.Uint64("deleteVersion", v.deleteVersion))
	}
}

func (v *versionedTableInfoStore) applyDDLFromPersistStorage(event *PersistedDDLEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.initialized {
		log.Panic("should not happen")
	}

	v.doApplyDDL(event)
}

func (v *versionedTableInfoStore) applyDDL(event *PersistedDDLEvent) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.initialized {
		// The usage of the parameter `event` may outlive the function call, so we copy it.
		v.pendingDDLs = append(v.pendingDDLs, *event)
		return
	}
	v.doApplyDDL(event)
}

// lock must be hold by the caller
func (v *versionedTableInfoStore) doApplyDDL(event *PersistedDDLEvent) {
	if len(v.infos) != 0 && event.FinishedTs <= v.infos[len(v.infos)-1].Version {
		log.Warn("already applied ddl, ignore it.",
			zap.Int64("tableID", v.tableID),
			zap.String("query", event.Query),
			zap.Uint64("finishedTS", event.FinishedTs),
			zap.Int("infosLen", len(v.infos)))
		return
	}
	ddlType := model.ActionType(event.Type)
	handler, ok := allDDLHandlers[ddlType]
	if !ok {
		log.Panic("unknown ddl type", zap.Any("ddlType", ddlType), zap.String("query", event.Query))
	}
	tableInfo, deleted := handler.extractTableInfoFunc(event, v.tableID)
	if tableInfo != nil {
		if ddlType == model.ActionRecoverTable {
			v.deleteVersion = math.MaxUint64
		} else {
			assertNonDeleted(v)
		}
		v.infos = append(v.infos, &tableInfoItem{Version: event.FinishedTs, Info: tableInfo})
	} else if deleted {
		v.deleteVersion = event.FinishedTs
	}
}
