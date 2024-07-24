package schemastore

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/flowbehappy/tigate/pkg/common"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/model"
	"go.uber.org/zap"
)

type tableInfoItem struct {
	version common.Ts
	info    *common.TableInfo
}

type versionedTableInfoStore struct {
	mu sync.Mutex

	tableID common.TableID

	// dispatcherID -> max ts successfully send to dispatcher
	// gcTS = min(dispatchers[dispatcherID])
	// when gc, just need retain one version <= gcTS
	dispatchers map[common.DispatcherID]common.Ts

	// ordered by ts
	infos []*tableInfoItem

	deleteVersion common.Ts

	initialized bool

	pendingDDLs []*model.Job

	// used to indicate whether the table info build is ready
	// must wait on it before reading table info from store
	readyToRead chan struct{}
}

func newEmptyVersionedTableInfoStore(tableID common.TableID) *versionedTableInfoStore {
	return &versionedTableInfoStore{
		tableID:       tableID,
		dispatchers:   make(map[common.DispatcherID]common.Ts),
		infos:         make([]*tableInfoItem, 0),
		deleteVersion: math.MaxUint64,
		initialized:   false,
		pendingDDLs:   make([]*model.Job, 0),
		readyToRead:   make(chan struct{}),
	}
}

func (v *versionedTableInfoStore) addInitialTableInfo(info *common.TableInfo) {
	v.mu.Lock()
	defer v.mu.Unlock()
	assertEmpty(v.infos)
	v.infos = append(v.infos, &tableInfoItem{version: common.Ts(info.Version), info: info})
}

func (v *versionedTableInfoStore) getTableID() common.TableID {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.tableID
}

func (v *versionedTableInfoStore) setTableInfoInitialized() {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, job := range v.pendingDDLs {
		v.doApplyDDL(job)
	}
	v.initialized = true
	close(v.readyToRead)
}

func (v *versionedTableInfoStore) waitTableInfoInitialized() {
	<-v.readyToRead
}

func (v *versionedTableInfoStore) getFirstVersion() common.Ts {
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(v.infos) == 0 {
		return math.MaxUint64
	}
	return v.infos[0].version
}

// return the table info with the largest version <= ts
func (v *versionedTableInfoStore) getTableInfo(ts common.Ts) (*common.TableInfo, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if !v.initialized {
		log.Panic("should wait for table info initialized")
	}

	if ts >= v.deleteVersion {
		return nil, errors.New("table info deleted")
	}

	target := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].version > ts
	})
	if target == 0 {
		log.Error("no version found",
			zap.Any("ts", ts),
			zap.Any("tableID", v.tableID),
			zap.Any("infos", v.infos),
			zap.Any("deleteVersion", v.deleteVersion))
		return nil, errors.New("no version found")
	}
	return v.infos[target-1].info, nil
}

// only keep one item with the largest version <= gcTS
func removeUnusedInfos(infos []*tableInfoItem, dispatchers map[common.DispatcherID]common.Ts) []*tableInfoItem {
	if len(infos) == 0 {
		log.Fatal("no table info found")
	}

	gcTS := common.Ts(math.MaxUint64)
	for _, ts := range dispatchers {
		if ts < gcTS {
			gcTS = ts
		}
	}

	target := sort.Search(len(infos), func(i int) bool {
		return infos[i].version > gcTS
	})
	// TODO: all info version is larger than gcTS seems impossible?
	if target == 0 {
		return infos
	}

	return infos[target-1:]
}

func (v *versionedTableInfoStore) registerDispatcher(dispatcherID common.DispatcherID, ts common.Ts) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if _, ok := v.dispatchers[dispatcherID]; ok {
		log.Info("dispatcher already registered", zap.Any("dispatcherID", dispatcherID))
	}
	v.dispatchers[dispatcherID] = ts
}

// return true when the store can be removed(no registered dispatchers)
func (v *versionedTableInfoStore) unregisterDispatcher(dispatcherID common.DispatcherID) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	delete(v.dispatchers, dispatcherID)
	if len(v.dispatchers) == 0 {
		return true
	}
	v.infos = removeUnusedInfos(v.infos, v.dispatchers)
	return false
}

func (v *versionedTableInfoStore) updateDispatcherSendTS(dispatcherID common.DispatcherID, ts common.Ts) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if oldTS, ok := v.dispatchers[dispatcherID]; !ok {
		log.Error("dispatcher cannot be found when update send ts",
			zap.Any("dispatcherID", dispatcherID), zap.Any("ts", ts))
		return errors.New("dispatcher not found")
	} else {
		if ts < oldTS {
			log.Error("send ts should be monotonically increasing",
				zap.Any("oldTS", oldTS), zap.Any("newTS", ts))
			return errors.New("send ts should be monotonically increasing")
		}
	}
	v.dispatchers[dispatcherID] = ts
	v.infos = removeUnusedInfos(v.infos, v.dispatchers)
	return nil
}

func assertEmpty(infos []*tableInfoItem) {
	if len(infos) != 0 {
		log.Panic("shouldn't happen")
	}
}

func assertNonEmpty(infos []*tableInfoItem) {
	if len(infos) == 0 {
		log.Panic("shouldn't happen")
	}
}

func assertNonDeleted(v *versionedTableInfoStore) {
	if v.deleteVersion != common.Ts(math.MaxUint64) {
		log.Panic("shouldn't happen")
	}
}

func (v *versionedTableInfoStore) applyDDL(job *model.Job) {
	v.mu.Lock()
	defer v.mu.Unlock()
	// delete table should not receive more ddl
	assertNonDeleted(v)

	if !v.initialized {
		v.pendingDDLs = append(v.pendingDDLs, job)
		return
	}
	v.doApplyDDL(job)
}

// lock must be hold by the caller
func (v *versionedTableInfoStore) doApplyDDL(job *model.Job) {
	if len(v.infos) != 0 && common.Ts(job.BinlogInfo.FinishedTS) <= v.infos[len(v.infos)-1].version {
		log.Panic("ddl job finished ts should be monotonically increasing")
	}
	if len(v.infos) > 0 {
		if common.Ts(job.BinlogInfo.FinishedTS) <= v.infos[len(v.infos)-1].version {
			log.Info("ignore job",
				zap.Int64("tableID", int64(v.tableID)),
				zap.String("query", job.Query),
				zap.Uint64("finishedTS", job.BinlogInfo.FinishedTS),
				zap.Any("infos", v.infos))
			return
		}
	}

	switch job.Type {
	case model.ActionCreateTable:
		assertEmpty(v.infos)
		info := common.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
		v.infos = append(v.infos, &tableInfoItem{version: common.Ts(job.BinlogInfo.FinishedTS), info: info})
	case model.ActionRenameTable:
		assertNonEmpty(v.infos)
		info := common.WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
		v.infos = append(v.infos, &tableInfoItem{version: common.Ts(job.BinlogInfo.FinishedTS), info: info})
	case model.ActionDropTable, model.ActionTruncateTable:
		v.deleteVersion = common.Ts(job.BinlogInfo.FinishedTS)
	default:
		// TODO: idenitify unexpected ddl or specify all expected ddl
	}
}

func (v *versionedTableInfoStore) copyRegisteredDispatchers(src *versionedTableInfoStore) {
	v.mu.Lock()
	src.mu.Lock()
	defer func() {
		v.mu.Unlock()
		src.mu.Unlock()
	}()
	if src.tableID != v.tableID {
		log.Panic("tableID not match")
	}
	for dispatcherID, ts := range src.dispatchers {
		if _, ok := v.dispatchers[dispatcherID]; ok {
			log.Panic("dispatcher already registered")
		}
		v.dispatchers[dispatcherID] = ts
	}
}

func (v *versionedTableInfoStore) checkAndCopyTailFrom(src *versionedTableInfoStore) {
	v.mu.Lock()
	src.mu.Lock()
	defer func() {
		v.mu.Unlock()
		src.mu.Unlock()
	}()
	if src.tableID != v.tableID {
		log.Panic("tableID not match")
	}
	if len(src.infos) == 0 {
		return
	}
	if len(v.infos) == 0 {
		v.infos = append(v.infos, src.infos[len(src.infos)-1])
	}
	// Check if the overlapping parts have the same common.Ts
	startCheckIndexInDest := sort.Search(len(v.infos), func(i int) bool {
		return v.infos[i].version >= src.infos[0].version
	})
	for i := startCheckIndexInDest; i < len(v.infos); i++ {
		if v.infos[i].version != src.infos[i-startCheckIndexInDest].version {
			log.Panic("version not match")
		}
	}

	startCopyIndexInSrc := len(v.infos) - startCheckIndexInDest
	v.infos = append(v.infos, src.infos[startCopyIndexInSrc:]...)

	v.deleteVersion = src.deleteVersion
}
