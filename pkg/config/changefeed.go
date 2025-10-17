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

package config

import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"net/url"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
)

// SortEngine is the sorter engine
type SortEngine = string

// sort engines
const (
	SortInMemory SortEngine = "memory"
	SortInFile   SortEngine = "file"
	SortUnified  SortEngine = "unified"
)

// FeedState represents the running state of a changefeed
type FeedState string

// All FeedStates
// Only `StateNormal` and `StatePending` changefeed is running,
// others are stopped.
const (
	StateNormal   FeedState = "normal"
	StatePending  FeedState = "pending"
	StateFailed   FeedState = "failed"
	StateStopped  FeedState = "stopped"
	StateRemoved  FeedState = "removed"
	StateFinished FeedState = "finished"
	StateWarning  FeedState = "warning"
	// StateUnInitialized is used for the changefeed that has not been initialized
	// it only exists in memory for a short time and will not be persisted to storage
	StateUnInitialized FeedState = ""
)

// ToInt return an int for each `FeedState`, only use this for metrics.
func (s FeedState) ToInt() int {
	switch s {
	case StateNormal:
		return 0
	case StatePending:
		return 1
	case StateFailed:
		return 2
	case StateStopped:
		return 3
	case StateFinished:
		return 4
	case StateRemoved:
		return 5
	case StateWarning:
		return 6
	case StateUnInitialized:
		return 7
	}
	// -1 for unknown feed state
	return -1
}

// IsNeeded return true if the given feedState matches the listState.
func (s FeedState) IsNeeded(need string) bool {
	if need == "all" {
		return true
	}
	if need == "" {
		switch s {
		case StateNormal:
			return true
		case StateStopped:
			return true
		case StateFailed:
			return true
		case StateWarning:
			return true
		case StatePending:
			return true
		}
	}
	return need == string(s)
}

// IsRunning return true if the feedState represents a running state.
func (s FeedState) IsRunning() bool {
	return s == StateNormal || s == StateWarning
}

// RunningError represents some running error from cdc components, such as processor.
type RunningError struct {
	Time    time.Time `json:"time"`
	Addr    string    `json:"addr"`
	Code    string    `json:"code"`
	Message string    `json:"message"`
}

// Value implements the driver.Valuer interface
func (e RunningError) Value() (driver.Value, error) {
	return json.Marshal(e)
}

// Scan implements the sql.Scanner interface
func (e *RunningError) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, e)
}

// AdminJobType represents for admin job type, both used in owner and processor
type AdminJobType int

// AdminJob holds an admin job
type AdminJob struct {
	CfID                  common.ChangeFeedID
	Type                  AdminJobType
	Error                 *RunningError
	OverwriteCheckpointTs uint64
}

// All AdminJob types
const (
	AdminNone AdminJobType = iota
	AdminStop
	AdminResume
	AdminRemove
	AdminFinish
)

// String implements fmt.Stringer interface.
func (t AdminJobType) String() string {
	switch t {
	case AdminNone:
		return "noop"
	case AdminStop:
		return "stop changefeed"
	case AdminResume:
		return "resume changefeed"
	case AdminRemove:
		return "remove changefeed"
	case AdminFinish:
		return "finish changefeed"
	}
	return "unknown"
}

// IsStopState returns whether changefeed is in stop state with give admin job
func (t AdminJobType) IsStopState() bool {
	switch t {
	case AdminStop, AdminRemove, AdminFinish:
		return true
	}
	return false
}

type ChangefeedConfig struct {
	ChangefeedID common.ChangeFeedID `json:"changefeed_id"`
	StartTS      uint64              `json:"start_ts"`
	TargetTS     uint64              `json:"target_ts"`
	SinkURI      string              `json:"sink_uri"`
	// timezone used when checking sink uri
	TimeZone      string `json:"timezone" default:"system"`
	CaseSensitive bool   `json:"case_sensitive" default:"false"`
	// if true, force to replicate some ineligible tables
	ForceReplicate bool          `json:"force_replicate" default:"false"`
	Filter         *FilterConfig `toml:"filter" json:"filter"`
	MemoryQuota    uint64        `toml:"memory-quota" json:"memory-quota"`
	// sync point related
	// TODO: Is syncPointRetention|default can be removed?
	EnableSyncPoint       bool          `json:"enable_sync_point" default:"false"`
	SyncPointInterval     time.Duration `json:"sync_point_interval" default:"1m"`
	SyncPointRetention    time.Duration `json:"sync_point_retention" default:"24h"`
	SinkConfig            *SinkConfig   `json:"sink_config"`
	EnableSplittableCheck bool          `json:"enable_splittable_check" default:"false"`
	// Epoch is the epoch of a changefeed, changes on every restart.
	Epoch   uint64 `json:"epoch"`
	BDRMode bool   `json:"bdr_mode" default:"false"`
	// redo releated
	Consistent *ConsistentConfig `toml:"consistent" json:"consistent,omitempty"`
}

// String implements fmt.Stringer interface, but hide some sensitive information
func (cfg *ChangefeedConfig) String() string {
	cloned := new(ChangefeedConfig)
	str, err := json.Marshal(cfg)
	if err != nil {
		log.Error("failed to marshal changefeed config", zap.Error(err))
		return ""
	}
	err = json.Unmarshal(str, cloned)
	if err != nil {
		log.Error("failed to unmarshal changefeed config", zap.Error(err))
		return ""
	}
	cloned.SinkConfig.MaskSensitiveData()
	res, err := json.Marshal(cloned)
	if err != nil {
		log.Error("failed to marshal changefeed config", zap.Error(err))
		return ""
	}
	return string(res)
}

// ChangeFeedInfo describes the detail of a ChangeFeed
type ChangeFeedInfo struct {
	ChangefeedID common.ChangeFeedID `json:"id"`
	UpstreamID   uint64              `json:"upstream-id"`
	SinkURI      string              `json:"sink-uri"`
	CreateTime   time.Time           `json:"create-time"`
	// Start sync at this commit ts if `StartTs` is specify or using the CreateTime of changefeed.
	StartTs uint64 `json:"start-ts"`
	// The ChangeFeed will exits until sync to timestamp TargetTs
	TargetTs uint64 `json:"target-ts"`
	// used for admin job notification, trigger watch event in capture
	AdminJobType AdminJobType `json:"admin-job-type"`
	Engine       SortEngine   `json:"sort-engine"`
	// SortDir is deprecated
	// it cannot be set by user in changefeed level, any assignment to it should be ignored.
	// but can be fetched for backward compatibility
	SortDir string `json:"sort-dir"`

	UpstreamInfo *UpstreamInfo  `json:"upstream-info"`
	Config       *ReplicaConfig `json:"config"`
	State        FeedState      `json:"state"`
	Error        *RunningError  `json:"error"`
	Warning      *RunningError  `json:"warning"`

	CreatorVersion string `json:"creator-version"`
	// Epoch is the epoch of a changefeed, changes on every restart.
	Epoch uint64 `json:"epoch"`
}

func (info *ChangeFeedInfo) ToChangefeedConfig() *ChangefeedConfig {
	return &ChangefeedConfig{
		ChangefeedID:          info.ChangefeedID,
		StartTS:               info.StartTs,
		TargetTS:              info.TargetTs,
		SinkURI:               info.SinkURI,
		CaseSensitive:         info.Config.CaseSensitive,
		ForceReplicate:        info.Config.ForceReplicate,
		SinkConfig:            info.Config.Sink,
		Filter:                info.Config.Filter,
		EnableSyncPoint:       util.GetOrZero(info.Config.EnableSyncPoint),
		SyncPointInterval:     util.GetOrZero(info.Config.SyncPointInterval),
		SyncPointRetention:    util.GetOrZero(info.Config.SyncPointRetention),
		EnableSplittableCheck: info.Config.Scheduler.EnableSplittableCheck,
		MemoryQuota:           info.Config.MemoryQuota,
		Epoch:                 info.Epoch,
		BDRMode:               util.GetOrZero(info.Config.BDRMode),
		TimeZone:              GetGlobalServerConfig().TZ,
		Consistent:            info.Config.Consistent,
		// other fields are not necessary for dispatcherManager
	}
}

// NeedBlockGC returns true if the changefeed need to block the GC safepoint.
// Note: if the changefeed is failed by GC, it should not block the GC safepoint.
func (info *ChangeFeedInfo) NeedBlockGC() bool {
	switch info.State {
	case StateNormal, StateStopped, StatePending, StateWarning:
		return true
	case StateFailed:
		return !info.isFailedByGC()
	case StateFinished, StateRemoved:
	default:
	}
	return false
}

func (info *ChangeFeedInfo) isFailedByGC() bool {
	if info.Error == nil {
		log.Panic("changefeed info is not consistent",
			zap.Any("state", info.State), zap.Any("error", info.Error))
	}
	return cerror.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(info.Error.Code))
}

// String implements fmt.Stringer interface, but hide some sensitive information
func (info *ChangeFeedInfo) String() (str string) {
	var err error
	str, err = info.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
		return
	}
	clone := new(ChangeFeedInfo)
	err = clone.Unmarshal([]byte(str))
	if err != nil {
		log.Error("failed to unmarshal changefeed info", zap.Error(err))
		return
	}

	clone.SinkURI = util.MaskSensitiveDataInURI(clone.SinkURI)
	if clone.Config != nil {
		clone.Config.MaskSensitiveData()
	}

	str, err = clone.Marshal()
	if err != nil {
		log.Error("failed to marshal changefeed info", zap.Error(err))
	}
	return
}

// GetStartTs returns StartTs if it's specified or using the
// CreateTime of changefeed.
func (info *ChangeFeedInfo) GetStartTs() uint64 {
	if info.StartTs > 0 {
		return info.StartTs
	}

	return oracle.GoTimeToTS(info.CreateTime)
}

// GetCheckpointTs returns CheckpointTs if it's specified in ChangeFeedStatus, otherwise StartTs is returned.
func (info *ChangeFeedInfo) GetCheckpointTs(status *ChangeFeedStatus) uint64 {
	if status != nil {
		return status.CheckpointTs
	}
	return info.GetStartTs()
}

// GetTargetTs returns TargetTs if it's specified, otherwise MaxUint64 is returned.
func (info *ChangeFeedInfo) GetTargetTs() uint64 {
	if info.TargetTs > 0 {
		return info.TargetTs
	}
	return uint64(math.MaxUint64)
}

// Marshal returns the json marshal format of a ChangeFeedInfo
func (info *ChangeFeedInfo) Marshal() (string, error) {
	return info.MarshalWithTruncation(true)
}

// MarshalWithTruncation allows controlling whether to truncate error messages
func (info *ChangeFeedInfo) MarshalWithTruncation(truncateError bool) (string, error) {
	var dataToMarshal interface{} = info

	if truncateError && info.Error != nil && len(info.Error.Message) > 100 {
		infoToMarshal := *info
		// Create a complete deep copy error message to avoid any data races
		errorCopy := *infoToMarshal.Error
		errorCopy.Message = errorCopy.Message[:100] + "..."
		infoToMarshal.Error = &errorCopy
		dataToMarshal = &infoToMarshal
	}

	data, err := json.Marshal(dataToMarshal)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal unmarshals into *ChangeFeedInfo from json marshal byte slice
func (info *ChangeFeedInfo) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, &info)
	if err != nil {
		return errors.Annotatef(
			cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
	}
	return nil
}

// Clone returns a cloned ChangeFeedInfo
func (info *ChangeFeedInfo) Clone() (*ChangeFeedInfo, error) {
	// Use MarshalWithTruncation(false) to preserve original data in clones
	s, err := info.MarshalWithTruncation(false)
	if err != nil {
		return nil, err
	}
	cloned := new(ChangeFeedInfo)
	err = cloned.Unmarshal([]byte(s))
	return cloned, err
}

// VerifyAndComplete verifies changefeed info and may fill in some fields.
// If a required field is not provided, return an error.
// If some necessary filed is missing but can use a default value, fill in it.
func (info *ChangeFeedInfo) VerifyAndComplete() {
	defaultConfig := GetDefaultReplicaConfig()
	if info.Config.Filter == nil {
		info.Config.Filter = defaultConfig.Filter
	}
	if info.Config.Mounter == nil {
		info.Config.Mounter = defaultConfig.Mounter
	}
	if info.Config.Sink == nil {
		info.Config.Sink = defaultConfig.Sink
	}
	if info.Config.Consistent == nil {
		info.Config.Consistent = defaultConfig.Consistent
	}
	if info.Config.Scheduler == nil {
		info.Config.Scheduler = defaultConfig.Scheduler
	}

	if info.Config.Integrity == nil {
		info.Config.Integrity = defaultConfig.Integrity
	}
	if info.Config.ChangefeedErrorStuckDuration == nil {
		info.Config.ChangefeedErrorStuckDuration = defaultConfig.ChangefeedErrorStuckDuration
	}
	if info.Config.SyncedStatus == nil {
		info.Config.SyncedStatus = defaultConfig.SyncedStatus
	}
	info.RmUnusedFields()
}

// RmUnusedFields removes unnecessary fields based on the downstream type and
// the protocol. Since we utilize a common changefeed configuration template,
// certain fields may not be utilized for certain protocols.
func (info *ChangeFeedInfo) RmUnusedFields() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn(
			"failed to parse the sink uri",
			zap.Error(err),
			zap.Any("sinkUri", info.SinkURI),
		)
		return
	}
	// blackhole is for testing purpose, no need to remove fields
	if IsBlackHoleScheme(uri.Scheme) {
		return
	}
	if !IsMQScheme(uri.Scheme) {
		info.rmMQOnlyFields()
	} else {
		// remove schema registry for MQ downstream with
		// protocol other than avro
		if util.GetOrZero(info.Config.Sink.Protocol) != ProtocolAvro.String() {
			info.Config.Sink.SchemaRegistry = nil
		}
	}

	if !IsStorageScheme(uri.Scheme) {
		info.rmStorageOnlyFields()
	}

	if !IsMySQLCompatibleScheme(uri.Scheme) {
		info.rmDBOnlyFields()
	} else {
		// remove fields only being used by MQ and Storage downstream
		info.Config.Sink.Protocol = nil
		info.Config.Sink.Terminator = nil
	}
}

func (info *ChangeFeedInfo) rmMQOnlyFields() {
	log.Info("since the downstream is not a MQ, remove MQ only fields",
		zap.String("keyspace", info.ChangefeedID.Keyspace()),
		zap.String("changefeed", info.ChangefeedID.Name()))
	info.Config.Sink.DispatchRules = nil
	info.Config.Sink.SchemaRegistry = nil
	info.Config.Sink.EncoderConcurrency = nil
	info.Config.Sink.OnlyOutputUpdatedColumns = nil
	info.Config.Sink.DeleteOnlyOutputHandleKeyColumns = nil
	info.Config.Sink.ContentCompatible = nil
	info.Config.Sink.KafkaConfig = nil
}

func (info *ChangeFeedInfo) rmStorageOnlyFields() {
	info.Config.Sink.CSVConfig = nil
	info.Config.Sink.DateSeparator = nil
	info.Config.Sink.EnablePartitionSeparator = nil
	info.Config.Sink.FileIndexWidth = nil
	info.Config.Sink.CloudStorageConfig = nil
}

func (info *ChangeFeedInfo) rmDBOnlyFields() {
	info.Config.EnableSyncPoint = nil
	info.Config.BDRMode = nil
	info.Config.SyncPointInterval = nil
	info.Config.SyncPointRetention = nil
	info.Config.Consistent = nil
	info.Config.Sink.SafeMode = nil
	info.Config.Sink.MySQLConfig = nil
}

// FixIncompatible fixes incompatible changefeed meta info.
func (info *ChangeFeedInfo) FixIncompatible() {
	creatorVersionGate := version.NewCreatorVersionGate(info.CreatorVersion)
	if creatorVersionGate.ChangefeedStateFromAdminJob() {
		log.Info("Start fixing incompatible changefeed state", zap.String("changefeed", info.String()))
		info.fixState()
		log.Info("Fix incompatibility changefeed state completed", zap.String("changefeed", info.String()))
	}

	if creatorVersionGate.ChangefeedAcceptUnknownProtocols() {
		log.Info("Start fixing incompatible changefeed MQ sink protocol", zap.String("changefeed", info.String()))
		info.fixMQSinkProtocol()
		log.Info("Fix incompatibility changefeed MQ sink protocol completed", zap.String("changefeed", info.String()))
	}

	if creatorVersionGate.ChangefeedAcceptProtocolInMysqlSinURI() {
		log.Info("Start fixing incompatible changefeed sink uri", zap.String("changefeed", info.String()))
		info.fixMySQLSinkProtocol()
		log.Info("Fix incompatibility changefeed sink uri completed", zap.String("changefeed", info.String()))
	}

	if info.Config.MemoryQuota == uint64(0) {
		log.Info("Start fixing incompatible memory quota", zap.String("changefeed", info.String()))
		info.fixMemoryQuota()
		log.Info("Fix incompatible memory quota completed", zap.String("changefeed", info.String()))
	}

	if info.Config.ChangefeedErrorStuckDuration == nil {
		log.Info("Start fixing incompatible error stuck duration", zap.String("changefeed", info.String()))
		info.Config.ChangefeedErrorStuckDuration = GetDefaultReplicaConfig().ChangefeedErrorStuckDuration
		log.Info("Fix incompatible error stuck duration completed", zap.String("changefeed", info.String()))
	}

	log.Info("Start fixing incompatible scheduler", zap.String("changefeed", info.String()))
	inheritV66 := creatorVersionGate.ChangefeedInheritSchedulerConfigFromV66()
	info.fixScheduler(inheritV66)
	log.Info("Fix incompatible scheduler completed", zap.String("changefeed", info.String()))
}

// fixState attempts to fix state loss from upgrading the old owner to the new owner.
func (info *ChangeFeedInfo) fixState() {
	// Notice: In the old owner we used AdminJobType field to determine if the task was paused or not,
	// we need to handle this field in the new owner.
	// Otherwise, we will see that the old version of the task is paused and then upgraded,
	// and the task is automatically resumed after the upgrade.
	state := info.State
	// Upgrading from an old owner, we need to deal with cases where the state is normal,
	// but actually contains errors and does not match the admin job type.
	if state == StateNormal {
		switch info.AdminJobType {
		// This corresponds to the case of failure or error.
		case AdminNone, AdminResume:
			if info.Error != nil {
				if cerror.IsChangefeedGCFastFailErrorCode(errors.RFCErrorCode(info.Error.Code)) {
					state = StateFailed
				} else {
					state = StateWarning
				}
			}
		case AdminStop:
			state = StateStopped
		case AdminFinish:
			state = StateFinished
		case AdminRemove:
			state = StateRemoved
		}
	}

	if state != info.State {
		log.Info("handle old owner inconsistent state",
			zap.String("oldState", string(info.State)),
			zap.String("adminJob", info.AdminJobType.String()),
			zap.String("newState", string(state)))
		info.State = state
	}
}

func (info *ChangeFeedInfo) fixMySQLSinkProtocol() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn("parse sink URI failed", zap.Error(err))
		// SAFETY: It is safe to ignore this unresolvable sink URI here,
		// as it is almost impossible for this to happen.
		// If we ignore it when fixing it after it happens,
		// it will expose the problem when starting the changefeed,
		// which is easier to troubleshoot than reporting the error directly in the bootstrap process.
		return
	}

	if IsMQScheme(uri.Scheme) {
		return
	}

	query := uri.Query()
	protocolStr := query.Get(ProtocolKey)
	if protocolStr != "" || info.Config.Sink.Protocol != nil {
		maskedSinkURI, _ := util.MaskSinkURI(info.SinkURI)
		log.Warn("sink URI or sink config contains protocol, but scheme is not mq",
			zap.String("sinkURI", maskedSinkURI),
			zap.String("protocol", protocolStr),
			zap.Any("sinkConfig", info.Config.Sink))
		// always set protocol of mysql sink to ""
		query.Del(ProtocolKey)
		info.updateSinkURIAndConfigProtocol(uri, "", query)
	}
}

func (info *ChangeFeedInfo) fixMQSinkProtocol() {
	uri, err := url.Parse(info.SinkURI)
	if err != nil {
		log.Warn("parse sink URI failed", zap.Error(err))
		return
	}

	if !IsMQScheme(uri.Scheme) {
		return
	}

	needsFix := func(protocolStr string) bool {
		_, err := ParseSinkProtocolFromString(protocolStr)
		// There are two cases:
		// 1. there is an error indicating that the old ticdc accepts
		//    a protocol that is not known. It needs to be fixed as open protocol.
		// 2. If it is default, then it needs to be fixed as open protocol.
		return err != nil || protocolStr == ProtocolDefault.String()
	}

	query := uri.Query()
	protocol := query.Get(ProtocolKey)
	openProtocol := ProtocolOpen.String()

	// The sinkURI always has a higher priority.
	if protocol != "" && needsFix(protocol) {
		query.Set(ProtocolKey, openProtocol)
		info.updateSinkURIAndConfigProtocol(uri, openProtocol, query)
		return
	}

	if needsFix(util.GetOrZero(info.Config.Sink.Protocol)) {
		log.Info("handle incompatible protocol from sink config",
			zap.String("oldProtocol", util.GetOrZero(info.Config.Sink.Protocol)),
			zap.String("fixedProtocol", openProtocol))
		info.Config.Sink.Protocol = util.AddressOf(openProtocol)
	}
}

func (info *ChangeFeedInfo) updateSinkURIAndConfigProtocol(uri *url.URL, newProtocol string, newQuery url.Values) {
	newRawQuery := newQuery.Encode()
	maskedURI, _ := util.MaskSinkURI(uri.String())
	log.Info("handle incompatible protocol from sink URI",
		zap.String("oldURI", maskedURI),
		zap.String("newProtocol", newProtocol))

	uri.RawQuery = newRawQuery
	fixedSinkURI := uri.String()
	info.SinkURI = fixedSinkURI
	info.Config.Sink.Protocol = util.AddressOf(newProtocol)
}

func (info *ChangeFeedInfo) fixMemoryQuota() {
	info.Config.FixMemoryQuota()
}

func (info *ChangeFeedInfo) fixScheduler(inheritV66 bool) {
	info.Config.FixScheduler(inheritV66)
}

type Progress int

var (
	ProgressNone     Progress = 1
	ProgressRemoving Progress = 2
	ProgressStopping Progress = 3
)

// ChangeFeedStatus stores information about a ChangeFeed
// It is stored in etcd.
type ChangeFeedStatus struct {
	CheckpointTs uint64 `json:"checkpoint-ts"`
	// Progress indicates changefeed progress status
	Progress Progress `json:"progress"`

	// MaintainerAddr is the address of the changefeed's maintainer
	// It is used to identify the changefeed's maintainer, and it is not stored in etcd.
	maintainerAddr string `json:"-"`
	// minTableBarrierTs is the minimum commitTs of all DDL events and is only
	// used to check whether there is a pending DDL job at the checkpointTs when
	// initializing the changefeed.
	MinTableBarrierTs uint64 `json:"min-table-barrier-ts"`
	// TODO: remove this filed after we don't use ChangeFeedStatus to
	// control processor. This is too ambiguous.
	AdminJobType AdminJobType `json:"admin-job-type"`
	// LastSyncedTs is the last synced max timestamp of the changefeed.
	// It is used to indicate the progress of the changefeed.
	// It is not stored in etcd.
	LastSyncedTs uint64 `json:"-"`
	// LogCoordinatorResolvedTs is the resolved timestamp from the log coordinator.
	LogCoordinatorResolvedTs uint64 `json:"-"`
}

// Marshal returns json encoded string of ChangeFeedStatus, only contains necessary fields stored in storage
func (status *ChangeFeedStatus) Marshal() (string, error) {
	data, err := json.Marshal(status)
	return string(data), cerror.WrapError(cerror.ErrMarshalFailed, err)
}

// Unmarshal into *ChangeFeedStatus from json marshal byte slice
func (status *ChangeFeedStatus) Unmarshal(data []byte) error {
	err := json.Unmarshal(data, status)
	return errors.Annotatef(
		cerror.WrapError(cerror.ErrUnmarshalFailed, err), "Unmarshal data: %v", data)
}

// GetMaintainerAddr returns the address of the changefeed's maintainer
func (status *ChangeFeedStatus) GetMaintainerAddr() string {
	return status.maintainerAddr
}

// SetMaintainerAddr sets the address of the changefeed's maintainer
func (status *ChangeFeedStatus) SetMaintainerAddr(addr string) {
	status.maintainerAddr = addr
}
