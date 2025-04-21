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

package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
)

const timeFormat = `"2006-01-02 15:04:05.000"`

// JSONTime used to wrap time into json format
type JSONTime time.Time

// MarshalJSON used to specify the time format
func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := time.Time(t).Format(timeFormat)
	return []byte(stamp), nil
}

// UnmarshalJSON is used to parse time.Time from bytes. The builtin json.Unmarshal function cannot unmarshal
// a date string formatted as "2006-01-02 15:04:05.000", so we must implement a customized unmarshal function.
func (t *JSONTime) UnmarshalJSON(data []byte) error {
	tm, err := time.Parse(timeFormat, string(data))
	if err != nil {
		return err
	}

	*t = JSONTime(tm)
	return nil
}

// HTTPError of cdc http api
type HTTPError struct {
	Error string `json:"error_msg"`
	Code  string `json:"error_code"`
}

// NewHTTPError wrap a err into HTTPError
func NewHTTPError(err error) HTTPError {
	errCode, _ := cerror.RFCCode(err)
	return HTTPError{
		Error: err.Error(),
		Code:  string(errCode),
	}
}

// httpBadRequestError is some errors that will cause a BadRequestError in http handler
var httpBadRequestError = []*errors.Error{
	cerror.ErrAPIInvalidParam, cerror.ErrSinkURIInvalid, cerror.ErrStartTsBeforeGC,
	cerror.ErrChangeFeedNotExists, cerror.ErrTargetTsBeforeStartTs, cerror.ErrTableIneligible,
	cerror.ErrFilterRuleInvalid, cerror.ErrChangefeedUpdateRefused, cerror.ErrMySQLConnectionError,
	cerror.ErrMySQLInvalidConfig, cerror.ErrCaptureNotExist, cerror.ErrSchedulerRequestFailed,
}

const (
	// OpVarAdminJob is the key of admin job in HTTP API
	OpVarAdminJob = "admin-job"
	// OpVarChangefeedID is the key of changefeed ID in HTTP API
	OpVarChangefeedID = "cf-id"
	// OpVarTargetCaptureID is the key of to-capture ID in HTTP API
	OpVarTargetCaptureID = "target-cp-id"
	// OpVarTableID is the key of table ID in HTTP API
	OpVarTableID = "table-id"

	// APIOpVarChangefeedState is the key of changefeed state in HTTP API.
	APIOpVarChangefeedState = "state"
	// APIOpVarChangefeedID is the key of changefeed ID in HTTP API.
	APIOpVarChangefeedID = "changefeed_id"
	// APIOpVarCaptureID is the key of capture ID in HTTP API.
	APIOpVarCaptureID = "capture_id"
	// APIOpVarNamespace is the key of changefeed namespace in HTTP API.
	APIOpVarNamespace = "namespace"
	// APIOpVarTiCDCUser is the key of ticdc user in HTTP API.
	APIOpVarTiCDCUser = "user"
	// APIOpVarTiCDCPassword is the key of ticdc password in HTTP API.
	APIOpVarTiCDCPassword = "password"
)

// IsHTTPBadRequestError check if a error is a http bad request error
func IsHTTPBadRequestError(err error) bool {
	if err == nil {
		return false
	}
	for _, e := range httpBadRequestError {
		if e.Equal(err) {
			return true
		}

		rfcCode, ok := cerror.RFCCode(err)
		if ok && e.RFCCode() == rfcCode {
			return true
		}

		if strings.Contains(err.Error(), string(e.RFCCode())) {
			return true
		}
	}
	return false
}

// WriteError write error message to response
func WriteError(w http.ResponseWriter, statusCode int, err error) {
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(err.Error()))
	if err != nil {
		log.Error("write error", zap.Error(err))
	}
}

// WriteData write data to response with http status code 200
func WriteData(w http.ResponseWriter, data interface{}) {
	js, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		log.Error("invalid json data", zap.Any("data", data), zap.Error(err))
		WriteError(w, http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(js)
	if err != nil {
		log.Error("fail to write data", zap.Error(err))
	}
}

// Liveness can only be changed from alive to stopping, and no way back.
type Liveness int32

const (
	// LivenessCaptureAlive means the capture is alive, and ready to serve.
	LivenessCaptureAlive Liveness = 0
	// LivenessCaptureStopping means the capture is in the process of graceful shutdown.
	LivenessCaptureStopping Liveness = 1
)

// Store the given liveness. Returns true if it success.
func (l *Liveness) Store(v Liveness) bool {
	return atomic.CompareAndSwapInt32(
		(*int32)(l), int32(LivenessCaptureAlive), int32(v))
}

// Load the liveness.
func (l *Liveness) Load() Liveness {
	return Liveness(atomic.LoadInt32((*int32)(l)))
}

func (l *Liveness) String() string {
	switch *l {
	case LivenessCaptureAlive:
		return "Alive"
	case LivenessCaptureStopping:
		return "Stopping"
	default:
		return "unknown"
	}
}
