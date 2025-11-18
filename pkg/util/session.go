// Copyright 2019 PingCAP, Inc.
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

package util

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	infoschema "github.com/pingcap/tidb/pkg/infoschema/context"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
)

// Duplicate from github.com/pingcap/tiflow/dm/pkg/utils

func init() {
	ZeroSessionCtx = NewSessionCtx(nil)
}

type session struct {
	sessionctx.Context
	vars                 *variable.SessionVars
	exprctx              exprctx.ExprContext
	values               map[fmt.Stringer]interface{}
	builtinFunctionUsage map[string]uint32
	mu                   sync.RWMutex
}

// GetSessionVars implements the sessionctx.Context interface.
func (se *session) GetSessionVars() *variable.SessionVars {
	return se.vars
}

func (se *session) GetExprCtx() exprctx.ExprContext {
	return se.exprctx
}

// SetValue implements the sessionctx.Context interface.
func (se *session) SetValue(key fmt.Stringer, value interface{}) {
	se.mu.Lock()
	se.values[key] = value
	se.mu.Unlock()
}

// Value implements the sessionctx.Context interface.
func (se *session) Value(key fmt.Stringer) interface{} {
	se.mu.RLock()
	value := se.values[key]
	se.mu.RUnlock()
	return value
}

// GetInfoSchema implements the sessionctx.Context interface.
func (se *session) GetInfoSchema() infoschema.MetaOnlyInfoSchema {
	return nil
}

// GetBuiltinFunctionUsage implements the sessionctx.Context interface.
func (se *session) GetBuiltinFunctionUsage() map[string]uint32 {
	return se.builtinFunctionUsage
}

func (se *session) BuiltinFunctionUsageInc(scalarFuncSigName string) {}

// ZeroSessionCtx is used when the session variables is not important.
var ZeroSessionCtx sessionctx.Context

// NewSessionCtx return a session context with specified session variables.
func NewSessionCtx(vars map[string]string) sessionctx.Context {
	variables := variable.NewSessionVars(nil)
	for k, v := range vars {
		_ = variables.SetSystemVar(k, v)
		if strings.EqualFold(k, "time_zone") {
			loc, _ := ParseTimeZone(v)
			variables.StmtCtx.SetTimeZone(loc)
			variables.TimeZone = loc
		}
	}
	sessionCtx := session{
		vars:                 variables,
		values:               make(map[fmt.Stringer]interface{}, 1),
		builtinFunctionUsage: make(map[string]uint32),
	}
	sessionCtx.exprctx = sessionexpr.NewExprContext(&sessionCtx)
	return &sessionCtx
}

// ParseTimeZone parse the time zone location by name or offset
//
// NOTE: we don't support the "SYSTEM" or "Local" time_zone.
func ParseTimeZone(s string) (*time.Location, error) {
	if s == "SYSTEM" || s == "Local" {
		return nil, errors.ErrConfigInvalidTimezone.FastGenByArgs("'SYSTEM' or 'Local' time_zone is not supported")
	}

	loc, err := time.LoadLocation(s)
	if err == nil {
		return loc, nil
	}

	// The value can be given as a string indicating an offset from UTC, such as '+10:00' or '-6:00'.
	// The time zone's value should in [-12:59,+14:00].
	// See: https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html#time-zone-variables
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		d, _, err := types.ParseDuration(types.DefaultStmtNoWarningContext, s[1:], 0)
		if err == nil {
			if s[0] == '-' {
				if d.Duration > 12*time.Hour+59*time.Minute {
					return nil, errors.ErrConfigInvalidTimezone.FastGenByArgs(s)
				}
			} else {
				if d.Duration > 14*time.Hour {
					return nil, errors.ErrConfigInvalidTimezone.FastGenByArgs(s)
				}
			}

			ofst := int(d.Duration / time.Second)
			if s[0] == '-' {
				ofst = -ofst
			}
			name := dbutil.FormatTimeZoneOffset(d.Duration)
			return time.FixedZone(name, ofst), nil
		}
	}

	return nil, errors.ErrConfigInvalidTimezone.FastGenByArgs(s)
}
