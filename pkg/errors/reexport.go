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

package errors

import (
	"errors"

	perrors "github.com/pingcap/errors"
)

type (
	Error        = perrors.Error
	RFCErrorCode = perrors.RFCErrorCode
	ErrCode      = perrors.ErrCode
)

var (
	Is              = errors.Is
	As              = errors.As
	New             = perrors.New
	Errorf          = perrors.Errorf
	Trace           = perrors.Trace
	Cause           = perrors.Cause
	Annotate        = perrors.Annotate
	Annotatef       = perrors.Annotatef
	WithMessage     = perrors.WithMessage
	WithStack       = perrors.WithStack
	ErrorStack      = perrors.ErrorStack
	ErrorEqual      = perrors.ErrorEqual
	NewNoStackError = perrors.NewNoStackError
	NotFoundf       = perrors.NotFoundf
	NotValidf       = perrors.NotValidf
	Normalize       = perrors.Normalize
	RFCCodeText     = perrors.RFCCodeText
	Unwrap          = perrors.Unwrap
	RedactLogEnabled = &perrors.RedactLogEnabled
)
