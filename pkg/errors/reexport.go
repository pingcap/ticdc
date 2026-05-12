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

// Type aliases expose github.com/pingcap/errors types so callers don't need to
// import it directly.
type (
	Error        = perrors.Error
	RFCErrorCode = perrors.RFCErrorCode
	ErrCode      = perrors.ErrCode
	ErrCodeText  = perrors.ErrCodeText
)

var (
	// Is is a shortcut for errors.Is.
	Is = errors.Is
	// As is a shortcut for errors.As.
	As = errors.As
	// New is a shortcut for github.com/pingcap/errors.New.
	New = perrors.New
	// Errorf is a shortcut for github.com/pingcap/errors.Errorf.
	Errorf = perrors.Errorf
	// Trace is a shortcut for github.com/pingcap/errors.Trace.
	Trace = perrors.Trace
	// Cause is a shortcut for github.com/pingcap/errors.Cause.
	Cause = perrors.Cause
	// Annotate is a shortcut for github.com/pingcap/errors.Annotate.
	Annotate = perrors.Annotate
	// Annotatef is a shortcut for github.com/pingcap/errors.Annotatef.
	Annotatef = perrors.Annotatef
	// WithMessage is a shortcut for github.com/pingcap/errors.WithMessage.
	WithMessage = perrors.WithMessage
	// WithStack is a shortcut for github.com/pingcap/errors.WithStack.
	WithStack = perrors.WithStack
	// Wrap is a shortcut for github.com/pingcap/errors.Wrap.
	Wrap = perrors.Wrap
	// Wrapf is a shortcut for github.com/pingcap/errors.Wrapf.
	Wrapf = perrors.Wrapf
	// ErrorStack is a shortcut for github.com/pingcap/errors.ErrorStack.
	ErrorStack = perrors.ErrorStack
	// ErrorEqual is a shortcut for github.com/pingcap/errors.ErrorEqual.
	ErrorEqual = perrors.ErrorEqual
	// NewNoStackError is a shortcut for github.com/pingcap/errors.NewNoStackError.
	NewNoStackError = perrors.NewNoStackError
	// NotFoundf is a shortcut for github.com/pingcap/errors.NotFoundf.
	NotFoundf = perrors.NotFoundf
	// NotValidf is a shortcut for github.com/pingcap/errors.NotValidf.
	NotValidf = perrors.NotValidf
	// Normalize is a shortcut for github.com/pingcap/errors.Normalize.
	Normalize = perrors.Normalize
	// RFCCodeText is a shortcut for github.com/pingcap/errors.RFCCodeText.
	RFCCodeText = perrors.RFCCodeText
	// Unwrap is a shortcut for github.com/pingcap/errors.Unwrap.
	Unwrap = perrors.Unwrap
	// Find is a shortcut for github.com/pingcap/errors.Find.
	Find = perrors.Find
	// HasStack is a shortcut for github.com/pingcap/errors.HasStack.
	HasStack = perrors.HasStack
	// GetErrStackMsg is a shortcut for github.com/pingcap/errors.GetErrStackMsg.
	GetErrStackMsg = perrors.GetErrStackMsg

	// RedactLogEnabled controls log redaction. See github.com/pingcap/errors.
	RedactLogEnabled = &perrors.RedactLogEnabled
	// RedactLogEnable is the redact mode "ON".
	RedactLogEnable = perrors.RedactLogEnable
	// RedactLogDisable is the redact mode "OFF".
	RedactLogDisable = perrors.RedactLogDisable
	// RedactLogMarker is the redact mode "MARKER".
	RedactLogMarker = perrors.RedactLogMarker
)
