// Copyright 2022 PingCAP, Inc.
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
	"net"
	"net/url"
	"strconv"
	"strings"

<<<<<<< HEAD
	"github.com/pingcap/log"
	"go.uber.org/zap"
=======
	"github.com/pingcap/ticdc/pkg/errors"
>>>>>>> 6ed23c60f (*: keep credentials out of logs (#6716))
)

// IsValidIPv6AddressFormatInURI reports whether hostPort is a valid IPv6 address in URI.
// See: https://www.ietf.org/rfc/rfc2732.txt.
func IsValidIPv6AddressFormatInURI(hostPort string) bool {
	hostname := hostPort

	colon := strings.LastIndexByte(hostname, ':')
	if colon != -1 && validOptionalPort(hostname[colon:]) {
		hostname = hostname[:colon]
	}

	if !strings.HasPrefix(hostname, "[") || !strings.HasSuffix(hostname, "]") {
		return false
	}

	return true
}

// IsIPv6Address reports whether hostname is a IPv6 address.
// Notice: There is hostname not host(host+port).
func IsIPv6Address(hostname string) bool {
	ip := net.ParseIP(hostname)
	if ip == nil {
		return false
	}
	return strings.Contains(hostname, ":")
}

// validOptionalPort reports whether port is either an empty string
// or matches /^:\d*$/
func validOptionalPort(port string) bool {
	if port == "" {
		return true
	}
	if port[0] != ':' {
		return false
	}
	for _, b := range port[1:] {
		if b < '0' || b > '9' {
			return false
		}
	}
	return true
}

// MaskSinkURI returns a sink uri that sensitive infos has been masked.
func MaskSinkURI(uri string) (string, error) {
	uriParsed, err := url.Parse(uri)
	if err != nil {
		log.Error("failed to parse sink URI", zap.Error(err))
		return "", err
	}
	queries := uriParsed.Query()
	if queries.Has("sasl-password") {
		queries.Set("sasl-password", "xxxxx")
		uriParsed.RawQuery = queries.Encode()
	}
	return uriParsed.Redacted(), nil
}

var sensitiveQueryParameterNames = []string{
	"password",
	"passwd",
	"pwd",
	"access",
	"token",
	"secret",
	"key",
	"signature",
	"credential",
	"private",
	"client",
	"authorization",
}

<<<<<<< HEAD
=======
func isSensitiveQueryParameter(name string) bool {
	name = strings.ToLower(name)
	if name == "kafka-client" {
		return false // Selects the Kafka client implementation, not a credential.
	}
	for _, sensitive := range sensitiveQueryParameterNames {
		if strings.Contains(name, sensitive) {
			return true
		}
	}
	return false
}

// maskSensitiveDataInQueryLikeText handles fragment-like request target text
// without rewriting ordinary URI fragments.
func maskSensitiveDataInQueryLikeText(text string) (string, bool) {
	prefix := ""
	queryText := text
	if question := strings.IndexByte(text, '?'); question >= 0 {
		if equals := strings.IndexByte(text, '='); equals == -1 || question < equals {
			prefix = text[:question+1]
			queryText = text[question+1:]
		}
	}
	parts := strings.Split(queryText, "&")
	masked := false
	for i, part := range parts {
		name, _, hasValue := strings.Cut(part, "=")
		if !hasValue {
			continue
		}
		decodedName, err := url.QueryUnescape(name)
		if err != nil || !isSensitiveQueryParameter(decodedName) {
			continue
		}
		parts[i] = name + "=xxxxx"
		masked = true
	}
	return prefix + strings.Join(parts, "&"), masked
}

type maskedWrappedURLError struct {
	err     error
	message string
}

func (e *maskedWrappedURLError) Error() string { return e.message }

func (e *maskedWrappedURLError) Unwrap() error { return e.err }

// ContainsSensitiveDataInURI reports whether a URI includes credential fields.
// Invalid URIs are treated as sensitive because they cannot be safely inspected.
func ContainsSensitiveDataInURI(uri string) bool {
	parsed, err := url.Parse(uri)
	if err != nil {
		return true
	}
	if parsed.User != nil {
		if _, present := parsed.User.Password(); present {
			return true
		}
	}
	query, err := url.ParseQuery(parsed.RawQuery)
	if err != nil {
		return true
	}
	for name := range query {
		if isSensitiveQueryParameter(name) {
			return true
		}
	}
	if _, sensitive := maskSensitiveDataInQueryLikeText(parsed.Fragment); sensitive {
		return true
	}
	return false
}

>>>>>>> 6ed23c60f (*: keep credentials out of logs (#6716))
// MaskSensitiveDataInURI returns an uri that sensitive infos has been masked.
func MaskSensitiveDataInURI(uri string) string {
	uriParsed, err := url.Parse(uri)
	if err != nil {
		log.Error("failed to parse sink URI", zap.Error(err))
		return ""
	}
	queries := uriParsed.Query()
	for key := range queries {
		lower := strings.ToLower(key)
		for _, secretKey := range sensitiveQueryParameterNames {
			if strings.Contains(lower, secretKey) {
				queries.Set(key, "xxxxx")
			}
		}
	}
	uriParsed.RawQuery = queries.Encode()
	if fragment, masked := maskSensitiveDataInQueryLikeText(uriParsed.Fragment); masked {
		uriParsed.Fragment = fragment
		uriParsed.RawFragment = ""
	}
	return uriParsed.Redacted()
}
<<<<<<< HEAD
=======

// MaskSensitiveDataInURIForError masks sensitive data in a URI for error messages.
func MaskSensitiveDataInURIForError(uri string) string {
	maskedURI := MaskSensitiveDataInURI(uri)
	if maskedURI == "" && uri != "" {
		return "<invalid uri>"
	}
	return maskedURI
}

// MaskSensitiveDataInURLError masks the URL carried by net/url errors.
func MaskSensitiveDataInURLError(err error) error {
	if err == nil {
		return nil
	}
	var urlErr *url.Error
	if !errors.As(err, &urlErr) {
		return err
	}
	if urlErr.URL == "" {
		return err
	}
	maskedURL := MaskSensitiveDataInURIForError(urlErr.URL)
	message := strings.ReplaceAll(err.Error(), strconv.Quote(urlErr.URL), strconv.Quote(maskedURL))
	message = strings.ReplaceAll(message, urlErr.URL, maskedURL)
	return &maskedWrappedURLError{
		err:     err,
		message: message,
	}
}
>>>>>>> 6ed23c60f (*: keep credentials out of logs (#6716))
