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
	"fmt"
	"net/url"
	"testing"

	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsValidIPv6AddressFormatInURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"valid ipv6 address", "[::1]", true},
		{"valid ipv6 address1 with port", "[::1]:8080", true},
		{"valid ipv6 address2 with port", "[1080:0:0:0:8:800:200C:417A]:8080", true},
		{"valid ipv6 address3 with port", "[::FFFF:129.144.52.38]:8080", true},
		{"invalid ipv6 address", "::1", false},
		{"invalid ipv6 address with port", "::1:8000", false},
	}
	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, IsValidIPv6AddressFormatInURI(test.host))
		})
	}
}

func TestIsIPv6Address(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		host string
		want bool
	}{
		{"valid ipv6 address1", "::1", true},
		{"valid ipv6 address2", "1080:0:0:0:8:800:200C:417A", true},
		{"ipv4 address", "127.0.0.1", false},
		{"empty address", "", false},
		{"not ip address", "emmmmmmmm", false},
	}

	for _, tt := range tests {
		test := tt
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, IsIPv6Address(test.host))
		})
	}
}

func TestMaskSinkURI(t *testing.T) {
	tests := []struct {
		uri    string
		masked string
	}{
		{
			"mysql://root:123456@127.0.0.1:3306/?time-zone=Asia/Shanghai",
			"mysql://root:xxxxx@127.0.0.1:3306/?time-zone=Asia/Shanghai",
		},
		{
			"kafka://127.0.0.1:9093/cdc?sasl-mechanism=SCRAM-SHA-256&sasl-user=ticdc&sasl-password=verysecure",
			"kafka://127.0.0.1:9093/cdc?sasl-mechanism=SCRAM-SHA-256&sasl-password=xxxxx&sasl-user=ticdc",
		},
	}

	for _, tt := range tests {
		maskedURI, err := MaskSinkURI(tt.uri)
		require.NoError(t, err)
		require.Equal(t, tt.masked, maskedURI)
	}
}

func TestMaskSensitiveDataInURI(t *testing.T) {
	tests := []struct {
		uri    string
		masked string
	}{
		{
			"kafka://127.0.0.1:9092/topic?kafka-client=franz&sasl-password=secret",
			"kafka://127.0.0.1:9092/topic?kafka-client=franz&sasl-password=xxxxx",
		},
		{
			"https://api.example.com/?keyspace=default&token=secret",
			"https://api.example.com/?keyspace=default&token=xxxxx",
		},
		{
			"https://api.example.com/?key=secret-value",
			"https://api.example.com/?key=xxxxx",
		},
		{
			"kafka://127.0.0.1:9092/topic?kafka-client-id=unit-test&output-row-key=true",
			"kafka://127.0.0.1:9092/topic?kafka-client-id=unit-test&output-row-key=true",
		},
		{
			"kafka://127.0.0.1:9092/topic?key=/tmp/client.key&sasl-gssapi-keytab-path=/tmp/client.keytab",
			"kafka://127.0.0.1:9092/topic?key=%2Ftmp%2Fclient.key&sasl-gssapi-keytab-path=%2Ftmp%2Fclient.keytab",
		},
		{
			"kafka+ssl://127.0.0.1:9092/topic?key=/tmp/client.key",
			"kafka+ssl://127.0.0.1:9092/topic?key=%2Ftmp%2Fclient.key",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?time-zone=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?time-zone=c",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?access_key=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?access_key=xxxxx",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?secret_access_key=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?secret_access_key=xxxxx",
		},
		{
			"mysql://root:123456@127.0.0.1:3306/?client_secret=c",
			"mysql://root:xxxxx@127.0.0.1:3306/?client_secret=xxxxx",
		},
		{
			"https://api.example.com/?api-key=secret&client-id=diagnostic-client",
			"https://api.example.com/?api-key=xxxxx&client-id=diagnostic-client",
		},
		{
			"https://kms.example.com/?authorization=Bearer%20secret-value&authorization-endpoint=https%3A%2F%2Fauth.example.com",
			"https://kms.example.com/?authorization=xxxxx&authorization-endpoint=https%3A%2F%2Fauth.example.com",
		},
		{
			"/api?region=west#token=secret-value",
			"/api?region=west#token=xxxxx",
		},
		{
			"http://h/p#/callback?token=secret-value",
			"http://h/p#/callback?token=xxxxx",
		},
		{
			"http://h/p#/callback?authorization=Bearer%20secret-value",
			"http://h/p#/callback?authorization=xxxxx",
		},
		{
			"http://h/p#/private?region=west",
			"http://h/p#/private?region=west",
		},
		{
			"/api?region=west#diagnostics",
			"/api?region=west#diagnostics",
		},
		{
			"/api?region=west#private-section",
			"/api?region=west#private-section",
		},
		{
			"",
			"",
		},
		{
			"abc",
			"abc",
		},
	}
	for _, q := range sensitiveQueryParameterNames {
		if !isSensitiveQueryParameter(q) {
			continue
		}
		tests = append(tests, struct {
			uri    string
			masked string
		}{
			"https://api.example.com/?" + q + "=verysecure",
			"https://api.example.com/?" + q + "=xxxxx",
		})
	}

	for _, tt := range tests {
		maskedURI := MaskSensitiveDataInURI(tt.uri)
		require.Equal(t, tt.masked, maskedURI)
	}
}

func TestContainsSensitiveDataInURI(t *testing.T) {
	for _, tc := range []struct {
		uri  string
		want bool
	}{
		{uri: "https://user@registry.example.com", want: false},
		{uri: "s3://bucket/prefix?region=us-west-1", want: false},
		{uri: "https://api.example.com?keyspace=default", want: false},
		{uri: "kafka://broker/topic?kafka-client-id=client-1&output-row-key=true", want: false},
		{uri: "kafka://broker/topic?key=%2Ftmp%2Fclient.key", want: false},
		{uri: "kafka+ssl://broker/topic?key=%2Ftmp%2Fclient.key", want: false},
		{uri: "https://api.example.com?authorization-endpoint=https%3A%2F%2Fauth.example.com", want: false},
		{uri: "/api?region=west#diagnostics", want: false},
		{uri: "https://user:password@registry.example.com", want: true},
		{uri: "s3://bucket/prefix?Secret-Access-Key=password", want: true},
		{uri: "https://api.example.com?authorization=Bearer%20secret-value", want: true},
		{uri: "https://api.example.com?key=secret-value", want: true},
		{uri: "/api?region=west#token=secret-value", want: true},
		{uri: "https://user:password@registry.example.com/%zz", want: true},
		{uri: "s3://bucket?secret-access-key=secret%zz", want: true},
	} {
		require.Equal(t, tc.want, ContainsSensitiveDataInURI(tc.uri), tc.uri)
	}
}

func TestMaskSensitiveDataInURIForError(t *testing.T) {
	require.Equal(t, "", MaskSensitiveDataInURIForError(""))
	require.Equal(t, "abc", MaskSensitiveDataInURIForError("abc"))
	require.Equal(t,
		"mysql://root:xxxxx@127.0.0.1:3306/?sasl-password=xxxxx",
		MaskSensitiveDataInURIForError("mysql://root:verysecure@127.0.0.1:3306/?sasl-password=rawsecret"))
	require.Equal(t, "<invalid uri>", MaskSensitiveDataInURIForError("mysql://root:verysecure@127.0.0.1/%zz"))
}

func TestMaskSensitiveDataInURLError(t *testing.T) {
	emptyURLErr := fmt.Errorf("request failed: %w", &url.Error{Op: "GET", Err: errors.New("missing URL")})
	require.Same(t, emptyURLErr, MaskSensitiveDataInURLError(emptyURLErr))

	rawURL := "mysql://root:verysecure@127.0.0.1/%zz"
	_, err := url.Parse(rawURL)
	require.Error(t, err)

	maskedErr := MaskSensitiveDataInURLError(err)
	require.NotContains(t, maskedErr.Error(), "verysecure")
	require.Contains(t, maskedErr.Error(), `parse "<invalid uri>"`)
	require.Contains(t, maskedErr.Error(), "invalid URL escape")

	for _, tc := range []struct {
		name       string
		rawURL     string
		credential string
	}{
		{
			name:       "quote in userinfo",
			rawURL:     `https://user:before"credential-after@kms.example.com`,
			credential: "credential-after",
		},
		{
			name:       "backslash in userinfo",
			rawURL:     `https://user:before\credential-after@kms.example.com`,
			credential: "credential-after",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, parseErr := url.Parse(tc.rawURL)
			require.Error(t, parseErr)

			maskedParseErr := MaskSensitiveDataInURLError(parseErr)
			require.NotContains(t, maskedParseErr.Error(), tc.credential)
			require.Contains(t, maskedParseErr.Error(), `parse "<invalid uri>"`)
		})
	}

	quotedQueryURL := `https://kms.example.com/path?token=before"query-credential`
	quotedQueryErr := &url.Error{Op: "GET", URL: quotedQueryURL, Err: errors.New("request failed")}
	maskedQuotedQueryErr := MaskSensitiveDataInURLError(quotedQueryErr)
	require.NotContains(t, maskedQuotedQueryErr.Error(), "query-credential")
	require.Contains(t, maskedQuotedQueryErr.Error(), "token=xxxxx")

	cause := errors.New("connection refused")
	wrappedErr := fmt.Errorf("kms request failed: %w", &url.Error{
		Op:  "POST",
		URL: "https://user:password@kms.example.com/path?region=west&token=secret-token",
		Err: cause,
	})
	maskedErr = MaskSensitiveDataInURLError(wrappedErr)
	require.Contains(t, maskedErr.Error(), "kms request failed")
	require.Contains(t, maskedErr.Error(), "connection refused")
	require.Contains(t, maskedErr.Error(), "region=west")
	require.NotContains(t, maskedErr.Error(), "password")
	require.NotContains(t, maskedErr.Error(), "secret-token")
	require.ErrorIs(t, maskedErr, cause)
}
