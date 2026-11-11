// Copyright 2026 PingCAP, Inc.
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

package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/aws/smithy-go"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestAWSMSKIAMTokenProvider(t *testing.T) {
	setAWSMSKIAMTestEnvironment(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	provider, err := newAWSMSKIAMTokenProvider(t.Context(), config.AWSMSKIAMConfig{
		Region: "us-east-1",
	})
	require.NoError(t, err)
	token, err := provider.Token()
	require.NoError(t, err)
	require.NotEmpty(t, token.Token)

	mechanism, err := buildSASLMechanism(t.Context(), &saslConfig{
		mechanism:     oauthMechanism,
		oauthProvider: SASLOAuthProviderAWSMSKIAM,
		awsMSKIAM:     config.AWSMSKIAMConfig{Region: "us-east-1"},
	})
	require.NoError(t, err)
	require.Equal(t, SASLTypeOAuth, mechanism.Name())
	_, initialResponse, err := mechanism.Authenticate(t.Context(), "")
	require.NoError(t, err)
	require.Contains(t, string(initialResponse), "auth=Bearer ")
}

func TestCompleteSaramaSASLConfigUsesAWSMSKIAMProvider(t *testing.T) {
	setAWSMSKIAMTestEnvironment(t)
	saramaConfig := sarama.NewConfig()
	options := NewOptions()
	options.sasl.mechanism = oauthMechanism
	options.sasl.oauthProvider = SASLOAuthProviderAWSMSKIAM
	options.sasl.awsMSKIAM = config.AWSMSKIAMConfig{
		Region:          "ap-northeast-1",
		RoleARN:         "arn:aws:iam::123456789012:role/TiCDCMSKProducer",
		RoleSessionName: "ticdc",
		ExternalID:      "external-id",
	}

	err := completeSaramaSASLConfig(context.Background(), saramaConfig, options)
	require.NoError(t, err)
	provider, ok := saramaConfig.Net.SASL.TokenProvider.(*awsMSKIAMTokenProvider)
	require.True(t, ok)
	require.Equal(t, options.sasl.awsMSKIAM, provider.config)
}

func TestAWSMSKIAMCredentialErrors(t *testing.T) {
	for _, client := range []string{KafkaClientSarama, KafkaClientFranz} {
		t.Run(client, func(t *testing.T) {
			t.Run("credential process output", func(t *testing.T) {
				configPath := setAWSMSKIAMTestEnvironment(t)
				outputPath := filepath.Join(t.TempDir(), "credentials.json")
				require.NoError(t, os.WriteFile(outputPath, []byte(
					`{"AccessKeyId":"access-sentinel","SecretAccessKey":"secret-sentinel","SessionToken":"session-sentinel",`), 0o600))
				require.NoError(t, os.WriteFile(configPath, []byte(
					fmt.Sprintf("[default]\ncredential_process = cat %q\n", outputPath)), 0o600))
				auth, err := newAWSMSKIAMTestAuth(t, client, config.AWSMSKIAMConfig{Region: "us-east-1"})
				require.NoError(t, err)
				_, err = auth()
				requireSafeAWSMSKIAMError(t, err)
				require.ErrorContains(t, err, "generate AWS MSK IAM token")
			})

			t.Run("configuration error", func(t *testing.T) {
				setAWSMSKIAMTestEnvironment(t)
				t.Setenv("AWS_PROFILE", "profile-sentinel")
				_, err := newAWSMSKIAMTestAuth(t, client, config.AWSMSKIAMConfig{Region: "us-east-1"})
				requireSafeAWSMSKIAMError(t, err)
				require.ErrorContains(t, err, "load AWS configuration")
			})

			t.Run("credential service connection failure", func(t *testing.T) {
				setAWSMSKIAMTestEnvironment(t)
				server := httptest.NewServer(http.NotFoundHandler())
				server.Close()
				t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", server.URL+"/url-sentinel")
				auth, err := newAWSMSKIAMTestAuth(t, client, config.AWSMSKIAMConfig{Region: "us-east-1"})
				require.NoError(t, err)
				_, err = auth()
				requireSafeAWSMSKIAMError(t, err)
			})
		})
	}
}

func TestAWSMSKIAMAssumeRoleRetry(t *testing.T) {
	for _, client := range []string{KafkaClientSarama, KafkaClientFranz} {
		for _, test := range []struct {
			status int
			code   string
		}{
			{http.StatusServiceUnavailable, "ServiceUnavailable"},
			{http.StatusBadRequest, "Throttling"},
			{http.StatusTooManyRequests, "ThrottlingException"},
			{http.StatusBadRequest, "code-sentinel"},
		} {
			t.Run(fmt.Sprintf("%s/%d/%s", client, test.status, test.code), func(t *testing.T) {
				setAWSMSKIAMTestEnvironment(t)
				t.Setenv("AWS_ACCESS_KEY_ID", "test-access-key")
				t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if err := r.ParseForm(); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if r.PostForm.Get("RoleSessionName") != "MSKSASLDefaultSession" {
						t.Error("unexpected default role session name")
					}
					w.Header().Set("Content-Type", "text/xml")
					if requests.Add(1) == 1 {
						w.WriteHeader(test.status)
						_, err := fmt.Fprintf(w, `<ErrorResponse><Error><Type>Receiver</Type><Code>%s</Code><Message>access-sentinel secret-sentinel session-sentinel</Message></Error><RequestId>request-sentinel</RequestId></ErrorResponse>`, test.code)
						if err != nil {
							t.Error(err)
						}
						return
					}
					writeAWSMSKIAMRoleCredentials(t, w, time.Now().Add(time.Hour))
				}))
				t.Cleanup(server.Close)
				t.Setenv("AWS_ENDPOINT_URL_STS", server.URL)
				auth, err := newAWSMSKIAMTestAuth(t, client, config.AWSMSKIAMConfig{
					Region: "us-east-1", RoleARN: "arn:aws:iam::123456789012:role/ticdc",
				})
				require.NoError(t, err)
				_, err = auth()
				requireSafeAWSMSKIAMError(t, err)
				require.ErrorContains(t, err, fmt.Sprintf("HTTP status %d", test.status))
				if test.code != "code-sentinel" {
					require.ErrorContains(t, err, test.code)
				}
				token, err := auth()
				require.NoError(t, err)
				require.NotEmpty(t, token)
				require.Equal(t, int32(2), requests.Load())
			})
		}
	}
}

func TestAWSMSKIAMContextErrors(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			err := wrapAWSMSKIAMError("generate AWS MSK IAM token", &smithy.OperationError{
				ServiceID: "service-sentinel", OperationName: "operation-sentinel", Err: cause,
			})
			requireSafeAWSMSKIAMError(t, err)
			require.ErrorIs(t, err, cause)
		})
	}
}

func TestAWSMSKIAMCredentialCache(t *testing.T) {
	for _, client := range []string{KafkaClientSarama, KafkaClientFranz} {
		for _, assumeRole := range []bool{false, true} {
			for _, expireFirst := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/role=%t/refresh=%t", client, assumeRole, expireFirst), func(t *testing.T) {
					setAWSMSKIAMTestEnvironment(t)
					var credentialRequests, roleRequests atomic.Int32
					roleInputs := make(chan url.Values, 32)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						expiration := time.Now().Add(time.Hour)
						if r.URL.Path == "/credentials" {
							if credentialRequests.Add(1) == 1 && !assumeRole && expireFirst {
								expiration = time.Now().Add(-time.Hour)
							}
							w.Header().Set("Content-Type", "application/json")
							err := json.NewEncoder(w).Encode(map[string]any{
								"AccessKeyId": "base-access-key", "SecretAccessKey": "test-secret-key",
								"Token": "base-session-token", "Expiration": expiration.Format(time.RFC3339),
							})
							if err != nil {
								t.Error(err)
							}
							return
						}
						if err := r.ParseForm(); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						roleInputs <- r.PostForm
						if roleRequests.Add(1) == 1 && expireFirst {
							expiration = time.Now().Add(-time.Hour)
						}
						writeAWSMSKIAMRoleCredentials(t, w, expiration)
					}))
					t.Cleanup(server.Close)
					t.Setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", server.URL+"/credentials")
					t.Setenv("AWS_ENDPOINT_URL_STS", server.URL+"/sts")
					cfg := config.AWSMSKIAMConfig{Region: "us-east-1"}
					if assumeRole {
						cfg.RoleARN = "arn:aws:iam::123456789012:role/ticdc"
						cfg.RoleSessionName = "ticdc"
						cfg.ExternalID = "external-id"
					}
					auth, err := newAWSMSKIAMTestAuth(t, client, cfg)
					require.NoError(t, err)
					// Start with concurrent cold-cache calls to exercise shared retrieval.
					if expireFirst {
						_, err := auth()
						require.NoError(t, err)
					}
					results := make(chan error, 24)
					var calls sync.WaitGroup
					for range cap(results) {
						calls.Go(func() {
							_, err := auth()
							results <- err
						})
					}
					calls.Wait()
					close(results)
					for err := range results {
						require.NoError(t, err)
					}
					var token string
					for range 3 {
						token, err = auth()
						require.NoError(t, err)
					}
					expectedRequests := int32(1)
					if expireFirst {
						expectedRequests++
					}
					if assumeRole {
						require.Equal(t, int32(1), credentialRequests.Load())
						require.Equal(t, expectedRequests, roleRequests.Load())
						for range expectedRequests {
							input := <-roleInputs
							require.Equal(t, cfg.RoleARN, input.Get("RoleArn"))
							require.Equal(t, cfg.RoleSessionName, input.Get("RoleSessionName"))
							require.Equal(t, cfg.ExternalID, input.Get("ExternalId"))
							require.Equal(t, "3600", input.Get("DurationSeconds"))
						}
					} else {
						require.Equal(t, expectedRequests, credentialRequests.Load())
					}
					decoded, err := base64.RawURLEncoding.DecodeString(token)
					require.NoError(t, err)
					signedURL, err := url.Parse(string(decoded))
					require.NoError(t, err)
					key, sessionToken := "base-access-key", "base-session-token"
					if assumeRole {
						key, sessionToken = "role-access-key", "role-session-token"
					}
					require.Contains(t, signedURL.Query().Get("X-Amz-Credential"), key+"/")
					require.Equal(t, sessionToken, signedURL.Query().Get("X-Amz-Security-Token"))
				})
			}
		}
	}
}

func setAWSMSKIAMTestEnvironment(t *testing.T) string {
	t.Helper()
	for _, name := range []string{
		"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
		"AWS_ROLE_ARN", "AWS_ROLE_SESSION_NAME", "AWS_WEB_IDENTITY_TOKEN_FILE",
		"AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
		"AWS_CONTAINER_AUTHORIZATION_TOKEN", "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
		"AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_STS",
	} {
		t.Setenv(name, "")
	}
	configPath := filepath.Join(t.TempDir(), "aws-config")
	credentialsPath := filepath.Join(t.TempDir(), "aws-credentials")
	require.NoError(t, os.WriteFile(configPath, nil, 0o600))
	require.NoError(t, os.WriteFile(credentialsPath, nil, 0o600))
	t.Setenv("AWS_CONFIG_FILE", configPath)
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", credentialsPath)
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")
	t.Setenv("AWS_MAX_ATTEMPTS", "1")
	t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
	return configPath
}

func newAWSMSKIAMTestAuth(t *testing.T, client string, cfg config.AWSMSKIAMConfig) (func() (string, error), error) {
	t.Helper()
	opts := NewOptions()
	opts.sasl.mechanism = oauthMechanism
	opts.sasl.oauthProvider = SASLOAuthProviderAWSMSKIAM
	opts.sasl.awsMSKIAM = cfg
	if client == KafkaClientSarama {
		saramaConfig := sarama.NewConfig()
		if err := completeSaramaSASLConfig(t.Context(), saramaConfig, opts); err != nil {
			return nil, err
		}
		return func() (string, error) {
			token, err := saramaConfig.Net.SASL.TokenProvider.Token()
			if err != nil {
				return "", err
			}
			return token.Token, nil
		}, nil
	}
	mechanism, err := buildSASLMechanism(t.Context(), opts.sasl)
	if err != nil {
		return nil, err
	}
	return func() (string, error) {
		_, response, err := mechanism.Authenticate(t.Context(), "")
		return strings.TrimSuffix(strings.TrimPrefix(string(response), "n,,\x01auth=Bearer "), "\x01\x01"), err
	}, nil
}

func requireSafeAWSMSKIAMError(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, errors.ErrNewKafkaSink)
	require.False(t, errors.ShouldFailChangefeed(err))
	// Check every retained cause, including stack formatting used by logging.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		require.NotContains(t, fmt.Sprintf("%+v", cause), "sentinel")
	}
}

func writeAWSMSKIAMRoleCredentials(t *testing.T, w http.ResponseWriter, expiration time.Time) {
	t.Helper()
	w.Header().Set("Content-Type", "text/xml")
	_, err := fmt.Fprintf(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><AssumeRoleResult><Credentials><AccessKeyId>role-access-key</AccessKeyId><SecretAccessKey>test-secret-key</SecretAccessKey><SessionToken>role-session-token</SessionToken><Expiration>%s</Expiration></Credentials></AssumeRoleResult><ResponseMetadata><RequestId>test-request</RequestId></ResponseMetadata></AssumeRoleResponse>`, expiration.Format(time.RFC3339))
	if err != nil {
		t.Error(err)
	}
}
