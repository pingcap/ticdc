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

package helper

import (
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// DDLDispatchRule is the dispatch rule for DDL event.
type DDLDispatchRule int

const (
	// PartitionZero means the DDL event will be dispatched to partition 0.
	// NOTICE: Only for canal and canal-json protocol.
	PartitionZero DDLDispatchRule = iota
	// PartitionAll means the DDL event will be broadcast to all the partitions.
	PartitionAll
)

func GetDDLDispatchRule(protocol config.Protocol) DDLDispatchRule {
	switch protocol {
	case config.ProtocolCanal, config.ProtocolCanalJSON:
		return PartitionZero
	default:
	}
	return PartitionAll
}

// GetExternalStorageFromURI creates a new storage.ExternalStorage from a uri.
func GetExternalStorageFromURI(
	ctx context.Context, uri string,
) (storage.ExternalStorage, error) {
	return GetExternalStorage(ctx, uri, nil, DefaultS3Retryer())
}

// GetExternalStorage creates a new storage.ExternalStorage based on the uri and options.
func GetExternalStorage(
	ctx context.Context, uri string,
	opts *storage.BackendOptions,
	retryer request.Retryer,
) (storage.ExternalStorage, error) {
	backEnd, err := storage.ParseBackend(uri, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret, err := storage.New(ctx, backEnd, &storage.ExternalStorageOptions{
		SendCredentials: false,
		S3Retryer:       retryer,
	})
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(errors.Trace(err))
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for s3")
	}

	// Check the connection and ignore the returned bool value, since we don't care if the file exists.
	_, err = ret.FileExists(ctx, "test")
	if err != nil {
		retErr := errors.ErrFailToCreateExternalStorage.Wrap(errors.Trace(err))
		return nil, retErr.GenWithStackByArgs("creating ExternalStorage for s3")
	}
	return ret, nil
}

// retryerWithLog wraps the client.DefaultRetryer, and logs when retrying.
type retryerWithLog struct {
	client.DefaultRetryer
}

// DefaultS3Retryer is the default s3 retryer, maybe this function
// should be extracted to another place.
func DefaultS3Retryer() request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    3,
			MinRetryDelay:    1 * time.Second,
			MinThrottleDelay: 2 * time.Second,
		},
	}
}

// NewS3Retryer creates a new s3 retryer.
func NewS3Retryer(maxRetries int, minRetryDelay, minThrottleDelay time.Duration) request.Retryer {
	return retryerWithLog{
		DefaultRetryer: client.DefaultRetryer{
			NumMaxRetries:    maxRetries,
			MinRetryDelay:    minRetryDelay,
			MinThrottleDelay: minThrottleDelay,
		},
	}
}

// GetTopic returns the topic name from the sink URI.
func GetTopic(sinkURI *url.URL) (string, error) {
	topic := strings.TrimFunc(sinkURI.Path, func(r rune) bool {
		return r == '/'
	})
	if topic == "" {
		return "", errors.ErrKafkaInvalidConfig.GenWithStack("no topic is specified in sink-uri")
	}
	return topic, nil
}

// GetProtocol returns the protocol from the sink URI.
func GetProtocol(protocolStr string) (config.Protocol, error) {
	protocol, err := config.ParseSinkProtocolFromString(protocolStr)
	if err != nil {
		return protocol, errors.WrapError(errors.ErrKafkaInvalidConfig, err)
	}

	return protocol, nil
}

// GetFileExtension returns the extension for specific protocol
func GetFileExtension(protocol config.Protocol) string {
	switch protocol {
	case config.ProtocolAvro, config.ProtocolCanalJSON, config.ProtocolMaxwell,
		config.ProtocolOpen, config.ProtocolSimple:
		return ".json"
	case config.ProtocolCraft:
		return ".craft"
	case config.ProtocolCanal:
		return ".canal"
	case config.ProtocolCsv:
		return ".csv"
	default:
		return ".unknown"
	}
}

// GetScheme returns the scheme of the url.
func GetScheme(url *url.URL) string {
	return strings.ToLower(url.Scheme)
}
