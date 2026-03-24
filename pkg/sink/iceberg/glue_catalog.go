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

package iceberg

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/smithy-go"
	"github.com/pingcap/ticdc/pkg/common"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/metrics"
	"github.com/pingcap/ticdc/pkg/retry"
)

const glueIcebergTableType = "ICEBERG"

const (
	defaultGlueMaxTries       = uint64(10)
	defaultGlueBackoffBaseMs  = int64(50)
	defaultGlueBackoffMaxMs   = int64(2000)
	defaultGlueRetryBudgetMs  = int64(30_000)
	glueRetryBudgetTimeFactor = int64(1)
)

func (w *TableWriter) getGlueMetadataLocation(
	ctx context.Context,
	schemaName string,
	tableName string,
) (string, bool, error) {
	if w == nil || w.cfg == nil || w.cfg.Catalog != CatalogGlue {
		return "", false, nil
	}

	client, err := w.getGlueClient(ctx)
	if err != nil {
		return "", false, err
	}

	dbName := w.glueDatabaseName(schemaName)
	glueTableName := w.glueTableName(schemaName, tableName)

	getTableOut, err := client.GetTable(ctx, &glue.GetTableInput{
		DatabaseName: aws.String(dbName),
		Name:         aws.String(glueTableName),
	})
	if err != nil {
		var notFound *gluetypes.EntityNotFoundException
		if cerror.As(err, &notFound) {
			return "", false, nil
		}
		return "", false, cerror.Trace(err)
	}

	params := getTableOut.Table.Parameters
	if params == nil {
		return "", true, cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue iceberg table parameters is nil")
	}
	location, ok := params["metadata_location"]
	if !ok {
		return "", true, cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue iceberg table metadata location is missing")
	}
	location = strings.TrimSpace(location)
	if location == "" {
		return "", true, cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue iceberg table metadata location is empty")
	}
	sanitized, err := sanitizeLocationURI(location)
	if err != nil {
		return "", true, cerror.WrapError(cerror.ErrSinkURIInvalid, err)
	}
	return sanitized, true, nil
}

func (w *TableWriter) ensureGlueTable(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	schemaName string,
	tableName string,
	tableRootLocation string,
	metadataLocation string,
) error {
	if w == nil || w.cfg == nil || w.cfg.Catalog != CatalogGlue {
		return nil
	}
	if strings.TrimSpace(metadataLocation) == "" {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("metadata location is empty")
	}

	client, err := w.getGlueClient(ctx)
	if err != nil {
		return err
	}

	var attempts uint64
	dbName := w.glueDatabaseName(schemaName)
	glueTableName := w.glueTableName(schemaName, tableName)

	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	schema := schemaName
	table := tableName

	budget := defaultGlueRetryBudgetMs * glueRetryBudgetTimeFactor
	err = retry.Do(
		ctx,
		func() error {
			attempts++

			_, err = client.GetDatabase(ctx, &glue.GetDatabaseInput{
				Name: aws.String(dbName),
			})
			if err != nil {
				var notFound *gluetypes.EntityNotFoundException
				if !cerror.As(err, &notFound) {
					return cerror.Trace(err)
				}
				_, err = client.CreateDatabase(ctx, &glue.CreateDatabaseInput{
					DatabaseInput: &gluetypes.DatabaseInput{
						Name: aws.String(dbName),
					},
				})
				if err != nil {
					var exists *gluetypes.AlreadyExistsException
					if cerror.As(err, &exists) {
						err = nil
					}
				}
				if err != nil {
					return cerror.Trace(err)
				}
			}

			getTableOut, err := client.GetTable(ctx, &glue.GetTableInput{
				DatabaseName: aws.String(dbName),
				Name:         aws.String(glueTableName),
			})
			if err != nil {
				var notFound *gluetypes.EntityNotFoundException
				if !cerror.As(err, &notFound) {
					return cerror.Trace(err)
				}
				_, err = client.CreateTable(ctx, &glue.CreateTableInput{
					DatabaseName: aws.String(dbName),
					TableInput: &gluetypes.TableInput{
						Name:      aws.String(glueTableName),
						TableType: aws.String(glueIcebergTableType),
						Parameters: map[string]string{
							"table_type":        glueIcebergTableType,
							"metadata_location": metadataLocation,
						},
						StorageDescriptor: &gluetypes.StorageDescriptor{
							Location: aws.String(tableRootLocation),
						},
					},
				})
				if err != nil {
					var exists *gluetypes.AlreadyExistsException
					if cerror.As(err, &exists) {
						getTableOut, err = client.GetTable(ctx, &glue.GetTableInput{
							DatabaseName: aws.String(dbName),
							Name:         aws.String(glueTableName),
						})
						if err != nil {
							return cerror.Trace(err)
						}
					} else {
						return cerror.Trace(err)
					}
				} else {
					return nil
				}
			}

			params := getTableOut.Table.Parameters
			if params == nil {
				params = make(map[string]string)
			}
			oldMetadataLocation := strings.TrimSpace(params["metadata_location"])
			if oldMetadataLocation == strings.TrimSpace(metadataLocation) && oldMetadataLocation != "" {
				return nil
			}
			if oldMetadataLocation != "" {
				params["previous_metadata_location"] = oldMetadataLocation
			}
			params["table_type"] = glueIcebergTableType
			params["metadata_location"] = metadataLocation

			tableType := getTableOut.Table.TableType
			if tableType == nil || strings.TrimSpace(*tableType) == "" {
				tableType = aws.String(glueIcebergTableType)
			}

			storageDescriptor := getTableOut.Table.StorageDescriptor
			if storageDescriptor == nil {
				storageDescriptor = &gluetypes.StorageDescriptor{}
			}
			if storageDescriptor.Location == nil || strings.TrimSpace(*storageDescriptor.Location) == "" {
				storageDescriptor.Location = aws.String(tableRootLocation)
			}

			_, err = client.UpdateTable(ctx, &glue.UpdateTableInput{
				DatabaseName: aws.String(dbName),
				TableInput: &gluetypes.TableInput{
					Name:              getTableOut.Table.Name,
					Description:       getTableOut.Table.Description,
					Owner:             getTableOut.Table.Owner,
					Retention:         getTableOut.Table.Retention,
					StorageDescriptor: storageDescriptor,
					PartitionKeys:     getTableOut.Table.PartitionKeys,
					TableType:         tableType,
					Parameters:        params,
					ViewOriginalText:  getTableOut.Table.ViewOriginalText,
					ViewExpandedText:  getTableOut.Table.ViewExpandedText,
				},
			})
			if err != nil {
				if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
					metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					if isGlueConflictError(err) {
						metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					}
				}
				return cerror.Trace(err)
			}
			return nil
		},
		retry.WithMaxTries(defaultGlueMaxTries),
		retry.WithBackoffBaseDelay(defaultGlueBackoffBaseMs),
		retry.WithBackoffMaxDelay(defaultGlueBackoffMaxMs),
		retry.WithTotalRetryDuration(time.Duration(budget)*time.Millisecond),
		retry.WithIsRetryableErr(isGlueRetryableError),
	)
	return err
}

// RenameGlueTable renames a Glue table within the configured database.
func (w *TableWriter) RenameGlueTable(
	ctx context.Context,
	changefeedID common.ChangeFeedID,
	oldSchemaName string,
	oldTableName string,
	newSchemaName string,
	newTableName string,
) error {
	if w == nil || w.cfg == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if w.cfg.Catalog != CatalogGlue {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue catalog is not enabled")
	}

	oldDBName := w.glueDatabaseName(oldSchemaName)
	oldGlueTableName := w.glueTableName(oldSchemaName, oldTableName)
	newDBName := w.glueDatabaseName(newSchemaName)
	newGlueTableName := w.glueTableName(newSchemaName, newTableName)

	if strings.TrimSpace(oldDBName) == strings.TrimSpace(newDBName) &&
		strings.TrimSpace(oldGlueTableName) == strings.TrimSpace(newGlueTableName) {
		return nil
	}

	client, err := w.getGlueClient(ctx)
	if err != nil {
		return err
	}

	var attempts uint64
	keyspace := changefeedID.Keyspace()
	changefeed := changefeedID.Name()
	schema := newSchemaName
	table := newTableName

	err = retry.Do(
		ctx,
		func() error {
			attempts++
			getTableOut, err := client.GetTable(ctx, &glue.GetTableInput{
				DatabaseName: aws.String(oldDBName),
				Name:         aws.String(oldGlueTableName),
			})
			if err != nil {
				var notFound *gluetypes.EntityNotFoundException
				if cerror.As(err, &notFound) {
					return nil
				}
				if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
					metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					if isGlueConflictError(err) {
						metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					}
				}
				return cerror.Trace(err)
			}
			if getTableOut.Table == nil {
				return cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue table is nil")
			}

			params := getTableOut.Table.Parameters
			if params == nil {
				params = make(map[string]string)
			}
			metadataLocation := strings.TrimSpace(params["metadata_location"])
			if metadataLocation == "" {
				return cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue iceberg table metadata location is empty")
			}

			_, err = client.GetDatabase(ctx, &glue.GetDatabaseInput{
				Name: aws.String(newDBName),
			})
			if err != nil {
				var notFound *gluetypes.EntityNotFoundException
				if !cerror.As(err, &notFound) {
					if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
						metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
						if isGlueConflictError(err) {
							metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
						}
					}
					return cerror.Trace(err)
				}
				_, err = client.CreateDatabase(ctx, &glue.CreateDatabaseInput{
					DatabaseInput: &gluetypes.DatabaseInput{
						Name: aws.String(newDBName),
					},
				})
				if err != nil {
					var exists *gluetypes.AlreadyExistsException
					if !cerror.As(err, &exists) {
						if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
							metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
							if isGlueConflictError(err) {
								metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
							}
						}
						return cerror.Trace(err)
					}
				}
			}

			storageDescriptor := getTableOut.Table.StorageDescriptor
			if storageDescriptor == nil {
				storageDescriptor = &gluetypes.StorageDescriptor{}
			}

			_, err = client.CreateTable(ctx, &glue.CreateTableInput{
				DatabaseName: aws.String(newDBName),
				TableInput: &gluetypes.TableInput{
					Name:      aws.String(newGlueTableName),
					TableType: aws.String(glueIcebergTableType),
					Parameters: map[string]string{
						"table_type":        glueIcebergTableType,
						"metadata_location": metadataLocation,
					},
					StorageDescriptor: storageDescriptor,
				},
			})
			if err != nil {
				var exists *gluetypes.AlreadyExistsException
				if !cerror.As(err, &exists) {
					if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
						metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
						if isGlueConflictError(err) {
							metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
						}
					}
					return cerror.Trace(err)
				}
			}

			_, err = client.DeleteTable(ctx, &glue.DeleteTableInput{
				DatabaseName: aws.String(oldDBName),
				Name:         aws.String(oldGlueTableName),
			})
			if err != nil {
				var notFound *gluetypes.EntityNotFoundException
				if cerror.As(err, &notFound) {
					return nil
				}
				if attempts < defaultGlueMaxTries && isGlueRetryableError(err) {
					metrics.IcebergCommitRetriesCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					if isGlueConflictError(err) {
						metrics.IcebergCommitConflictsCounter.WithLabelValues(keyspace, changefeed, schema, table).Inc()
					}
				}
				return cerror.Trace(err)
			}
			return nil
		},
		retry.WithMaxTries(defaultGlueMaxTries),
		retry.WithBackoffBaseDelay(defaultGlueBackoffBaseMs),
		retry.WithBackoffMaxDelay(defaultGlueBackoffMaxMs),
		retry.WithTotalRetryDuration(time.Duration(defaultGlueRetryBudgetMs)*time.Millisecond),
		retry.WithIsRetryableErr(isGlueRetryableError),
	)
	return err
}

// VerifyCatalog checks that the catalog configuration is usable.
func (w *TableWriter) VerifyCatalog(ctx context.Context) error {
	if w == nil || w.cfg == nil {
		return cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if w.cfg.Catalog != CatalogGlue {
		return nil
	}
	_, err := w.getGlueClient(ctx)
	return err
}

func (w *TableWriter) getGlueClient(ctx context.Context) (*glue.Client, error) {
	if w == nil || w.cfg == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("iceberg config is nil")
	}
	if w.cfg.Catalog != CatalogGlue {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue catalog is not enabled")
	}

	w.glueOnce.Do(func() {
		var (
			awsCfg aws.Config
			err    error
		)
		if strings.TrimSpace(w.cfg.AWSRegion) != "" {
			awsCfg, err = awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(w.cfg.AWSRegion))
		} else {
			awsCfg, err = awsconfig.LoadDefaultConfig(ctx)
		}
		if err != nil {
			w.glueErr = cerror.Trace(err)
			return
		}
		if strings.TrimSpace(awsCfg.Region) == "" {
			w.glueErr = cerror.ErrSinkURIInvalid.GenWithStackByArgs(
				"aws region is empty for glue catalog (set iceberg-config.region or AWS_REGION)",
			)
			return
		}
		w.glueClient = glue.NewFromConfig(awsCfg)
	})

	if w.glueErr != nil {
		return nil, w.glueErr
	}
	client, ok := w.glueClient.(*glue.Client)
	if !ok || client == nil {
		return nil, cerror.ErrSinkURIInvalid.GenWithStackByArgs("glue client is nil")
	}
	return client, nil
}

func (w *TableWriter) glueDatabaseName(schemaName string) string {
	if strings.TrimSpace(w.cfg.GlueDatabase) != "" {
		return sanitizeGlueIdentifier(w.cfg.GlueDatabase)
	}
	return sanitizeGlueIdentifier(w.cfg.Namespace + "_" + schemaName)
}

func (w *TableWriter) glueTableName(schemaName, tableName string) string {
	if strings.TrimSpace(w.cfg.GlueDatabase) != "" {
		return sanitizeGlueIdentifier(schemaName + "__" + tableName)
	}
	return sanitizeGlueIdentifier(tableName)
}

func sanitizeGlueIdentifier(in string) string {
	s := strings.ToLower(strings.TrimSpace(in))
	if s == "" {
		return "ticdc"
	}
	var b strings.Builder
	b.Grow(len(s))

	lastUnderscore := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "ticdc"
	}
	if out[0] >= '0' && out[0] <= '9' {
		return "t_" + out
	}
	return out
}

func isGlueRetryableError(err error) bool {
	if err == nil {
		return false
	}

	var concurrent *gluetypes.ConcurrentModificationException
	if cerror.As(err, &concurrent) {
		return true
	}
	var internal *gluetypes.InternalServiceException
	if cerror.As(err, &internal) {
		return true
	}
	var timeout *gluetypes.OperationTimeoutException
	if cerror.As(err, &timeout) {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch strings.TrimSpace(apiErr.ErrorCode()) {
		case "ConcurrentModificationException", "ThrottlingException", "TooManyRequestsException", "RequestLimitExceeded",
			"InternalServiceException", "OperationTimeoutException":
			return true
		default:
		}
	}
	return false
}

func isGlueConflictError(err error) bool {
	if err == nil {
		return false
	}
	var concurrent *gluetypes.ConcurrentModificationException
	if cerror.As(err, &concurrent) {
		return true
	}
	var apiErr smithy.APIError
	return errors.As(err, &apiErr) && strings.TrimSpace(apiErr.ErrorCode()) == "ConcurrentModificationException"
}
