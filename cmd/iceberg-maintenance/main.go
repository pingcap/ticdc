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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
)

func main() {
	var (
		region              = flag.String("region", "", "AWS region (optional; defaults to AWS_REGION)")
		database            = flag.String("database", "", "Glue database name (required)")
		sparkCatalog        = flag.String("spark-catalog", "glue", "Spark catalog name to use in SQL output")
		targetFileSizeBytes = flag.Int64("target-file-size-bytes", 536870912, "Target file size for rewrite_data_files")
		retainLast          = flag.Int("retain-last", 100, "retain_last for expire_snapshots")
		olderThan           = flag.String("older-than", "", "older_than timestamp in 'YYYY-MM-DD HH:MM:SS' (UTC) for expire_snapshots/remove_orphan_files; if empty, those calls are omitted")
	)
	flag.Parse()

	if strings.TrimSpace(*database) == "" {
		fmt.Fprintln(os.Stderr, "missing required flag: --database")
		os.Exit(2)
	}

	ctx := context.Background()
	var opts []func(*awsconfig.LoadOptions) error
	if strings.TrimSpace(*region) != "" {
		opts = append(opts, awsconfig.WithRegion(strings.TrimSpace(*region)))
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load aws config failed: %v\n", err)
		os.Exit(1)
	}
	if strings.TrimSpace(cfg.Region) == "" {
		fmt.Fprintln(os.Stderr, "aws region is empty (set --region or AWS_REGION)")
		os.Exit(2)
	}

	client := glue.NewFromConfig(cfg)

	names, err := listIcebergGlueTables(ctx, client, strings.TrimSpace(*database))
	if err != nil {
		fmt.Fprintf(os.Stderr, "list glue tables failed: %v\n", err)
		os.Exit(1)
	}
	if len(names) == 0 {
		fmt.Fprintln(os.Stderr, "no iceberg tables found")
		return
	}

	sort.Strings(names)

	fmt.Printf("-- Iceberg maintenance templates (%s.%s)\n\n", strings.TrimSpace(*sparkCatalog), strings.TrimSpace(*database))
	for _, name := range names {
		tableIdent := fmt.Sprintf("%s.%s.%s", strings.TrimSpace(*sparkCatalog), strings.TrimSpace(*database), name)
		fmt.Printf("-- %s\n", tableIdent)
		fmt.Printf("CALL %s.system.rewrite_data_files(\n", strings.TrimSpace(*sparkCatalog))
		fmt.Printf("  table => '%s',\n", tableIdent)
		fmt.Printf("  options => map('target-file-size-bytes','%d')\n", *targetFileSizeBytes)
		fmt.Printf(");\n\n")

		fmt.Printf("CALL %s.system.rewrite_delete_files(table => '%s');\n", strings.TrimSpace(*sparkCatalog), tableIdent)
		fmt.Printf("CALL %s.system.rewrite_manifests(table => '%s');\n", strings.TrimSpace(*sparkCatalog), tableIdent)

		if strings.TrimSpace(*olderThan) != "" {
			fmt.Printf("CALL %s.system.expire_snapshots(\n", strings.TrimSpace(*sparkCatalog))
			fmt.Printf("  table => '%s',\n", tableIdent)
			fmt.Printf("  older_than => TIMESTAMP '%s',\n", strings.TrimSpace(*olderThan))
			fmt.Printf("  retain_last => %d\n", *retainLast)
			fmt.Printf(");\n\n")

			fmt.Printf("CALL %s.system.remove_orphan_files(\n", strings.TrimSpace(*sparkCatalog))
			fmt.Printf("  table => '%s',\n", tableIdent)
			fmt.Printf("  older_than => TIMESTAMP '%s'\n", strings.TrimSpace(*olderThan))
			fmt.Printf(");\n\n")
		} else {
			fmt.Printf("-- NOTE: pass --older-than to include expire_snapshots/remove_orphan_files\n\n")
		}
	}
}

func listIcebergGlueTables(ctx context.Context, client *glue.Client, database string) ([]string, error) {
	var (
		names     []string
		nextToken *string
	)
	for {
		out, err := client.GetTables(ctx, &glue.GetTablesInput{
			DatabaseName: aws.String(database),
			NextToken:    nextToken,
		})
		if err != nil {
			return nil, err
		}
		for _, t := range out.TableList {
			if !isGlueIcebergTable(t) {
				continue
			}
			name := strings.TrimSpace(aws.ToString(t.Name))
			if name == "" {
				continue
			}
			names = append(names, name)
		}
		if out.NextToken == nil || strings.TrimSpace(*out.NextToken) == "" {
			break
		}
		nextToken = out.NextToken
	}
	return names, nil
}

func isGlueIcebergTable(t gluetypes.Table) bool {
	tableType := strings.TrimSpace(aws.ToString(t.TableType))
	if strings.EqualFold(tableType, "ICEBERG") {
		return true
	}
	if t.Parameters == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(t.Parameters["table_type"]), "ICEBERG")
}
