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

package common

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type moduleInfo struct {
	Path    string `json:"Path"`
	Version string `json:"Version"`
	Dir     string `json:"Dir"`
	Origin  struct {
		Hash string `json:"Hash"`
	} `json:"Origin"`
}

// structGuardCase describes one upstream struct contract check.
type structGuardCase struct {
	name            string
	modulePath      string
	pkgRelativePath string
	typeName        string
	expectedFields  []string
}

// TestLatestTiDBTableInfoSharedSchemaGuard verifies the upstream struct fields
// that TiCDC shared-schema logic depends on.
func TestLatestTiDBTableInfoSharedSchemaGuard(t *testing.T) {
	// This guard intentionally checks the latest TiDB master to detect
	// upstream struct-field changes before TiCDC upgrades its pinned TiDB version.
	cases := []structGuardCase{
		{
			name:            "table_info_fields_for_shared_schema",
			modulePath:      "github.com/pingcap/tidb",
			pkgRelativePath: "pkg/meta/model",
			typeName:        "TableInfo",
			expectedFields: []string{
				"ID", "Name", "Charset", "Collate", "Columns", "Indices", "Constraints", "ForeignKeys", "State",
				"PKIsHandle", "IsCommonHandle", "CommonHandleVersion",
				"Comment", "AutoIncID", "AutoIncIDExtra", "AutoIDCache", "AutoRandID",
				"MaxColumnID", "MaxIndexID", "MaxForeignKeyID", "MaxConstraintID", "UpdateTS", "AutoIDSchemaID",
				"ShardRowIDBits", "MaxShardRowIDBits", "AutoRandomBits", "AutoRandomRangeBits", "PreSplitRegions",
				"Partition", "Compression", "View", "Sequence", "Lock", "Version", "TiFlashReplica", "IsColumnar",
				"TempTableType", "TableCacheStatusType", "PlacementPolicyRef", "StatsOptions",
				"ExchangePartitionInfo", "TTLInfo", "IsActiveActive", "SoftdeleteInfo", "Affinity",
				"Revision", "DBID", "Mode",
			},
		},
		{
			name:            "column_info_fields_for_shared_schema",
			modulePath:      "github.com/pingcap/tidb",
			pkgRelativePath: "pkg/meta/model",
			typeName:        "ColumnInfo",
			expectedFields: []string{
				"ID", "Name", "Offset", "OriginDefaultValue", "OriginDefaultValueBit", "DefaultValue", "DefaultValueBit",
				"DefaultIsExpr", "GeneratedExprString", "GeneratedStored", "Dependences", "FieldType", "ChangingFieldType",
				"State", "Comment", "Hidden", "ChangeStateInfo", "Version",
			},
		},
		{
			name:            "index_info_fields_for_shared_schema",
			modulePath:      "github.com/pingcap/tidb",
			pkgRelativePath: "pkg/meta/model",
			typeName:        "IndexInfo",
			expectedFields: []string{
				"ID", "Name", "Table", "Columns", "State", "BackfillState", "Comment", "Tp", "Unique", "Primary",
				"Invisible", "Global", "MVIndex", "VectorInfo", "InvertedInfo", "FullTextInfo",
				"ConditionExprString", "AffectColumn", "GlobalIndexVersion",
			},
		},
		{
			name:            "index_column_fields_for_shared_schema",
			modulePath:      "github.com/pingcap/tidb",
			pkgRelativePath: "pkg/meta/model",
			typeName:        "IndexColumn",
			expectedFields:  []string{"Name", "Offset", "Length", "UseChangingType"},
		},
		{
			name:            "field_type_fields_for_shared_schema",
			modulePath:      "github.com/pingcap/tidb/pkg/parser",
			pkgRelativePath: "types",
			typeName:        "FieldType",
			expectedFields:  []string{"tp", "flag", "flen", "decimal", "charset", "collate", "elems", "elemsIsBinaryLit", "array"},
		},
	}

	moduleCache := make(map[string]*moduleInfo)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mod, ok := moduleCache[tc.modulePath]
			if !ok {
				mod = queryModuleInfo(t, tc.modulePath)
				moduleCache[tc.modulePath] = mod
			}

			pkgDir := filepath.Join(mod.Dir, filepath.FromSlash(tc.pkgRelativePath))
			actualFields, err := extractStructFields(pkgDir, tc.typeName)
			require.NoError(t, err, "extract struct fields failed for %s in %s", tc.typeName, pkgDir)

			assertStructContract(t, tc, mod, actualFields)
		})
	}
}

// queryModuleInfo resolves a module at @master and returns location/version
// information for source-level contract checks.
func queryModuleInfo(t *testing.T, modulePath string) *moduleInfo {
	t.Helper()

	cmd := exec.Command("go", "list", "-m", "-json", modulePath+"@master")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "query module %s failed: %s", modulePath, string(output))

	mod, err := decodeModuleInfo(output)
	require.NoError(t, err, "unmarshal module metadata for %s failed", modulePath)

	// go list may omit Dir when the queried version is not yet downloaded.
	// In that case, explicitly download the resolved version to get a stable
	// source directory for AST parsing.
	if mod.Dir == "" {
		query := modulePath + "@master"
		if mod.Version != "" {
			query = modulePath + "@" + mod.Version
		}
		downloadCmd := exec.Command("go", "mod", "download", "-json", query)
		downloadOutput, downloadErr := downloadCmd.CombinedOutput()
		require.NoError(
			t,
			downloadErr,
			"download module %s failed: %s",
			query,
			string(downloadOutput),
		)

		downloaded, err := decodeModuleInfo(downloadOutput)
		require.NoError(t, err, "unmarshal downloaded module metadata for %s failed", query)

		if mod.Path == "" {
			mod.Path = downloaded.Path
		}
		if mod.Version == "" {
			mod.Version = downloaded.Version
		}
		if mod.Origin.Hash == "" {
			mod.Origin.Hash = downloaded.Origin.Hash
		}
		mod.Dir = downloaded.Dir
	}

	require.NotEmpty(t, mod.Dir, "module dir should not be empty for %s", modulePath)
	log.Info(
		"shared schema guard resolved module version",
		zap.String("module", mod.Path),
		zap.String("version", mod.Version),
		zap.String("hash", mod.Origin.Hash),
		zap.String("dir", mod.Dir),
	)
	return &mod
}

// decodeModuleInfo parses module metadata JSON from go command output.
// Some go commands may print informational lines before JSON payload.
func decodeModuleInfo(output []byte) (moduleInfo, error) {
	start := bytes.IndexByte(output, '{')
	if start == -1 {
		return moduleInfo{}, fmt.Errorf("no JSON object found in output: %s", string(output))
	}

	var mod moduleInfo
	if err := json.Unmarshal(output[start:], &mod); err != nil {
		return moduleInfo{}, err
	}
	return mod, nil
}

// assertStructContract compares expected fields with the latest upstream fields
// and fails fast with module version/hash details on mismatch.
func assertStructContract(t *testing.T, tc structGuardCase, mod *moduleInfo, actualFields []string) {
	t.Helper()

	missingFields, extraFields := diffFieldLists(tc.expectedFields, actualFields)
	if len(missingFields) == 0 && len(extraFields) == 0 {
		return
	}

	require.FailNowf(
		t,
		"latest TiDB struct contract changed",
		"case=%s struct=%s module=%s@%s hash=%s missing=%v extra=%v\n"+
			"please review shared-schema compatibility and update this guard test intentionally",
		tc.name,
		tc.typeName,
		mod.Path,
		mod.Version,
		mod.Origin.Hash,
		missingFields,
		extraFields,
	)
}

// extractStructFields parses non-test source files and returns the field list of
// a target struct type in sorted order.
func extractStructFields(packageDir string, typeName string) ([]string, error) {
	fileSet := token.NewFileSet()
	pkgs, err := parser.ParseDir(fileSet, packageDir, func(_ fs.FileInfo) bool {
		return true
	}, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}

	fieldSet := make(map[string]struct{})
	found := false
	for _, pkg := range pkgs {
		for fileName, fileNode := range pkg.Files {
			if strings.HasSuffix(fileName, "_test.go") {
				continue
			}
			for _, decl := range fileNode.Decls {
				genDecl, ok := decl.(*ast.GenDecl)
				if !ok || genDecl.Tok != token.TYPE {
					continue
				}
				for _, spec := range genDecl.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok || typeSpec.Name.Name != typeName {
						continue
					}
					structType, ok := typeSpec.Type.(*ast.StructType)
					if !ok {
						return nil, fmt.Errorf("type %s in %s is not a struct", typeName, packageDir)
					}
					if found {
						return nil, fmt.Errorf("type %s found multiple times in %s", typeName, packageDir)
					}
					found = true
					for _, field := range structType.Fields.List {
						if len(field.Names) == 0 {
							name, ok := embeddedFieldName(field.Type)
							if !ok {
								return nil, fmt.Errorf("unsupported embedded field expression in %s", typeName)
							}
							fieldSet[name] = struct{}{}
							continue
						}
						for _, name := range field.Names {
							fieldSet[name.Name] = struct{}{}
						}
					}
				}
			}
		}
	}

	if !found {
		return nil, fmt.Errorf("type %s not found in %s", typeName, packageDir)
	}

	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}
	slices.Sort(fields)
	return fields, nil
}

// embeddedFieldName extracts the synthetic field name for anonymous embedded
// fields (for example, *ChangeStateInfo -> ChangeStateInfo).
func embeddedFieldName(expr ast.Expr) (string, bool) {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name, true
	case *ast.StarExpr:
		return embeddedFieldName(x.X)
	case *ast.SelectorExpr:
		return x.Sel.Name, true
	case *ast.ParenExpr:
		return embeddedFieldName(x.X)
	case *ast.IndexExpr:
		return embeddedFieldName(x.X)
	case *ast.IndexListExpr:
		return embeddedFieldName(x.X)
	default:
		return "", false
	}
}

// diffFieldLists reports fields missing from actual and fields unexpectedly
// added in actual.
func diffFieldLists(expected, actual []string) ([]string, []string) {
	expectedSet := make(map[string]struct{}, len(expected))
	for _, field := range expected {
		expectedSet[field] = struct{}{}
	}
	actualSet := make(map[string]struct{}, len(actual))
	for _, field := range actual {
		actualSet[field] = struct{}{}
	}

	missing := make([]string, 0, len(expected))
	for field := range expectedSet {
		if _, ok := actualSet[field]; !ok {
			missing = append(missing, field)
		}
	}

	extra := make([]string, 0, len(actual))
	for field := range actualSet {
		if _, ok := expectedSet[field]; !ok {
			extra = append(extra, field)
		}
	}
	slices.Sort(missing)
	slices.Sort(extra)
	return missing, extra
}
