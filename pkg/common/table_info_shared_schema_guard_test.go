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
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type moduleInfo struct {
	Path    string `json:"Path"`
	Version string `json:"Version"`
	Dir     string `json:"Dir"`
	Zip     string `json:"Zip"`
	Error   string `json:"Error"`
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

type packageKey struct {
	modulePath      string
	pkgRelativePath string
}

type goSourceFile struct {
	name string
	src  []byte
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

	requiredTypesByPackage := buildRequiredTypesByPackage(cases)
	moduleCache := make(map[string]*moduleInfo)
	packageCache := make(map[packageKey]map[string][]string)
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			mod, ok := moduleCache[tc.modulePath]
			if !ok {
				mod = queryModuleInfo(t, tc.modulePath)
				moduleCache[tc.modulePath] = mod
			}

			pkgKey := packageKey{
				modulePath:      tc.modulePath,
				pkgRelativePath: tc.pkgRelativePath,
			}
			fieldsByType, ok := packageCache[pkgKey]
			if !ok {
				var err error
				fieldsByType, err = extractStructFieldsFromModule(
					mod,
					tc.pkgRelativePath,
					requiredTypesByPackage[pkgKey],
				)
				require.NoError(
					t,
					err,
					"extract struct fields failed for package=%s in module=%s version=%s types=%v",
					tc.pkgRelativePath,
					mod.Path,
					mod.Version,
					requiredTypesByPackage[pkgKey],
				)
				packageCache[pkgKey] = fieldsByType
			}

			actualFields, ok := fieldsByType[tc.typeName]
			require.True(
				t,
				ok,
				"type %s not found in package=%s module=%s@%s",
				tc.typeName,
				tc.pkgRelativePath,
				mod.Path,
				mod.Version,
			)

			assertStructContract(t, tc, mod, actualFields)
		})
	}
}

func buildRequiredTypesByPackage(cases []structGuardCase) map[packageKey][]string {
	requiredTypeSetByPackage := make(map[packageKey]map[string]struct{})
	for _, tc := range cases {
		key := packageKey{
			modulePath:      tc.modulePath,
			pkgRelativePath: tc.pkgRelativePath,
		}
		typeSet, ok := requiredTypeSetByPackage[key]
		if !ok {
			typeSet = make(map[string]struct{})
			requiredTypeSetByPackage[key] = typeSet
		}
		typeSet[tc.typeName] = struct{}{}
	}

	requiredTypesByPackage := make(map[packageKey][]string, len(requiredTypeSetByPackage))
	for key, typeSet := range requiredTypeSetByPackage {
		typeNames := make([]string, 0, len(typeSet))
		for typeName := range typeSet {
			typeNames = append(typeNames, typeName)
		}
		slices.Sort(typeNames)
		requiredTypesByPackage[key] = typeNames
	}
	return requiredTypesByPackage
}

// queryModuleInfo resolves a module at @master and returns location/version
// information for source-level contract checks.
func queryModuleInfo(t *testing.T, modulePath string) *moduleInfo {
	t.Helper()

	cmd := exec.Command("go", "mod", "download", "-json", modulePath+"@master")
	output, cmdErr := cmd.CombinedOutput()
	mod, decodeErr := decodeModuleInfo(output)
	require.NoError(t, decodeErr, "unmarshal module metadata for %s failed", modulePath)
	if cmdErr != nil && mod.Dir == "" && mod.Zip == "" {
		require.NoError(t, cmdErr, "download module %s failed: %s", modulePath, string(output))
	}
	if mod.Path == "" {
		mod.Path = modulePath
	}

	require.True(t, mod.Dir != "" || mod.Zip != "", "module source should not be empty for %s", modulePath)
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

// extractStructFieldsFromModule reads required struct fields from module source.
// It prefers local module dir, and falls back to module zip when dir is unavailable.
func extractStructFieldsFromModule(mod *moduleInfo, pkgRelativePath string, typeNames []string) (map[string][]string, error) {
	if len(typeNames) == 0 {
		return map[string][]string{}, nil
	}

	sourceFiles, err := collectPackageGoSourceFiles(mod, pkgRelativePath)
	if err != nil {
		return nil, err
	}
	return extractStructFieldsFromSourceFiles(sourceFiles, typeNames)
}

func collectPackageGoSourceFiles(mod *moduleInfo, pkgRelativePath string) ([]goSourceFile, error) {
	if mod.Dir != "" {
		pkgDir := filepath.Join(mod.Dir, filepath.FromSlash(pkgRelativePath))
		return collectPackageGoSourceFilesFromDir(pkgDir)
	}
	if mod.Zip != "" {
		return collectPackageGoSourceFilesFromZip(mod.Zip, pkgRelativePath)
	}
	return nil, fmt.Errorf("no source available for module=%s version=%s", mod.Path, mod.Version)
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

func collectPackageGoSourceFilesFromDir(packageDir string) ([]goSourceFile, error) {
	entries, err := os.ReadDir(packageDir)
	if err != nil {
		return nil, err
	}

	sourceFiles := make([]goSourceFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if !strings.HasSuffix(fileName, ".go") || strings.HasSuffix(fileName, "_test.go") {
			continue
		}

		src, err := os.ReadFile(filepath.Join(packageDir, fileName))
		if err != nil {
			return nil, err
		}
		sourceFiles = append(sourceFiles, goSourceFile{
			name: fileName,
			src:  src,
		})
	}
	return sourceFiles, nil
}

func collectPackageGoSourceFilesFromZip(zipPath string, pkgRelativePath string) ([]goSourceFile, error) {
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	sourceFiles := make([]goSourceFile, 0, len(reader.File))
	targetDir := filepath.ToSlash(pkgRelativePath) + "/"

	for _, file := range reader.File {
		if file.FileInfo().IsDir() {
			continue
		}

		name := filepath.ToSlash(file.Name)
		// Module zip entries are prefixed with "<module>@<version>/", where
		// "<module>" may itself contain multiple path segments.
		// Locate the target package by suffix pattern instead of assuming the
		// first path segment is the whole module root.
		targetPrefix := "/" + targetDir
		pkgPos := strings.Index(name, targetPrefix)
		if pkgPos < 0 {
			continue
		}
		rest := name[pkgPos+len(targetPrefix):]
		if strings.Contains(rest, "/") {
			continue
		}
		if !strings.HasSuffix(rest, ".go") || strings.HasSuffix(rest, "_test.go") {
			continue
		}

		rc, err := file.Open()
		if err != nil {
			return nil, err
		}
		src, readErr := io.ReadAll(rc)
		closeErr := rc.Close()
		if readErr != nil {
			return nil, readErr
		}
		if closeErr != nil {
			return nil, closeErr
		}

		sourceFiles = append(sourceFiles, goSourceFile{
			name: rest,
			src:  src,
		})
	}
	return sourceFiles, nil
}

func extractStructFieldsFromSourceFiles(sourceFiles []goSourceFile, typeNames []string) (map[string][]string, error) {
	targetTypeSet := make(map[string]struct{}, len(typeNames))
	for _, typeName := range typeNames {
		targetTypeSet[typeName] = struct{}{}
	}

	fileSet := token.NewFileSet()
	foundTypeSet := make(map[string]struct{}, len(typeNames))
	nonStructTypeSet := make(map[string]struct{}, len(typeNames))
	fieldSetByType := make(map[string]map[string]struct{}, len(typeNames))

	for _, sourceFile := range sourceFiles {
		fileNode, err := parser.ParseFile(fileSet, sourceFile.name, sourceFile.src, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		if err := collectStructFieldsFromFile(
			fileNode,
			targetTypeSet,
			foundTypeSet,
			nonStructTypeSet,
			fieldSetByType,
		); err != nil {
			return nil, err
		}
	}

	fieldsByType := make(map[string][]string, len(typeNames))
	for _, typeName := range typeNames {
		if _, ok := nonStructTypeSet[typeName]; ok {
			return nil, fmt.Errorf("type %s is not a struct", typeName)
		}

		fieldSet, ok := fieldSetByType[typeName]
		if !ok {
			return nil, fmt.Errorf("type %s not found in package source files", typeName)
		}
		fields := make([]string, 0, len(fieldSet))
		for field := range fieldSet {
			fields = append(fields, field)
		}
		slices.Sort(fields)
		fieldsByType[typeName] = fields
	}
	return fieldsByType, nil
}

// collectStructFieldsFromFile collects fields from required structs in one AST file.
func collectStructFieldsFromFile(
	fileNode *ast.File,
	targetTypeSet map[string]struct{},
	foundTypeSet map[string]struct{},
	nonStructTypeSet map[string]struct{},
	fieldSetByType map[string]map[string]struct{},
) error {
	for _, decl := range fileNode.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}

			typeName := typeSpec.Name.Name
			if _, ok := targetTypeSet[typeName]; !ok {
				continue
			}
			if _, found := foundTypeSet[typeName]; found {
				return fmt.Errorf("type %s found multiple times", typeName)
			}
			foundTypeSet[typeName] = struct{}{}

			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				nonStructTypeSet[typeName] = struct{}{}
				continue
			}

			fieldSet := make(map[string]struct{}, len(structType.Fields.List))
			for _, field := range structType.Fields.List {
				if len(field.Names) == 0 {
					name, ok := embeddedFieldName(field.Type)
					if !ok {
						return fmt.Errorf("unsupported embedded field expression in %s", typeName)
					}
					fieldSet[name] = struct{}{}
					continue
				}
				for _, name := range field.Names {
					fieldSet[name.Name] = struct{}{}
				}
			}
			fieldSetByType[typeName] = fieldSet
		}
	}
	return nil
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
