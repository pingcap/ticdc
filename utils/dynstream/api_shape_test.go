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

package dynstream

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDynamicStreamAreaAPIShape(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "interfaces.go", nil, 0)
	require.NoError(t, err)

	var handlerSpec *ast.TypeSpec
	var dynamicStreamSpec *ast.TypeSpec
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.TYPE {
			continue
		}
		for _, spec := range genDecl.Specs {
			typeSpec, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			switch typeSpec.Name.Name {
			case "Handler":
				handlerSpec = typeSpec
			case "DynamicStream":
				dynamicStreamSpec = typeSpec
			}
		}
	}

	require.NotNil(t, handlerSpec)
	require.NotNil(t, dynamicStreamSpec)

	require.NotNil(t, handlerSpec.TypeParams)
	require.Equal(t, 3, handlerSpec.TypeParams.NumFields())
	requireGetAreaReturnString(t, handlerSpec)

	require.NotNil(t, dynamicStreamSpec.TypeParams)
	require.Equal(t, 4, dynamicStreamSpec.TypeParams.NumFields())
	requireSetAreaSettingsArgString(t, dynamicStreamSpec)
}

func requireGetAreaReturnString(t *testing.T, spec *ast.TypeSpec) {
	t.Helper()
	iface, ok := spec.Type.(*ast.InterfaceType)
	require.True(t, ok)

	var getAreaFunc *ast.FuncType
	for _, field := range iface.Methods.List {
		for _, name := range field.Names {
			if name.Name == "GetArea" {
				getAreaFunc, _ = field.Type.(*ast.FuncType)
				break
			}
		}
	}
	require.NotNil(t, getAreaFunc)
	require.NotNil(t, getAreaFunc.Results)
	require.Len(t, getAreaFunc.Results.List, 1)

	resultIdent, ok := getAreaFunc.Results.List[0].Type.(*ast.Ident)
	require.True(t, ok)
	require.Equal(t, "string", resultIdent.Name)
}

func requireSetAreaSettingsArgString(t *testing.T, spec *ast.TypeSpec) {
	t.Helper()
	iface, ok := spec.Type.(*ast.InterfaceType)
	require.True(t, ok)

	var setAreaSettingsFunc *ast.FuncType
	for _, field := range iface.Methods.List {
		for _, name := range field.Names {
			if name.Name == "SetAreaSettings" {
				setAreaSettingsFunc, _ = field.Type.(*ast.FuncType)
				break
			}
		}
	}
	require.NotNil(t, setAreaSettingsFunc)
	require.NotNil(t, setAreaSettingsFunc.Params)
	require.GreaterOrEqual(t, setAreaSettingsFunc.Params.NumFields(), 2)

	firstParam := setAreaSettingsFunc.Params.List[0]
	firstIdent, ok := firstParam.Type.(*ast.Ident)
	require.True(t, ok)
	require.Equal(t, "string", firstIdent.Name)
}
