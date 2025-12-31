#!/usr/bin/env bash
set -euo pipefail

# Tidy the classic go.mod
# Note: go mod tidy does not support -tags, so it will try to parse all files.
# We use -e to continue despite errors, then rely on build to verify correctness.
echo "Tidying go.mod..."
GO111MODULE=on go mod tidy -modfile=go.mod -e

if [ "$(git --no-pager diff go.mod go.sum | wc -c)" -ne 0 ]; then
	echo "Please run \`go mod tidy -modfile=go.mod\` to clean up"
	git --no-pager diff go.mod go.sum
	exit 1
fi

# Tidy the nextgen go.mod
echo "Tidying nextgen.go.mod..."
GO111MODULE=on go mod tidy -modfile=nextgen.go.mod -e

if [ "$(git --no-pager diff nextgen.go.mod nextgen.go.sum 2>/dev/null | wc -c)" -ne 0 ]; then
	echo "Please run \`go mod tidy -modfile=nextgen.go.mod\` to clean up"
	git --no-pager diff nextgen.go.mod nextgen.go.sum 2>/dev/null || git --no-pager diff nextgen.go.mod 2>/dev/null
	exit 1
fi

echo "All go.mod files are tidy"
