#!/usr/bin/env bash
set -euo pipefail

# Note: go mod tidy does not support -tags, so it will try to parse all files.

# Tidy the classic go.mod
echo "Tidying go.mod (Classic)..."
# Hidding the nextgen files
find . -name '*_nextgen.go' -exec mv {} {}.bak \;
find . -name '*_nextgen_test.go' -exec mv {} {}.bak \;
GO111MODULE=on go mod tidy
# Restore the nextgen files
find . -name '*_nextgen.go.bak' -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \;
find . -name '*_nextgen_test.go.bak' -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \;

# Tidy the nextgen go.mod
echo "Tidying nextgen.go.mod..."
# Hidding the classic files.
find . -name '*_classic.go' -exec mv {} {}.bak \;
find . -name '*_classic_test.go' -exec mv {} {}.bak \;
mv go.mod classic.go.mod
mv go.sum classic.go.sum
mv nextgen.go.mod go.mod
mv nextgen.go.sum go.sum
GO111MODULE=on go mod tidy
find . -name '*_classic.go.bak' -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \;
find . -name '*_classic_test.go.bak' -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \;
mv go.mod nextgen.go.mod
mv go.sum nextgen.go.sum
mv classic.go.mod go.mod
mv classic.go.sum go.sum

if [ "$(git --no-pager diff go.mod go.sum nextgen.go.mod nextgen.go.sum | wc -c)" -ne 0 ]; then
	echo "Please run \`make tidy\` to clean up"
	git --no-pager diff go.mod go.sum nextgen.go.mod nextgen.go.sum
	exit 1
fi

echo "All go.mod files are tidy"
