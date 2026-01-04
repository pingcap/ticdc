#!/usr/bin/env bash
set -euo pipefail

# Note: go mod tidy does not support -tags, so it will try to parse all files.

hide_files() {
	local pattern="$1"
	find . -name "$pattern" -exec mv {} {}.bak \;
}

restore_files() {
	local pattern="$1"
	find . -name "$pattern" -exec sh -c 'mv "$1" "${1%.bak}"' _ {} \; || true
}

switch_to_nextgen() {
	mv go.mod classic.go.mod
	mv go.sum classic.go.sum
	mv nextgen.go.mod go.mod
	mv nextgen.go.sum go.sum
}

restore_classic() {
	if [ -f go.mod ]; then
		mv go.mod nextgen.go.mod
	fi
	if [ -f go.sum ]; then
		mv go.sum nextgen.go.sum
	fi
	if [ -f classic.go.mod ]; then
		mv classic.go.mod go.mod
	fi
	if [ -f classic.go.sum ]; then
		mv classic.go.sum go.sum
	fi
}

cleanup() {
	# Restore files if strictly needed
	restore_files '*_nextgen.go.bak'
	restore_files '*_nextgen_test.go.bak'
	restore_files '*_classic.go.bak'
	restore_files '*_classic_test.go.bak'

	# Restore go.mod/go.sum if they were swapped
	if [ -f classic.go.mod ]; then
		restore_classic
	fi
}

trap cleanup EXIT

# Tidy the classic go.mod
echo "Tidying go.mod (Classic)..."
hide_files '*_nextgen.go'
hide_files '*_nextgen_test.go'

GO111MODULE=on go mod tidy

restore_files '*_nextgen.go.bak'
restore_files '*_nextgen_test.go.bak'

# Tidy the nextgen go.mod
echo "Tidying nextgen.go.mod..."
hide_files '*_classic.go'
hide_files '*_classic_test.go'

switch_to_nextgen

GO111MODULE=on go mod tidy

restore_files '*_classic.go.bak'
restore_files '*_classic_test.go.bak'

restore_classic

if [ "$(git --no-pager diff go.mod go.sum nextgen.go.mod nextgen.go.sum | wc -c)" -ne 0 ]; then
	echo "Please run 'make tidy' to clean up"
	git --no-pager diff go.mod go.sum nextgen.go.mod nextgen.go.sum
	exit 1
fi

echo "All go.mod files are tidy"
