#!/bin/bash
# Copyright 2024 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

# Usage:
#   scripts/check-branch-diff.sh [release_branch] [master_branch]
# Default branches are release-8.5 and master.
# The script verifies both commit sets and tree diffs.

RELEASE_BRANCH=${1:-release-8.5}
MASTER_BRANCH=${2:-master}
EXPECTED_COMMITS=(
	"a9cc2cea6e520d114e5ebe3e722238dc6fd24e2d"
	"dfb4ce04ba3ada9ce548fae6b3ca75e8b1e314d0"
)

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	echo "Not inside a git repository" >&2
	exit 1
fi

ROOT_DIR=$(git rev-parse --show-toplevel)
cd "$ROOT_DIR"

for ref in "$RELEASE_BRANCH" "$MASTER_BRANCH" "${EXPECTED_COMMITS[@]}"; do
	if ! git rev-parse --verify "$ref^{commit}" >/dev/null 2>&1; then
		echo "Cannot resolve ref: $ref" >&2
		exit 1
	fi
done

merge_base=$(git merge-base "$MASTER_BRANCH" "$RELEASE_BRANCH")
mapfile -t master_only_commits < <(git rev-list --reverse "${merge_base}..${MASTER_BRANCH}")

if [[ ${#master_only_commits[@]} -ne ${#EXPECTED_COMMITS[@]} ]]; then
	echo "Unexpected number of commits on $MASTER_BRANCH above merge-base $merge_base" >&2
	echo "Expected: ${#EXPECTED_COMMITS[@]}, actual: ${#master_only_commits[@]}" >&2
	git log --oneline "${merge_base}..${MASTER_BRANCH}" >&2
	exit 1
fi

for i in "${!EXPECTED_COMMITS[@]}"; do
	if [[ "${master_only_commits[$i]}" != "${EXPECTED_COMMITS[$i]}" ]]; then
		echo "Commit mismatch at position $i" >&2
		echo "Expected: ${EXPECTED_COMMITS[$i]}" >&2
		echo "Actual:   ${master_only_commits[$i]}" >&2
		git log --oneline "${merge_base}..${MASTER_BRANCH}" >&2
		exit 1
	fi
done

tmp_index=$(mktemp)
tmp_worktree=$(mktemp -d)
trap 'rm -f "$tmp_index"; rm -rf "$tmp_worktree"' EXIT

GIT_INDEX_FILE="$tmp_index" GIT_WORK_TREE="$tmp_worktree" git read-tree "$RELEASE_BRANCH"
for commit in "${EXPECTED_COMMITS[@]}"; do
	if ! GIT_INDEX_FILE="$tmp_index" GIT_WORK_TREE="$tmp_worktree" git show --no-color --pretty=format: "$commit" | GIT_INDEX_FILE="$tmp_index" GIT_WORK_TREE="$tmp_worktree" git apply --cached --3way; then
		echo "Failed to apply commit $commit onto $RELEASE_BRANCH in dry-run index" >&2
		exit 1
	fi
done

expected_tree=$(GIT_INDEX_FILE="$tmp_index" GIT_WORK_TREE="$tmp_worktree" git write-tree)
master_tree=$(git rev-parse "${MASTER_BRANCH}^{tree}")

if git diff --quiet "$expected_tree" "$master_tree"; then
	echo "Diff check passed: $MASTER_BRANCH differs from $RELEASE_BRANCH only by the expected commits."
	exit 0
fi

echo "Diff mismatch: tree built from $RELEASE_BRANCH + expected commits does not match $MASTER_BRANCH." >&2
git diff --stat "$expected_tree" "$master_tree" >&2
exit 1
