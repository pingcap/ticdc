#!/usr/bin/env bash
# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ORIGIN_FILE="metrics/grafana/ticdc_new_arch.json"

NEXT_GEN_SHARED_FILE="${1:-metrics/grafana/ticdc_new_arch_next_gen.json}"
NEXT_GEN_USER_FILE="${2:-metrics/grafana/ticdc_new_arch_with_keyspace_name.json}"

# Detect the appropriate sed command
SED_CMD=""
if command -v gsed &>/dev/null; then
	SED_CMD="gsed"
elif [[ $(sed --version 2>/dev/null) == *"GNU"* ]]; then
	SED_CMD="sed"
else
	echo "This script requires GNU sed." >&2
	echo "On macOS, you can install it with 'brew install gnu-sed' and use it as 'gsed'." >&2
	exit 1
fi

"$SED_CMD" 's/namespace/keyspace_name/g;' $ORIGIN_FILE >"$NEXT_GEN_SHARED_FILE"

if ! command -v jq &>/dev/null; then
	echo "Error: jq is not installed. Please install it to run this script." >&2
	exit 1
fi

if [ ! -f "$NEXT_GEN_SHARED_FILE" ]; then
	echo "Error: Input file not found at '$NEXT_GEN_SHARED_FILE'" >&2
	exit 1
fi

# This jq script filters the panels in the Grafana dashboard.
# It defines a recursive function `filter_panels` that:
# 1. For panels of type "row", it recursively filters their sub-panels.
#    It keeps the row only if it contains relevant sub-panels after filtering.
# 2. For all other panels, it checks if the `expr` field of any target
#    contains the string "keyspace_name".
# The main part of the script applies this filter to the `.panels` array
# of the input JSON.
jq '
  def filter_panels:
    map(
      if .type == "row" then
        .panels |= filter_panels
        | select(.panels | length > 0)
      else
        select(any(.targets[]?.expr; test("keyspace_name")))
      end
    );
  .panels |= filter_panels
' "$NEXT_GEN_SHARED_FILE" >"$NEXT_GEN_USER_FILE"

sed -i "s/Test-Cluster-TiCDC-New-Arch/Test-Cluster-TiCDC-New-Arch-KeyspaceName/" "$NEXT_GEN_USER_FILE"
sed -i "s/YiGL8hBZ0aac/lGT5hED6vqTn/" "$NEXT_GEN_USER_FILE"

echo "Userscope dashboard created at '$NEXT_GEN_USER_FILE'"

"$SED_CMD" -i 's/tidb_cluster_id/tidb_cluster/' "$NEXT_GEN_SHARED_FILE"
"$SED_CMD" -i 's/tidb_cluster/sharedpool_id/' "$NEXT_GEN_SHARED_FILE"

echo "Sharedscope dashboard created at '$NEXT_GEN_SHARED_FILE'"
