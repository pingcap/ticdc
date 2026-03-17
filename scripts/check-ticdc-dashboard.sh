# Copyright 2022 PingCAP, Inc.
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

dashboard_file="metrics/grafana/ticdc_new_arch.json"
has_error=0

if command -v jq &>/dev/null; then
	dup=$(jq '[.panels[] | .panels[]?] | group_by(.id) | .[] | select(length > 1) | .[] | { id: .id, title: .title }' "$dashboard_file")
	if [[ -n $dup ]]; then
		echo "Find panels with duplicated ID in $dashboard_file"
		echo "$dup"
		echo "Please choose a new ID that is larger than the max ID:"
		jq '[.panels[] | .panels[]? | .id] | max' "$dashboard_file"
		has_error=1
	fi
fi

if command -v python3 &>/dev/null; then
	overlap_output=""
	if ! overlap_output=$(
		python3 - "$dashboard_file" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)

def overlaps(a, b):
    return (
        a["x"] < b["x"] + b["w"]
        and a["x"] + a["w"] > b["x"]
        and a["y"] < b["y"] + b["h"]
        and a["y"] + a["h"] > b["y"]
    )

def collect(items):
    result = []
    for item in items:
        grid_pos = item.get("gridPos")
        if not grid_pos:
            continue
        result.append(
            {
                "title": item.get("title", "<untitled>"),
                "x": grid_pos["x"],
                "y": grid_pos["y"],
                "w": grid_pos["w"],
                "h": grid_pos["h"],
            }
        )
    return result

messages = []
top_level = collect(data.get("panels", []))
for i, left in enumerate(top_level):
    for right in top_level[i + 1 :]:
        if overlaps(left, right):
            messages.append(
                f"Top level overlap: {left['title']} <-> {right['title']}"
            )

for row in data.get("panels", []):
    if row.get("type") != "row":
        continue
    nested = collect(row.get("panels", []))
    for i, left in enumerate(nested):
        for right in nested[i + 1 :]:
            if overlaps(left, right):
                messages.append(
                    f"Row '{row.get('title', '<untitled>')}' overlap: "
                    f"{left['title']} <-> {right['title']}"
                )

if messages:
    print("\n".join(messages))
    sys.exit(1)
PY
	); then
		echo "Find overlapped panels in $dashboard_file"
		echo "$overlap_output"
		has_error=1
	fi
fi

exit "$has_error"
