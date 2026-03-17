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

if command -v python3 &>/dev/null; then
	overlap_output=""
	if ! overlap_output=$(
		python3 - "$dashboard_file" <<'PY'
import json
from collections import defaultdict
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

def collect(items, parents=()):
    result = []
    for item in items:
        title = item.get("title", "<untitled>")
        path = " / ".join(parents + (title,))
        grid_pos = item.get("gridPos")
        if grid_pos:
            result.append(
                {
                    "path": path,
                    "x": grid_pos["x"],
                    "y": grid_pos["y"],
                    "w": grid_pos["w"],
                    "h": grid_pos["h"],
                }
            )
    return result

def collect_ids(items, parents=()):
    result = []
    for item in items:
        title = item.get("title", "<untitled>")
        path = " / ".join(parents + (title,))
        if "id" in item:
            result.append({"id": item["id"], "path": path})
        nested = item.get("panels", [])
        if nested:
            result.extend(collect_ids(nested, parents + (title,)))
    return result

def check_container(items, parents=()):
    messages = []
    panels = collect(items, parents)
    for i, left in enumerate(panels):
        for right in panels[i + 1 :]:
            if overlaps(left, right):
                messages.append(f"Overlap: {left['path']} <-> {right['path']}")

    for item in items:
        nested = item.get("panels", [])
        if nested:
            title = item.get("title", "<untitled>")
            messages.extend(check_container(nested, parents + (title,)))
    return messages

messages = []
id_groups = defaultdict(list)
for item in collect_ids(data.get("panels", [])):
    id_groups[item["id"]].append(item["path"])

for panel_id, paths in sorted(id_groups.items()):
    if len(paths) > 1:
        messages.append(
            f"Duplicate ID {panel_id}: " + " <-> ".join(paths)
        )

messages.extend(check_container(data.get("panels", [])))

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
