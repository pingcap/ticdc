#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc.
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

import json
from collections import defaultdict
import sys


def overlaps(left, right):
    return (
        left["x"] < right["x"] + right["w"]
        and left["x"] + left["w"] > right["x"]
        and left["y"] < right["y"] + right["h"]
        and left["y"] + left["h"] > right["y"]
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


def main():
    path = sys.argv[1]
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = []
    id_groups = defaultdict(list)
    for item in collect_ids(data.get("panels", [])):
        id_groups[item["id"]].append(item["path"])

    for panel_id, paths in sorted(id_groups.items()):
        if len(paths) > 1:
            messages.append(f"Duplicate ID {panel_id}: " + " <-> ".join(paths))

    messages.extend(check_container(data.get("panels", [])))

    if messages:
        print("\n".join(messages))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
